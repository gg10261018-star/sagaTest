package saga

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"strings"
	"time"

	"saga-points-demo/internal/clock"
	"saga-points-demo/internal/errs"
)

type Config struct {
	// 正向步驟遇到暫時性錯誤時，當場重試的次數與間隔
	ForwardMaxAttempts int
	ForwardRetryDelay  time.Duration

	// 租約長度：處理者崩潰後，最久多久別人可以接手
	LeaseDuration time.Duration

	// 補償重試：指數退避（BackoffBase × 2^(n-1)，上限 BackoffMax，加上隨機抖動）
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// 補償總時限：從第一次失敗起算，超過就升級人工介入。
	// 沒有標準答案，取決於「故障預期恢復時間」跟「卡在哪個業務環節」——
	// 點數這種低敏感度可以寬鬆；金流 / 庫存應該縮短、提早介入。
	CompensationDeadline time.Duration

	// 補償重試達到這個次數時，先發低優先級告警（分層告警）
	WarnAfterRetries int

	// pending 狀態超過這麼久沒更新，且沒有有效租約 → 視為流程中斷，轉入補償
	StuckPendingAfter time.Duration
}

func DefaultConfig() Config {
	return Config{
		ForwardMaxAttempts:   3,
		ForwardRetryDelay:    100 * time.Millisecond,
		LeaseDuration:        30 * time.Second,
		BackoffBase:          2 * time.Second,
		BackoffMax:           5 * time.Minute,
		CompensationDeadline: 30 * time.Minute,
		WarnAfterRetries:     5,
		StuckPendingAfter:    2 * time.Minute,
	}
}

type Orchestrator struct {
	store   *Store
	steps   []Step
	clock   clock.Clock
	cfg     Config
	alerter Alerter
	log     *slog.Logger
}

func NewOrchestrator(store *Store, steps []Step, c clock.Clock, cfg Config, alerter Alerter, log *slog.Logger) *Orchestrator {
	if log == nil {
		log = slog.Default()
	}
	return &Orchestrator{store: store, steps: steps, clock: c, cfg: cfg, alerter: alerter, log: log}
}

func (o *Orchestrator) Store() *Store { return o.store }

// Start 發起（或重送）一次兌換。
//
// 同一個 request_id 不論送幾次、併發送幾次，都只會有一個 saga 被推進：
// sagas.request_id 唯一鍵保證只建立一筆，租約保證只有一個處理者在跑。
// 沒搶到租約的請求，直接回傳 saga 目前的狀態。
func (o *Orchestrator) Start(ctx context.Context, req Request) (*Saga, error) {
	if req.RequestID == "" || req.UserID <= 0 || req.ItemID <= 0 || req.Cost <= 0 {
		return nil, fmt.Errorf("%w: request_id / user_id / item_id / cost 皆為必填且須為正數", errs.ErrPermanent)
	}

	s, err := o.store.CreateOrGet(ctx, req, o.clock.Now())
	if err != nil {
		return nil, err
	}
	if s.Status != StatusPending {
		return s, nil // 已經處理過（或正在補償，交給背景 worker）
	}

	owner := newOwnerToken()
	ok, err := o.store.Claim(ctx, s.ID, owner, o.clock.Now(), o.cfg.LeaseDuration)
	if err != nil {
		return nil, err
	}
	if !ok {
		return s, nil // 另一個請求正在處理同一筆
	}
	defer o.release(s.ID, owner)

	// 拿到租約後重讀一次：搶租約之前，別人可能已經把它推進完了
	if s, err = o.store.getByID(ctx, s.ID); err != nil {
		return nil, err
	}
	if s.Status == StatusPending {
		if err := o.runForward(ctx, s, owner); err != nil {
			return nil, err
		}
	}
	return o.store.getByID(ctx, s.ID)
}

// runForward 依序執行正向步驟，每完成一步就把進度持久化。
func (o *Orchestrator) runForward(ctx context.Context, s *Saga, owner string) error {
	for s.CurrentStep < len(o.steps) {
		step := o.steps[s.CurrentStep]
		err := o.executeWithRetry(ctx, step, s)
		if err == nil {
			s.CurrentStep++
			if err := o.save(ctx, s, owner); err != nil {
				return err
			}
			continue
		}

		failedStep := s.CurrentStep + 1 // 1-based
		s.LastError = fmt.Sprintf("step %d (%s) 失敗: %v", failedStep, step.Name(), err)
		o.log.Warn("saga 正向步驟失敗", "request_id", s.RequestID, "step", step.Name(), "err", err)

		// 關鍵判斷：失敗的這一步「到底有沒有執行」？
		//   - 確定的業務拒絕（點數不足、庫存不足）→ 確定沒執行，從前一步開始補償
		//   - 暫時性錯誤（逾時、斷線）→ 不確定！可能下游已經成功、只是回應遺失，
		//     所以連同失敗的這一步一起補償。下游的補償邏輯會處理「其實沒執行」的情況
		//     （空補償），並寫入作廢標記擋掉晚到的正向請求（防懸掛）。
		if errs.IsPermanent(err) {
			s.CompNext = s.CurrentStep
		} else {
			s.CompNext = failedStep
		}

		if s.CompNext == 0 {
			s.Status = StatusFailed // 第一步就被拒絕，沒有任何東西需要復原
			return o.save(ctx, s, owner)
		}

		now := o.clock.Now()
		s.Status = StatusCompensating
		s.FirstFailedAt = &now
		s.NextAttemptAt = &now
		if err := o.save(ctx, s, owner); err != nil {
			return err
		}
		return o.runCompensation(ctx, s, owner)
	}

	s.Status = StatusCompleted
	return o.save(ctx, s, owner)
}

func (o *Orchestrator) executeWithRetry(ctx context.Context, step Step, s *Saga) error {
	var err error
	for attempt := 1; attempt <= o.cfg.ForwardMaxAttempts; attempt++ {
		err = step.Execute(ctx, s)
		if err == nil || errs.IsPermanent(err) {
			return err // 業務拒絕重試也沒用，直接放棄
		}
		if attempt < o.cfg.ForwardMaxAttempts && o.cfg.ForwardRetryDelay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(o.cfg.ForwardRetryDelay):
			}
		}
	}
	return err
}

// runCompensation 反向、接力式補償：從 CompNext 往回補到第 1 步。
//
// 「接力」的意思：第 N 步補償沒成功，就不會去做第 N-1 步。
// 每補完一步就持久化進度，所以任何時間點中斷，狀態都是「後面已復原、前面還沒」，
// 合理且可解釋，不會出現「點數已退但庫存仍卡住」這種東補一塊西空一塊的中間態。
func (o *Orchestrator) runCompensation(ctx context.Context, s *Saga, owner string) error {
	for s.CompNext > 0 {
		step := o.steps[s.CompNext-1]
		if err := step.Compensate(ctx, s); err != nil {
			return o.handleCompensationFailure(ctx, s, owner, step, err)
		}
		o.log.Info("saga 補償步驟完成", "request_id", s.RequestID, "step", step.Name())
		s.CompNext--
		if err := o.save(ctx, s, owner); err != nil {
			return err
		}
	}
	s.Status = StatusCompensated
	s.NextAttemptAt = nil
	return o.save(ctx, s, owner)
}

// handleCompensationFailure 補償失敗時的處理。
//
// 正向跟補償的設計哲學不對稱：正向失敗可以放棄、通知使用者；
// 補償「必須想辦法成功」，所以不會放棄，而是排程背景重試（指數退避），
// 直到超過總時限才升級人工。
func (o *Orchestrator) handleCompensationFailure(ctx context.Context, s *Saga, owner string, step Step, cause error) error {
	now := o.clock.Now()
	s.CompRetryCount++
	s.LastError = fmt.Sprintf("補償 step %d (%s) 失敗（第 %d 次）: %v", s.CompNext, step.Name(), s.CompRetryCount, cause)

	if s.FirstFailedAt != nil && now.Sub(*s.FirstFailedAt) >= o.cfg.CompensationDeadline {
		s.Status = StatusNeedsManual
		s.NextAttemptAt = nil
		if err := o.save(ctx, s, owner); err != nil {
			return err
		}
		o.alerter.Alert(ctx, AlertCritical, s, "補償超過時限仍未成功，已升級人工介入")
		return nil
	}

	next := now.Add(o.backoff(s.CompRetryCount))
	s.NextAttemptAt = &next
	if err := o.save(ctx, s, owner); err != nil {
		return err
	}
	if s.CompRetryCount == o.cfg.WarnAfterRetries {
		o.alerter.Alert(ctx, AlertWarning, s, "補償已多次重試失敗，持續重試中")
	}
	return nil
}

// backoff 指數退避 + 抖動（jitter）。
// 抖動是為了避免大量失敗的 saga 在同一瞬間一起重試，對剛恢復的下游造成二次衝擊。
func (o *Orchestrator) backoff(retry int) time.Duration {
	d := o.cfg.BackoffBase
	for i := 1; i < retry && d < o.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > o.cfg.BackoffMax {
		d = o.cfg.BackoffMax
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(mrand.Int64N(int64(half)+1)) // 落在 [d/2, d]
}

func (o *Orchestrator) save(ctx context.Context, s *Saga, owner string) error {
	return o.store.Save(ctx, s, owner, o.clock.Now(), o.cfg.LeaseDuration)
}

func (o *Orchestrator) release(id int64, owner string) {
	// 用獨立 context：就算原本的 request 已經被取消，也要把租約還回去
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := o.store.Release(ctx, id, owner); err != nil {
		o.log.Error("釋放租約失敗（租約到期後會自動失效）", "saga_id", id, "err", err)
	}
}

// ── 背景補償 worker ──────────────────────────────────────────────────

// RecoverOnce 掃描一輪需要處理的 saga，回傳實際處理的數量。
// 多台機器同時跑也安全：搶不到租約的就跳過。
func (o *Orchestrator) RecoverOnce(ctx context.Context) (int, error) {
	now := o.clock.Now()
	ids, err := o.store.FindRecoverable(ctx, now, now.Add(-o.cfg.StuckPendingAfter), 50)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, id := range ids {
		did, err := o.recoverOne(ctx, id)
		if err != nil {
			o.log.Error("背景處理 saga 失敗", "saga_id", id, "err", err)
			continue
		}
		if did {
			processed++
		}
	}
	return processed, nil
}

func (o *Orchestrator) recoverOne(ctx context.Context, id int64) (bool, error) {
	owner := newOwnerToken()
	ok, err := o.store.Claim(ctx, id, owner, o.clock.Now(), o.cfg.LeaseDuration)
	if err != nil || !ok {
		return false, err
	}
	defer o.release(id, owner)

	s, err := o.store.getByID(ctx, id)
	if err != nil {
		return false, err
	}
	now := o.clock.Now()

	switch s.Status {
	case StatusCompensating:
		if s.NextAttemptAt != nil && s.NextAttemptAt.After(now) {
			return false, nil
		}
	case StatusPending:
		// 處理到一半程式崩潰（太久沒做完）。
		// 先到各服務的流水表查證「到底做到哪」，再決定從哪一步開始補償。
		//
		// 這裡選擇「中斷就回滾」而不是「接著做完」，因為使用者早就收到逾時回應了，
		// 讓他重新兌換比默默在背景幫他完成更不容易造成誤解。
		if s.UpdatedAt.After(now.Add(-o.cfg.StuckPendingAfter)) {
			return false, nil
		}
		report, highestApplied := o.ProbeAll(ctx, s)

		// 補償起點 = max(流水證據裡最後一個生效的步驟, 當時正在執行的那一步)
		//
		// 為什麼不完全相信流水？因為「當時正在執行的那一步」查不到流水，
		// 不代表它不會生效——那個請求可能還卡在網路上，晚一點才到。
		// 把它也納入補償，下游會寫入作廢標記，晚到的請求就會被擋掉（防懸掛）。
		// 流水證據的價值是：讓決策跟告警「有憑有據」，人工介入時一眼看懂現場。
		s.CompNext = max(highestApplied, min(s.CurrentStep+1, len(o.steps)))
		s.Status = StatusCompensating
		s.FirstFailedAt = &now
		s.NextAttemptAt = &now
		s.LastError = fmt.Sprintf("流程中斷：停在 step %d 超過 %s 未更新，轉入補償。流水查證：%s",
			s.CurrentStep+1, o.cfg.StuckPendingAfter, report)
		o.log.Warn("偵測到中斷的 saga", "request_id", s.RequestID, "probe", report)
		if err := o.save(ctx, s, owner); err != nil {
			return false, err
		}
	default:
		return false, nil
	}

	if err := o.runCompensation(ctx, s, owner); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// StepProbe 是單一步驟的查證結果。
type StepProbe struct {
	Step  string `json:"step"`
	State string `json:"state"` // none / applied / compensated / voided / error
	Error string `json:"error,omitempty"`
}

// ProbeAll 逐步查證每個步驟在下游流水裡的真實狀態。
// 回傳人看得懂的報告，以及「仍處於 applied 狀態的最後一個步驟」（1-based，0 = 都沒有）。
// 查證失敗的步驟保守地當作 applied，寧可多補（補償有空補償保護），不能漏補。
func (o *Orchestrator) ProbeAll(ctx context.Context, s *Saga) (string, int) {
	results := o.Probe(ctx, s)
	highest := 0
	parts := make([]string, 0, len(results))
	for i, r := range results {
		if r.State == "applied" || r.State == "error" {
			highest = i + 1
		}
		parts = append(parts, r.Step+"="+r.State)
	}
	return strings.Join(parts, ", "), highest
}

func (o *Orchestrator) Probe(ctx context.Context, s *Saga) []StepProbe {
	out := make([]StepProbe, 0, len(o.steps))
	for _, st := range o.steps {
		state, err := st.Probe(ctx, s)
		p := StepProbe{Step: st.Name(), State: state}
		if err != nil {
			p.State, p.Error = "error", err.Error()
		}
		out = append(out, p)
	}
	return out
}

// RunWorker 定期執行 RecoverOnce，直到 ctx 被取消。
func (o *Orchestrator) RunWorker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := o.RecoverOnce(ctx); err != nil {
				o.log.Error("背景 worker 掃描失敗", "err", err)
			} else if n > 0 {
				o.log.Info("背景 worker 處理完成", "count", n)
			}
		}
	}
}

func newOwnerToken() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
