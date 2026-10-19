package saga_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"saga-points-demo/internal/clock"
	"saga-points-demo/internal/inventory"
	"saga-points-demo/internal/items"
	"saga-points-demo/internal/points"
	"saga-points-demo/internal/saga"
	"saga-points-demo/internal/testdb"
)

const (
	userID       = 1001
	itemID       = 1
	startBalance = 1000
	startStock   = 5
	cost         = 100
)

// env 是一套完整的測試環境：三個服務 + 協調器 + 故障注入 + 可快轉的時鐘。
type env struct {
	ctx     context.Context
	clk     *clock.Fake
	points  *points.Service
	inv     *inventory.Service
	items   *items.Service
	orch    *saga.Orchestrator
	alerts  *saga.RecordingAlerter
	chaos   map[string]*saga.ChaosStep
	compLog *compLog
}

// compLog 記錄補償「實際成功」的順序，用來驗證反向接力。
type compLog struct {
	mu    sync.Mutex
	order []string
}

type recordingStep struct {
	saga.Step
	log *compLog
}

func (r recordingStep) Compensate(ctx context.Context, s *saga.Saga) error {
	if err := r.Step.Compensate(ctx, s); err != nil {
		return err
	}
	r.log.mu.Lock()
	r.log.order = append(r.log.order, r.Name())
	r.log.mu.Unlock()
	return nil
}

func newEnv(t *testing.T) *env {
	t.Helper()
	conn := testdb.Open(t, "saga")
	clk := clock.NewFake(time.Date(2026, 10, 19, 6, 0, 0, 0, time.UTC))
	e := &env{
		ctx:     context.Background(),
		clk:     clk,
		points:  points.NewService(conn, clk),
		inv:     inventory.NewService(conn, clk),
		items:   items.NewService(conn, clk),
		alerts:  &saga.RecordingAlerter{},
		chaos:   map[string]*saga.ChaosStep{},
		compLog: &compLog{},
	}

	var steps []saga.Step
	for _, st := range []saga.Step{
		saga.DeductPointsStep{Svc: e.points},
		saga.DeductStockStep{Svc: e.inv},
		saga.GrantItemStep{Svc: e.items},
	} {
		c := saga.NewChaos(st)
		e.chaos[st.Name()] = c
		steps = append(steps, recordingStep{Step: c, log: e.compLog})
	}

	cfg := saga.Config{
		ForwardMaxAttempts:   3,
		ForwardRetryDelay:    0,
		LeaseDuration:        30 * time.Second,
		BackoffBase:          time.Second,
		BackoffMax:           time.Minute,
		CompensationDeadline: 10 * time.Minute,
		WarnAfterRetries:     3,
		StuckPendingAfter:    2 * time.Minute,
	}
	e.orch = saga.NewOrchestrator(saga.NewStore(conn), steps, clk, cfg, e.alerts, nil)

	if _, err := e.points.ApplyPointsChange(e.ctx, points.Change{
		RequestID: "seed", UserID: userID, Delta: startBalance, BizType: points.BizAdminAdjust,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.inv.SetStock(e.ctx, itemID, startStock); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) redeem(t *testing.T, requestID string) *saga.Saga {
	t.Helper()
	s, err := e.orch.Start(e.ctx, saga.Request{RequestID: requestID, UserID: userID, ItemID: itemID, Cost: cost})
	if err != nil {
		t.Fatalf("Start 失敗: %v", err)
	}
	return s
}

// assertState 一次檢查三個服務的狀態，並做點數對帳。
func (e *env) assertState(t *testing.T, wantBalance, wantStock int64, wantItems int) {
	t.Helper()
	bal, sum, err := e.points.Reconcile(e.ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	if bal != wantBalance {
		t.Errorf("點數餘額 = %d，期望 %d", bal, wantBalance)
	}
	if bal != sum {
		t.Errorf("對帳失敗：餘額 %d ≠ 流水加總 %d", bal, sum)
	}
	if stock, _ := e.inv.Stock(e.ctx, itemID); stock != wantStock {
		t.Errorf("庫存 = %d，期望 %d", stock, wantStock)
	}
	if n, _ := e.items.CountGranted(e.ctx, userID, itemID); n != wantItems {
		t.Errorf("持有道具數 = %d，期望 %d", n, wantItems)
	}
}

func (e *env) get(t *testing.T, requestID string) *saga.Saga {
	t.Helper()
	s, err := e.orch.Store().Get(e.ctx, requestID)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// runWorkerUntil 模擬背景排程：每次把時鐘快轉 step，再跑一輪 RecoverOnce。
func (e *env) runWorkerUntil(t *testing.T, requestID string, step time.Duration, maxRounds int, done func(*saga.Saga) bool) *saga.Saga {
	t.Helper()
	for i := 0; i < maxRounds; i++ {
		e.clk.Advance(step)
		if _, err := e.orch.RecoverOnce(e.ctx); err != nil {
			t.Fatal(err)
		}
		if s := e.get(t, requestID); done(s) {
			return s
		}
	}
	s := e.get(t, requestID)
	t.Fatalf("經過 %d 輪仍未達到預期狀態，目前: %+v", maxRounds, s)
	return nil
}

func assertOrder(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("補償順序 = %v，期望 %v", got, want)
	}
}

// ─────────────────────────────────────────────────────────────────────

func TestHappyPath(t *testing.T) {
	e := newEnv(t)
	s := e.redeem(t, "r-happy")

	if s.Status != saga.StatusCompleted {
		t.Fatalf("狀態 = %s，期望 completed（%s）", s.Status, s.LastError)
	}
	e.assertState(t, startBalance-cost, startStock-1, 1)

	ledger, _ := e.points.ListBySaga(e.ctx, "r-happy")
	if len(ledger) != 1 || ledger[0].BizType != points.BizRedeemDeduct || ledger[0].Delta != -cost {
		t.Fatalf("流水應只有一筆扣點，實際 %+v", ledger)
	}
}

// 第一步就被業務拒絕：確定沒執行，不需要補償。
func TestInsufficientPoints_FailsWithoutCompensation(t *testing.T) {
	e := newEnv(t)
	s, err := e.orch.Start(e.ctx, saga.Request{RequestID: "r-poor", UserID: userID, ItemID: itemID, Cost: 99999})
	if err != nil {
		t.Fatal(err)
	}
	if s.Status != saga.StatusFailed {
		t.Fatalf("狀態 = %s，期望 failed", s.Status)
	}
	if len(e.compLog.order) != 0 {
		t.Fatalf("不該有任何補償，實際 %v", e.compLog.order)
	}
	e.assertState(t, startBalance, startStock, 0)
}

// 庫存不足（第二步的業務拒絕）：第二步確定沒執行，只補償第一步。
func TestOutOfStock_CompensatesOnlyPoints(t *testing.T) {
	e := newEnv(t)
	if err := e.inv.SetStock(e.ctx, itemID, 0); err != nil {
		t.Fatal(err)
	}
	s := e.redeem(t, "r-nostock")

	if s.Status != saga.StatusCompensated {
		t.Fatalf("狀態 = %s，期望 compensated", s.Status)
	}
	assertOrder(t, e.compLog.order, "deduct_points")
	e.assertState(t, startBalance, 0, 0)
}

// 發道具一直逾時（結果不確定）→ 三步全部「反向」補償：先收道具、再補庫存、最後退點。
func TestGrantFails_ReverseCompensation(t *testing.T) {
	e := newEnv(t)
	e.chaos["grant_item"].SetExecFailures(-1)

	s := e.redeem(t, "r-grantfail")

	if s.Status != saga.StatusCompensated {
		t.Fatalf("狀態 = %s，期望 compensated（%s）", s.Status, s.LastError)
	}
	// 發道具逾時屬於「不確定」→ 連同它一起補償（下游查不到紀錄就寫作廢標記）
	assertOrder(t, e.compLog.order, "grant_item", "deduct_stock", "deduct_points")
	e.assertState(t, startBalance, startStock, 0)

	ledger, _ := e.points.ListBySaga(e.ctx, "r-grantfail")
	if len(ledger) != 2 || ledger[0].BizType != points.BizRedeemDeduct || ledger[1].BizType != points.BizRedeemRefund {
		t.Fatalf("流水應為「扣點 → 退點」兩筆，實際 %+v", ledger)
	}

	// 懸掛防護：發道具的請求晚到，不能讓道具「復活」
	err := e.items.Grant(e.ctx, "grant_item:r-grantfail", userID, itemID)
	if err == nil {
		t.Fatal("晚到的發道具請求應被拒絕")
	}
	e.assertState(t, startBalance, startStock, 0)
}

// 扣庫存「其實成功了，但回應遺失」→ 協調器以為失敗。
// 期望：補償時查得到扣庫存紀錄，真的把庫存補回來（不會因為以為沒做就漏補）。
func TestStockSucceededButResponseLost_StillCompensated(t *testing.T) {
	e := newEnv(t)
	e.chaos["deduct_stock"].SetFailAfter(true)
	e.chaos["deduct_stock"].SetExecFailures(-1)

	s := e.redeem(t, "r-lost")

	if s.Status != saga.StatusCompensated {
		t.Fatalf("狀態 = %s，期望 compensated（%s）", s.Status, s.LastError)
	}
	assertOrder(t, e.compLog.order, "deduct_stock", "deduct_points")
	e.assertState(t, startBalance, startStock, 0)
}

// 反向「接力」：扣庫存的補償一直失敗時，不能跳過它先去退點。
func TestRelay_DoesNotSkipFailedCompensation(t *testing.T) {
	e := newEnv(t)
	e.chaos["grant_item"].SetExecFailures(-1)
	e.chaos["deduct_stock"].SetCompFailures(-1)

	s := e.redeem(t, "r-relay")

	if s.Status != saga.StatusCompensating {
		t.Fatalf("狀態 = %s，期望 compensating", s.Status)
	}
	if s.CompNext != 2 {
		t.Fatalf("應卡在第 2 步補償，實際 comp_next = %d", s.CompNext)
	}
	assertOrder(t, e.compLog.order, "grant_item") // 只補了第 3 步
	// 點數仍是被扣的狀態：寧可讓狀態「合理地停在中間」，也不要東補一塊西空一塊
	e.assertState(t, startBalance-cost, startStock-1, 0)
}

// 補償暫時失敗 → 背景 worker 依指數退避重試 → 最終成功。
func TestCompensationRetry_WithBackoff(t *testing.T) {
	e := newEnv(t)
	e.chaos["grant_item"].SetExecFailures(-1)
	e.chaos["deduct_points"].SetCompFailures(4) // 退點連續失敗 4 次後恢復

	s := e.redeem(t, "r-retry")
	if s.Status != saga.StatusCompensating || s.CompRetryCount != 1 || s.NextAttemptAt == nil {
		t.Fatalf("應進入補償重試，實際 %+v", s)
	}

	// 還沒到下次重試時間，worker 不該動它
	if n, _ := e.orch.RecoverOnce(e.ctx); n != 0 {
		t.Fatalf("未到重試時間不該處理，實際處理 %d 筆", n)
	}

	// 記錄每次排定的退避間隔，驗證是「指數成長」
	var gaps []time.Duration
	prev := e.get(t, "r-retry")
	final := e.runWorkerUntil(t, "r-retry", 500*time.Millisecond, 500, func(cur *saga.Saga) bool {
		if cur.CompRetryCount != prev.CompRetryCount && cur.NextAttemptAt != nil {
			gaps = append(gaps, cur.NextAttemptAt.Sub(cur.UpdatedAt))
		}
		prev = cur
		return cur.Status == saga.StatusCompensated
	})

	if final.CompRetryCount != 4 {
		t.Fatalf("應重試 4 次，實際 %d", final.CompRetryCount)
	}
	t.Logf("退避間隔: %v", gaps)
	// 第 n 次的間隔落在 [base·2^(n-1)/2, base·2^(n-1)]，相鄰兩次的區間剛好首尾相接，
	// 所以即使有抖動，間隔也一定是非遞減的
	for i := 1; i < len(gaps); i++ {
		if gaps[i] < gaps[i-1] {
			t.Errorf("退避間隔應隨重試次數成長: %v", gaps)
		}
	}
	if len(gaps) < 3 {
		t.Errorf("應記錄到至少 3 次退避間隔，實際 %v", gaps)
	}
	e.assertState(t, startBalance, startStock, 0)
	if e.alerts.Count(saga.AlertWarning) != 1 {
		t.Errorf("重試達 3 次應發 1 次低優先級告警，實際 %d", e.alerts.Count(saga.AlertWarning))
	}
	if e.alerts.Count(saga.AlertCritical) != 0 {
		t.Errorf("最終有成功，不該發高優先級告警")
	}
}

// 補償超過總時限仍失敗 → needs_manual + 高優先級告警。
func TestCompensationDeadline_EscalatesToManual(t *testing.T) {
	e := newEnv(t)
	e.chaos["grant_item"].SetExecFailures(-1)
	e.chaos["deduct_points"].SetCompFailures(-1) // 退點永遠失敗

	e.redeem(t, "r-manual")
	final := e.runWorkerUntil(t, "r-manual", 30*time.Second, 200, func(s *saga.Saga) bool {
		return s.Status == saga.StatusNeedsManual
	})

	if final.FirstFailedAt == nil || e.clk.Now().Sub(*final.FirstFailedAt) < 10*time.Minute {
		t.Fatalf("應在超過 10 分鐘時限後才升級")
	}
	if e.alerts.Count(saga.AlertWarning) != 1 || e.alerts.Count(saga.AlertCritical) != 1 {
		t.Fatalf("分層告警應為 warning×1、critical×1，實際 %+v", e.alerts.Alerts)
	}
	// 升級後 worker 不再處理
	e.clk.Advance(time.Hour)
	if n, _ := e.orch.RecoverOnce(e.ctx); n != 0 {
		t.Fatalf("needs_manual 不該再被自動處理")
	}
	e.assertState(t, startBalance-cost, startStock, 0) // 道具、庫存已復原，點數待人工
}

// 同一個兌換請求被併發送 10 次：只會兌換一次。
func TestConcurrentSameRedeem_OnlyOnce(t *testing.T) {
	e := newEnv(t)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.orch.Start(e.ctx, saga.Request{RequestID: "r-dup", UserID: userID, ItemID: itemID, Cost: cost}); err != nil {
				t.Errorf("Start 失敗: %v", err)
			}
		}()
	}
	wg.Wait()

	if s := e.get(t, "r-dup"); s.Status != saga.StatusCompleted {
		t.Fatalf("狀態 = %s，期望 completed", s.Status)
	}
	e.assertState(t, startBalance-cost, startStock-1, 1)
}

// 相同 request_id 帶不同參數：視為客戶端錯誤，拒絕。
func TestSameRequestIDDifferentPayload_Rejected(t *testing.T) {
	e := newEnv(t)
	e.redeem(t, "r-mismatch")
	_, err := e.orch.Start(e.ctx, saga.Request{RequestID: "r-mismatch", UserID: userID, ItemID: itemID, Cost: cost + 1})
	if err == nil {
		t.Fatal("參數不同應被拒絕")
	}
}

// 情境：程式在「扣完點、扣完庫存，但還沒來得及把進度寫回 saga 表」時崩潰。
// saga 表只記到 current_step = 1，實際上第 2 步也已經生效。
// 排程撈到「太久沒做完」的 saga → 去流水表查證 → 依證據補償，並把查證結果記下來。
func TestCrashedSaga_ProbedFromLedgerAndCompensated(t *testing.T) {
	e := newEnv(t)
	store := e.orch.Store()

	// 1) 模擬崩潰前的現場
	s, err := store.CreateOrGet(e.ctx, saga.Request{RequestID: "r-crash", UserID: userID, ItemID: itemID, Cost: cost}, e.clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := store.Claim(e.ctx, s.ID, "crashed-instance", e.clk.Now(), 30*time.Second); !ok {
		t.Fatal("claim 失敗")
	}
	if err := (saga.DeductPointsStep{Svc: e.points}).Execute(e.ctx, s); err != nil {
		t.Fatal(err)
	}
	s.CurrentStep = 1
	if err := store.Save(e.ctx, s, "crashed-instance", e.clk.Now(), 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := (saga.DeductStockStep{Svc: e.inv}).Execute(e.ctx, s); err != nil {
		t.Fatal(err)
	}
	// ← 這裡程式崩潰：第 2 步生效了，但 current_step 還是 1，租約也沒釋放

	e.assertState(t, startBalance-cost, startStock-1, 0)

	// 2) 還沒超過門檻，排程不該動它
	e.clk.Advance(time.Minute)
	if n, _ := e.orch.RecoverOnce(e.ctx); n != 0 {
		t.Fatal("未超過 StuckPendingAfter 不該處理")
	}

	// 3) 超過門檻 → 查證流水 → 補償
	e.clk.Advance(2 * time.Minute)
	if n, _ := e.orch.RecoverOnce(e.ctx); n != 1 {
		t.Fatal("應處理 1 筆中斷的 saga")
	}

	final := e.get(t, "r-crash")
	if final.Status != saga.StatusCompensated {
		t.Fatalf("狀態 = %s，期望 compensated（%s）", final.Status, final.LastError)
	}
	for _, want := range []string{"deduct_points=applied", "deduct_stock=applied", "grant_item=none"} {
		if !strings.Contains(final.LastError, want) {
			t.Errorf("查證紀錄應包含 %q，實際: %s", want, final.LastError)
		}
	}
	t.Logf("排程留下的查證紀錄: %s", final.LastError)
	assertOrder(t, e.compLog.order, "deduct_stock", "deduct_points")
	e.assertState(t, startBalance, startStock, 0)

	// 補償完成後再查一次：每一步的流水都對得上
	for _, p := range e.orch.Probe(e.ctx, final) {
		t.Logf("查證 %-14s → %s", p.Step, p.State)
	}
}

// 情境：三步都成功、current_step 也寫到 3，但在寫入 completed 之前崩潰。
// 這筆兌換其實已經完成，排程不能把它當成「中斷」整筆退掉。
func TestCrashedAfterAllStepsApplied_MarkedCompleted(t *testing.T) {
	e := newEnv(t)
	store := e.orch.Store()

	s, err := store.CreateOrGet(e.ctx, saga.Request{RequestID: "r-done", UserID: userID, ItemID: itemID, Cost: cost}, e.clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := store.Claim(e.ctx, s.ID, "crashed-instance", e.clk.Now(), 30*time.Second); !ok {
		t.Fatal("claim 失敗")
	}
	for _, st := range []saga.Step{
		saga.DeductPointsStep{Svc: e.points},
		saga.DeductStockStep{Svc: e.inv},
		saga.GrantItemStep{Svc: e.items},
	} {
		if err := st.Execute(e.ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	s.CurrentStep = 3
	if err := store.Save(e.ctx, s, "crashed-instance", e.clk.Now(), 30*time.Second); err != nil {
		t.Fatal(err)
	}
	// ← 這裡程式崩潰：status 還是 pending

	e.clk.Advance(3 * time.Minute)
	if _, err := e.orch.RecoverOnce(e.ctx); err != nil {
		t.Fatal(err)
	}

	final := e.get(t, "r-done")
	if final.Status != saga.StatusCompleted {
		t.Fatalf("狀態 = %s，期望 completed（%s）", final.Status, final.LastError)
	}
	if len(e.compLog.order) != 0 {
		t.Fatalf("已完成的兌換不該被補償，實際補償了: %v", e.compLog.order)
	}
	e.assertState(t, startBalance-cost, startStock-1, 1)
}
