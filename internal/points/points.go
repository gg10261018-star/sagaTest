// Package points 是「點數服務」，所有異動都記錄在 points_ledger（點數流水表）。
//
// 核心函式 ApplyPointsChange 實作三層冪等防護（對應 review-saga-idempotency.md）：
//
//	第一層：先查流水表的 request_id，處理過就直接重放結果（最快，但檢查時間點太早）
//	第二層：SELECT ... FOR UPDATE 鎖住餘額列，避免併發算出錯誤餘額；
//	        拿到鎖之後「再檢查一次 request_id」，解決排隊者醒來不會重新檢查的問題
//	第三層：points_ledger.request_id 唯一鍵（最終防線），撞到 1062 就回頭重放
package points

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"saga-points-demo/internal/clock"
	"saga-points-demo/internal/db"
	"saga-points-demo/internal/errs"
)

// 流水業務類型
const (
	BizRedeemDeduct = "redeem_deduct" // 兌換扣點
	BizRedeemRefund = "redeem_refund" // 兌換失敗退點（補償）
	BizAdminAdjust  = "admin_adjust"  // 管理員調整 / 儲值
	BizVoid         = "void"          // 作廢標記（防懸掛），delta = 0
)

var (
	ErrInsufficient = fmt.Errorf("%w: 點數不足", errs.ErrPermanent)
	ErrCancelled    = fmt.Errorf("%w: 此請求已被補償流程作廢", errs.ErrPermanent)
	ErrInvalidDelta = fmt.Errorf("%w: 異動點數不可為 0", errs.ErrPermanent)
)

// Change 是一次點數異動的請求。
type Change struct {
	RequestID     string // 冪等鍵（必填）
	SagaRequestID string // 屬於哪一次兌換（非 Saga 發起的異動可留空）
	UserID        int64
	Delta         int64 // 正數加點、負數扣點
	BizType       string
}

// Entry 是流水表的一列。
type Entry struct {
	RequestID     string    `json:"request_id"`
	SagaRequestID string    `json:"saga_request_id,omitempty"`
	UserID        int64     `json:"user_id"`
	BizType       string    `json:"biz_type"`
	Delta         int64     `json:"delta"`
	BalanceBefore int64     `json:"balance_before"`
	BalanceAfter  int64     `json:"balance_after"`
	CreatedAt     time.Time `json:"created_at"`
	// Replayed = true 代表這次沒有真的執行，是把之前的結果原封不動回傳
	Replayed bool `json:"replayed,omitempty"`
}

type Service struct {
	db    *sql.DB
	clock clock.Clock
}

func NewService(conn *sql.DB, c clock.Clock) *Service {
	return &Service{db: conn, clock: c}
}

// CompensationID 從正向 request_id 衍生出固定的補償 request_id。
// 補償被重複觸發時（背景重試不確定上一次有沒有成功），會撞同一個唯一鍵而被擋下。
func CompensationID(forwardRequestID string) string { return "comp:" + forwardRequestID }

// ApplyPointsChange 對使用者點數做加減，具冪等性，並寫入一筆流水。
func (s *Service) ApplyPointsChange(ctx context.Context, c Change) (*Entry, error) {
	if c.Delta == 0 {
		return nil, ErrInvalidDelta
	}
	if c.BizType == "" {
		c.BizType = BizAdminAdjust
	}

	// ── 第一層：快速攔截 ──────────────────────────────────────────────
	// 大部分「使用者連點 / 網路重送」都在這裡就被擋下，不必開交易、不必搶鎖。
	existing, err := findByRequestID(ctx, s.db, c.RequestID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return replay(existing)
	}

	var result *Entry
	err = db.WithDeadlockRetry(ctx, 5, func() error {
		var txErr error
		result, txErr = s.applyInTx(ctx, c)
		return txErr
	})

	// ── 第三層：唯一鍵兜底 ────────────────────────────────────────────
	// 前兩層都因為時間差沒攔住時，寫入那一刻資料庫一定會擋下第二筆。
	// 判斷到 1062 後，回頭查「搶先成功的那一筆」，原封不動回傳給慢了一步的請求。
	if db.IsDuplicateKey(err) {
		return s.replayByRequestID(ctx, c.RequestID)
	}
	return result, err
}

func (s *Service) applyInTx(ctx context.Context, c Change) (*Entry, error) {
	now := s.clock.Now()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // Commit 成功後 Rollback 是 no-op

	// ── 第二層：悲觀鎖 ────────────────────────────────────────────────
	balance, found, err := lockBalance(ctx, tx, c.UserID)
	if err != nil {
		return nil, err
	}
	if !found {
		// 漏洞修補：FOR UPDATE 只能鎖「已存在」的列。新使用者第一次操作時鎖不到任何東西，
		// 兩個併發請求可能同時決定 INSERT。所以 INSERT 撞到唯一鍵時，
		// 代表對方先建好了 → 改用 FOR UPDATE 鎖住對方那一筆，沿用結果繼續往下走。
		_, err = tx.ExecContext(ctx,
			`INSERT INTO user_points (user_id, balance, updated_at) VALUES (?, 0, ?)`, c.UserID, now)
		switch {
		case err == nil:
			balance = 0 // 自己 INSERT 的列，本交易已持有它的排他鎖
		case db.IsDuplicateKey(err):
			balance, found, err = lockBalance(ctx, tx, c.UserID)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fmt.Errorf("建立使用者 %d 點數帳戶時發生不一致", c.UserID)
			}
		default:
			return nil, err
		}
	}

	// 鎖內二次檢查：
	// 「鎖只保證不會同時執行，不保證排隊的請求醒來後會重新檢查 requestID」。
	// 所以拿到鎖之後補查一次。這一步很重要的情境是「餘額剛好只夠扣一次」：
	// 如果不補查，排隊者醒來會看到餘額已被扣掉，誤回「點數不足」，
	// 但它其實是同一筆請求的重送，應該回「成功（重放）」才對。
	//
	// 注意：這裡是一般 SELECT（非鎖定讀）。InnoDB 在 Repeatable Read 下，
	// 快照是在交易中「第一個非鎖定讀」時才建立；前面只有 FOR UPDATE（鎖定讀），
	// 所以這裡建立的快照時間點在拿到鎖之後，看得到對方已 commit 的紀錄。
	if existing, err := findByRequestID(ctx, tx, c.RequestID); err != nil {
		return nil, err
	} else if existing != nil {
		return replay(existing)
	}

	newBalance := balance + c.Delta
	if newBalance < 0 {
		return nil, ErrInsufficient
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE user_points SET balance = ?, updated_at = ? WHERE user_id = ?`,
		newBalance, now, c.UserID); err != nil {
		return nil, err
	}

	entry := &Entry{
		RequestID: c.RequestID, SagaRequestID: c.SagaRequestID, UserID: c.UserID,
		BizType: c.BizType, Delta: c.Delta, BalanceBefore: balance, BalanceAfter: newBalance, CreatedAt: now,
	}
	if err := insertEntry(ctx, tx, entry); err != nil {
		return nil, err // 1062 交給外層的第三層處理；defer 會回滾上面的餘額異動
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return entry, nil
}

// Compensate 抵銷某一筆正向異動（Saga 補償用），具冪等性，並處理兩種邊界情況：
//
//   - 空補償：正向請求根本沒生效（例如請求在網路上就遺失了），卻收到補償 → 不能憑空退點
//   - 懸掛：補償先到、正向請求因為網路延遲「晚到」→ 晚到的正向請求不能再生效
//
// 做法：先查流水表。正向流水不存在時，用「正向 request_id」寫入一筆 void 作廢標記。
// 之後晚到的正向請求會撞到唯一鍵，重放時看到 void，回傳 ErrCancelled。
// 誰先寫入誰贏，一樣由唯一鍵裁決。
func (s *Service) Compensate(ctx context.Context, forwardRequestID, sagaRequestID string, userID int64) error {
	for attempt := 0; attempt < 3; attempt++ {
		fwd, err := findByRequestID(ctx, s.db, forwardRequestID)
		if err != nil {
			return err
		}

		if fwd == nil {
			err := insertEntry(ctx, s.db, &Entry{
				RequestID: forwardRequestID, SagaRequestID: sagaRequestID, UserID: userID,
				BizType: BizVoid, CreatedAt: s.clock.Now(),
			})
			if db.IsDuplicateKey(err) {
				continue // 正向請求剛好搶先寫入了 → 重新判斷，改走正常補償
			}
			return err
		}

		if fwd.BizType == BizVoid {
			return nil // 已經作廢過，補償重複觸發 → 什麼都不用做
		}

		// 反向操作：扣了 100 就加回 100。補償 request_id 固定，重複觸發也只會生效一次。
		_, err = s.ApplyPointsChange(ctx, Change{
			RequestID:     CompensationID(forwardRequestID),
			SagaRequestID: fwd.SagaRequestID,
			UserID:        fwd.UserID,
			Delta:         -fwd.Delta,
			BizType:       BizRedeemRefund,
		})
		return err
	}
	return fmt.Errorf("補償 %s 時反覆發生衝突", forwardRequestID)
}

// 查證結果
const (
	ProbeNone        = "none"        // 流水裡沒有這筆正向異動：沒扣過
	ProbeApplied     = "applied"     // 有扣，且尚未退回
	ProbeCompensated = "compensated" // 有扣，也已經退回
	ProbeVoided      = "voided"      // 已被作廢標記佔位
)

// Probe 查證某筆正向異動的真實狀態——排程撈到「太久沒做完」的兌換時，
// 用它回答「上次到底有沒有扣到點數？退了沒？」。
func (s *Service) Probe(ctx context.Context, forwardRequestID string) (string, error) {
	fwd, err := findByRequestID(ctx, s.db, forwardRequestID)
	if err != nil {
		return "", err
	}
	switch {
	case fwd == nil:
		return ProbeNone, nil
	case fwd.BizType == BizVoid:
		return ProbeVoided, nil
	}
	comp, err := findByRequestID(ctx, s.db, CompensationID(forwardRequestID))
	if err != nil {
		return "", err
	}
	if comp != nil {
		return ProbeCompensated, nil
	}
	return ProbeApplied, nil
}

// FindEntry 用 request_id 查單筆流水。
func (s *Service) FindEntry(ctx context.Context, requestID string) (*Entry, error) {
	return findByRequestID(ctx, s.db, requestID)
}

// ListBySaga 列出某一次兌換相關的所有流水（正向扣點、補償退點、作廢標記），對帳用。
func (s *Service) ListBySaga(ctx context.Context, sagaRequestID string) ([]Entry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT request_id, COALESCE(saga_request_id, ''), user_id, biz_type, delta,
		        balance_before, balance_after, created_at
		 FROM points_ledger WHERE saga_request_id = ? ORDER BY id`, sagaRequestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.RequestID, &e.SagaRequestID, &e.UserID, &e.BizType, &e.Delta,
			&e.BalanceBefore, &e.BalanceAfter, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Reconcile 對帳：檢查「目前餘額」是否等於「流水 delta 加總」。
// 流水表只新增不修改，所以它是事實的來源；餘額只是流水的快取結果。
func (s *Service) Reconcile(ctx context.Context, userID int64) (balance, ledgerSum int64, err error) {
	if balance, err = s.Balance(ctx, userID); err != nil {
		return
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(delta), 0) FROM points_ledger WHERE user_id = ?`, userID).Scan(&ledgerSum)
	return
}

// Balance 查詢目前餘額（帳戶不存在視為 0）。
func (s *Service) Balance(ctx context.Context, userID int64) (int64, error) {
	var b int64
	err := s.db.QueryRowContext(ctx, `SELECT balance FROM user_points WHERE user_id = ?`, userID).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return b, err
}

func (s *Service) replayByRequestID(ctx context.Context, requestID string) (*Entry, error) {
	existing, err := findByRequestID(ctx, s.db, requestID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("request_id %s 撞到唯一鍵，卻查不到既有紀錄", requestID)
	}
	return replay(existing)
}

func replay(e *Entry) (*Entry, error) {
	if e.BizType == BizVoid {
		return nil, ErrCancelled
	}
	cp := *e
	cp.Replayed = true
	return &cp, nil
}

func insertEntry(ctx context.Context, q db.Querier, e *Entry) error {
	var sagaID any
	if e.SagaRequestID != "" {
		sagaID = e.SagaRequestID
	}
	_, err := q.ExecContext(ctx,
		`INSERT INTO points_ledger (request_id, saga_request_id, user_id, biz_type, delta,
		                            balance_before, balance_after, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		e.RequestID, sagaID, e.UserID, e.BizType, e.Delta, e.BalanceBefore, e.BalanceAfter, e.CreatedAt)
	return err
}

func lockBalance(ctx context.Context, tx *sql.Tx, userID int64) (balance int64, found bool, err error) {
	err = tx.QueryRowContext(ctx,
		`SELECT balance FROM user_points WHERE user_id = ? FOR UPDATE`, userID).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return balance, true, nil
}

func findByRequestID(ctx context.Context, q db.Querier, requestID string) (*Entry, error) {
	var e Entry
	err := q.QueryRowContext(ctx,
		`SELECT request_id, COALESCE(saga_request_id, ''), user_id, biz_type, delta,
		        balance_before, balance_after, created_at
		 FROM points_ledger WHERE request_id = ?`, requestID).
		Scan(&e.RequestID, &e.SagaRequestID, &e.UserID, &e.BizType, &e.Delta,
			&e.BalanceBefore, &e.BalanceAfter, &e.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}
