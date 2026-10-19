// Package inventory 是「庫存服務」。
//
// 冪等設計跟點數服務是同一套模式（查詢攔截 → FOR UPDATE → 鎖內二次檢查 → 唯一鍵兜底），
// 刻意各自實作一份，因為真實世界裡它們是不同團隊、不同資料庫的獨立服務。
// 差別在於：庫存品項必須事先存在，不會自動建立。
package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"saga-points-demo/internal/clock"
	"saga-points-demo/internal/db"
	"saga-points-demo/internal/errs"
)

const (
	KindNormal = "normal"
	KindVoid   = "void"
)

var (
	ErrOutOfStock   = fmt.Errorf("%w: 庫存不足", errs.ErrPermanent)
	ErrItemNotFound = fmt.Errorf("%w: 品項不存在", errs.ErrPermanent)
	ErrCancelled    = fmt.Errorf("%w: 此請求已被補償流程作廢", errs.ErrPermanent)
)

type record struct {
	itemID int64
	delta  int64
	kind   string
}

type Service struct {
	db    *sql.DB
	clock clock.Clock
}

func NewService(conn *sql.DB, c clock.Clock) *Service { return &Service{db: conn, clock: c} }

func CompensationID(forwardRequestID string) string { return "comp:" + forwardRequestID }

// Apply 調整庫存（負數扣庫存、正數補回），具冪等性。
func (s *Service) Apply(ctx context.Context, requestID string, itemID, delta int64) error {
	if rec, err := find(ctx, s.db, requestID); err != nil {
		return err
	} else if rec != nil {
		return replayErr(rec)
	}

	err := db.WithDeadlockRetry(ctx, 5, func() error { return s.applyInTx(ctx, requestID, itemID, delta) })
	if db.IsDuplicateKey(err) {
		rec, ferr := find(ctx, s.db, requestID)
		if ferr != nil {
			return ferr
		}
		if rec == nil {
			return fmt.Errorf("request_id %s 撞到唯一鍵，卻查不到既有紀錄", requestID)
		}
		return replayErr(rec)
	}
	return err
}

func (s *Service) applyInTx(ctx context.Context, requestID string, itemID, delta int64) error {
	now := s.clock.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	var stock int64
	err = tx.QueryRowContext(ctx, `SELECT stock FROM inventory WHERE item_id = ? FOR UPDATE`, itemID).Scan(&stock)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrItemNotFound
	}
	if err != nil {
		return err
	}

	if rec, err := find(ctx, tx, requestID); err != nil {
		return err
	} else if rec != nil {
		return replayErr(rec)
	}

	newStock := stock + delta
	if newStock < 0 {
		return ErrOutOfStock
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE inventory SET stock = ?, updated_at = ? WHERE item_id = ?`, newStock, now, itemID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO inventory_transactions (request_id, item_id, delta, stock_after, kind, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`, requestID, itemID, delta, newStock, KindNormal, now); err != nil {
		return err
	}
	return tx.Commit()
}

// Compensate 抵銷正向扣庫存，處理空補償與懸掛（說明見 points.Compensate）。
func (s *Service) Compensate(ctx context.Context, forwardRequestID string, itemID int64) error {
	for attempt := 0; attempt < 3; attempt++ {
		fwd, err := find(ctx, s.db, forwardRequestID)
		if err != nil {
			return err
		}
		if fwd == nil {
			_, err := s.db.ExecContext(ctx,
				`INSERT INTO inventory_transactions (request_id, item_id, delta, stock_after, kind, created_at)
				 VALUES (?, ?, 0, 0, ?, ?)`, forwardRequestID, itemID, KindVoid, s.clock.Now())
			if db.IsDuplicateKey(err) {
				continue
			}
			return err
		}
		if fwd.kind == KindVoid {
			return nil
		}
		return s.Apply(ctx, CompensationID(forwardRequestID), fwd.itemID, -fwd.delta)
	}
	return fmt.Errorf("補償 %s 時反覆發生衝突", forwardRequestID)
}

// Probe 查證某筆正向扣庫存的真實狀態：none / applied / compensated / voided。
func (s *Service) Probe(ctx context.Context, forwardRequestID string) (string, error) {
	fwd, err := find(ctx, s.db, forwardRequestID)
	if err != nil {
		return "", err
	}
	switch {
	case fwd == nil:
		return "none", nil
	case fwd.kind == KindVoid:
		return "voided", nil
	}
	comp, err := find(ctx, s.db, CompensationID(forwardRequestID))
	if err != nil {
		return "", err
	}
	if comp != nil {
		return "compensated", nil
	}
	return "applied", nil
}

func (s *Service) Stock(ctx context.Context, itemID int64) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx, `SELECT stock FROM inventory WHERE item_id = ?`, itemID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrItemNotFound
	}
	return v, err
}

// SetStock 管理用：直接設定庫存（示範與測試用）。
func (s *Service) SetStock(ctx context.Context, itemID, stock int64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO inventory (item_id, stock, updated_at) VALUES (?, ?, ?)
		 ON DUPLICATE KEY UPDATE stock = VALUES(stock), updated_at = VALUES(updated_at)`,
		itemID, stock, s.clock.Now())
	return err
}

func replayErr(r *record) error {
	if r.kind == KindVoid {
		return ErrCancelled
	}
	return nil // 已處理過，冪等地回傳成功
}

func find(ctx context.Context, q db.Querier, requestID string) (*record, error) {
	var r record
	err := q.QueryRowContext(ctx,
		`SELECT item_id, delta, kind FROM inventory_transactions WHERE request_id = ?`, requestID).
		Scan(&r.itemID, &r.delta, &r.kind)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}
