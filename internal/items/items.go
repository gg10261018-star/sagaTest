// Package items 是「道具服務」：發放道具給使用者。
//
// 發放本身是單純的 INSERT，request_id 唯一鍵就足以保證冪等，不需要鎖。
// 補償 = 把狀態改成 revoked（收回），一樣具冪等性。
package items

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
	StatusGranted = "granted"
	StatusRevoked = "revoked"
	StatusVoid    = "void"
)

var ErrCancelled = fmt.Errorf("%w: 此請求已被補償流程作廢", errs.ErrPermanent)

type Service struct {
	db    *sql.DB
	clock clock.Clock
}

func NewService(conn *sql.DB, c clock.Clock) *Service { return &Service{db: conn, clock: c} }

// Grant 發放道具，具冪等性。
func (s *Service) Grant(ctx context.Context, requestID string, userID, itemID int64) error {
	now := s.clock.Now()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO user_items (request_id, user_id, item_id, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`, requestID, userID, itemID, StatusGranted, now, now)
	if err == nil {
		return nil
	}
	if !db.IsDuplicateKey(err) {
		return err
	}
	status, err := s.Status(ctx, requestID)
	if err != nil {
		return err
	}
	if status == StatusGranted {
		return nil // 重複請求，冪等成功
	}
	return ErrCancelled // 已被收回或作廢，晚到的請求不能讓它復活
}

// Compensate 收回道具。正向紀錄不存在時寫入 void 作廢標記（防懸掛）。
func (s *Service) Compensate(ctx context.Context, forwardRequestID string, userID, itemID int64) error {
	for attempt := 0; attempt < 3; attempt++ {
		now := s.clock.Now()
		res, err := s.db.ExecContext(ctx,
			`UPDATE user_items SET status = ?, updated_at = ? WHERE request_id = ? AND status = ?`,
			StatusRevoked, now, forwardRequestID, StatusGranted)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			return nil
		}

		status, err := s.Status(ctx, forwardRequestID)
		if err != nil {
			return err
		}
		if status == StatusRevoked || status == StatusVoid {
			return nil // 補償重複觸發
		}

		// 正向紀錄不存在 → 空補償，寫作廢標記
		_, err = s.db.ExecContext(ctx,
			`INSERT INTO user_items (request_id, user_id, item_id, status, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)`, forwardRequestID, userID, itemID, StatusVoid, now, now)
		if db.IsDuplicateKey(err) {
			continue // 正向剛好搶先寫入 → 重來一輪，改走 UPDATE 收回
		}
		return err
	}
	return fmt.Errorf("補償 %s 時反覆發生衝突", forwardRequestID)
}

// Probe 查證某筆發放的真實狀態：none / applied / compensated / voided。
func (s *Service) Probe(ctx context.Context, forwardRequestID string) (string, error) {
	st, err := s.Status(ctx, forwardRequestID)
	if err != nil {
		return "", err
	}
	switch st {
	case "":
		return "none", nil
	case StatusGranted:
		return "applied", nil
	case StatusRevoked:
		return "compensated", nil
	default:
		return "voided", nil
	}
}

// Status 回傳某筆發放的狀態，不存在回空字串。
func (s *Service) Status(ctx context.Context, requestID string) (string, error) {
	var st string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM user_items WHERE request_id = ?`, requestID).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return st, err
}

// CountGranted 計算使用者目前「有效持有」的某道具數量。
func (s *Service) CountGranted(ctx context.Context, userID, itemID int64) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM user_items WHERE user_id = ? AND item_id = ? AND status = ?`,
		userID, itemID, StatusGranted).Scan(&n)
	return n, err
}
