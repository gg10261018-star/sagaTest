package saga

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"saga-points-demo/internal/db"
	"saga-points-demo/internal/errs"
)

var (
	ErrNotFound        = errors.New("saga 不存在")
	ErrLeaseLost       = errors.New("租約已失效，其他處理者接手了這個 saga")
	ErrRequestMismatch = fmt.Errorf("%w: 相同 request_id 卻帶了不同參數", errs.ErrPermanent)
)

// Store 負責 sagas 表的存取。
//
// 租約（lease）機制：要推進某個 saga 前，必須先用一條「條件式 UPDATE」搶到租約。
// 這讓「同一個 request_id 併發進來的兩個請求」、或「多台機器上的背景 worker」
// 不會同時推進同一個 saga。之後每次寫回狀態都帶 WHERE lease_owner = ?，
// 如果租約過期被別人接手，寫回會失敗（fencing），舊的處理者就知道該停手。
type Store struct {
	db *sql.DB
}

func NewStore(conn *sql.DB) *Store { return &Store{db: conn} }

// CreateOrGet 建立 saga；request_id 已存在時回傳既有的（saga 層級的冪等）。
func (st *Store) CreateOrGet(ctx context.Context, req Request, now time.Time) (*Saga, error) {
	_, err := st.db.ExecContext(ctx,
		`INSERT INTO sagas (request_id, user_id, item_id, cost, status, current_step, comp_next,
		                    comp_retry_count, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, 0, 0, 0, ?, ?)`,
		req.RequestID, req.UserID, req.ItemID, req.Cost, StatusPending, now, now)
	if err != nil && !db.IsDuplicateKey(err) {
		return nil, err
	}
	s, err := st.Get(ctx, req.RequestID)
	if err != nil {
		return nil, err
	}
	// 冪等鍵相同但內容不同 → 通常是客戶端 bug，不能默默當成同一筆
	if s.UserID != req.UserID || s.ItemID != req.ItemID || s.Cost != req.Cost {
		return nil, ErrRequestMismatch
	}
	return s, nil
}

func (st *Store) Get(ctx context.Context, requestID string) (*Saga, error) {
	return st.scanOne(ctx, `WHERE request_id = ?`, requestID)
}

func (st *Store) getByID(ctx context.Context, id int64) (*Saga, error) {
	return st.scanOne(ctx, `WHERE id = ?`, id)
}

func (st *Store) scanOne(ctx context.Context, where string, arg any) (*Saga, error) {
	var (
		s             Saga
		firstFailedAt sql.NullTime
		nextAttemptAt sql.NullTime
		lastError     sql.NullString
	)
	err := st.db.QueryRowContext(ctx,
		`SELECT id, request_id, user_id, item_id, cost, status, current_step, comp_next,
		        comp_retry_count, first_failed_at, next_attempt_at, last_error, created_at, updated_at
		 FROM sagas `+where, arg).
		Scan(&s.ID, &s.RequestID, &s.UserID, &s.ItemID, &s.Cost, &s.Status, &s.CurrentStep, &s.CompNext,
			&s.CompRetryCount, &firstFailedAt, &nextAttemptAt, &lastError, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if firstFailedAt.Valid {
		t := firstFailedAt.Time
		s.FirstFailedAt = &t
	}
	if nextAttemptAt.Valid {
		t := nextAttemptAt.Time
		s.NextAttemptAt = &t
	}
	s.LastError = lastError.String
	return &s, nil
}

// Claim 嘗試取得租約。只有「沒人持有」或「租約已過期」時才會成功。
func (st *Store) Claim(ctx context.Context, id int64, owner string, now time.Time, lease time.Duration) (bool, error) {
	res, err := st.db.ExecContext(ctx,
		`UPDATE sagas SET lease_owner = ?, lease_until = ?
		 WHERE id = ? AND (lease_until IS NULL OR lease_until < ?)`,
		owner, now.Add(lease), id, now)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (st *Store) Release(ctx context.Context, id int64, owner string) error {
	_, err := st.db.ExecContext(ctx,
		`UPDATE sagas SET lease_owner = NULL, lease_until = NULL WHERE id = ? AND lease_owner = ?`, id, owner)
	return err
}

// Save 寫回 saga 狀態，順便延長租約。租約已不屬於自己時回傳 ErrLeaseLost。
func (st *Store) Save(ctx context.Context, s *Saga, owner string, now time.Time, lease time.Duration) error {
	s.UpdatedAt = now
	res, err := st.db.ExecContext(ctx,
		`UPDATE sagas SET status = ?, current_step = ?, comp_next = ?, comp_retry_count = ?,
		        first_failed_at = ?, next_attempt_at = ?, last_error = ?, updated_at = ?, lease_until = ?
		 WHERE id = ? AND lease_owner = ?`,
		s.Status, s.CurrentStep, s.CompNext, s.CompRetryCount,
		nullTime(s.FirstFailedAt), nullTime(s.NextAttemptAt), s.LastError, now, now.Add(lease),
		s.ID, owner)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrLeaseLost
	}
	return nil
}

// FindRecoverable 找出背景 worker 該處理的 saga：
//  1. 補償中、且已到下次重試時間
//  2. 卡在 pending 太久、且沒有有效租約（處理到一半程式崩潰了）
func (st *Store) FindRecoverable(ctx context.Context, now, stuckBefore time.Time, limit int) ([]int64, error) {
	rows, err := st.db.QueryContext(ctx,
		`SELECT id FROM sagas
		 WHERE (status = ? AND next_attempt_at <= ?)
		    OR (status = ? AND updated_at < ? AND (lease_until IS NULL OR lease_until < ?))
		 ORDER BY id LIMIT ?`,
		StatusCompensating, now, StatusPending, stuckBefore, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func nullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *t, Valid: true}
}
