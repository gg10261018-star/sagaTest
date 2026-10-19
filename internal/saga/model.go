// Package saga 實作「兌換道具」的 Saga 協調器（Orchestration 模式）。
//
// 正向流程：Step1 扣點數 → Step2 扣庫存 → Step3 發放道具
// 任何一步失敗：從失敗點「反向、接力」補償（先補 Step2，再補 Step1），不能跳步。
package saga

import (
	"time"
)

type Status string

const (
	StatusPending      Status = "pending"      // 正向流程進行中
	StatusCompleted    Status = "completed"    // 全部成功
	StatusFailed       Status = "failed"       // 第一步就被業務拒絕（例如點數不足），沒有東西需要補償
	StatusCompensating Status = "compensating" // 補償中（可能在等背景重試）
	StatusCompensated  Status = "compensated"  // 補償完成，系統回到一致狀態
	StatusNeedsManual  Status = "needs_manual" // 補償超過時限仍失敗，升級人工介入
)

type Saga struct {
	ID             int64      `json:"-"`
	RequestID      string     `json:"request_id"`
	UserID         int64      `json:"user_id"`
	ItemID         int64      `json:"item_id"`
	Cost           int64      `json:"cost"`
	Status         Status     `json:"status"`
	CurrentStep    int        `json:"current_step"` // 已確認成功的正向步驟數
	CompNext       int        `json:"comp_next"`    // 下一個要補償的步驟（1-based），0 = 補償完成
	CompRetryCount int        `json:"comp_retry_count"`
	FirstFailedAt  *time.Time `json:"first_failed_at,omitempty"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// Request 是使用者發起的一次兌換。
type Request struct {
	RequestID string `json:"request_id"` // 客戶端產生的冪等鍵
	UserID    int64  `json:"user_id"`
	ItemID    int64  `json:"item_id"`
	Cost      int64  `json:"cost"`
}

func (s *Saga) Terminal() bool {
	switch s.Status {
	case StatusCompleted, StatusFailed, StatusCompensated, StatusNeedsManual:
		return true
	}
	return false
}
