package saga

import (
	"context"
	"log/slog"
	"sync"
)

type AlertLevel string

const (
	// AlertWarning 補償已重試多次仍失敗：低優先級提醒，先讓人知道有狀況
	AlertWarning AlertLevel = "warning"
	// AlertCritical 超過補償時限：高優先級，必須人工介入
	AlertCritical AlertLevel = "critical"
)

// Alerter 實務上會接 Slack / Telegram（像預警系統那樣），這裡抽成介面。
type Alerter interface {
	Alert(ctx context.Context, level AlertLevel, s *Saga, message string)
}

type LogAlerter struct{ Logger *slog.Logger }

func (a LogAlerter) Alert(_ context.Context, level AlertLevel, s *Saga, message string) {
	log := a.Logger
	if log == nil {
		log = slog.Default()
	}
	attrs := []any{"level", level, "request_id", s.RequestID, "user_id", s.UserID,
		"comp_retry_count", s.CompRetryCount, "last_error", s.LastError}
	if level == AlertCritical {
		log.Error("[SAGA 告警] "+message, attrs...)
	} else {
		log.Warn("[SAGA 告警] "+message, attrs...)
	}
}

// RecordingAlerter 測試用，把告警記下來方便斷言。
type RecordingAlerter struct {
	mu     sync.Mutex
	Alerts []RecordedAlert
}

type RecordedAlert struct {
	Level     AlertLevel
	RequestID string
	Message   string
}

func (r *RecordingAlerter) Alert(_ context.Context, level AlertLevel, s *Saga, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Alerts = append(r.Alerts, RecordedAlert{Level: level, RequestID: s.RequestID, Message: message})
}

func (r *RecordingAlerter) Count(level AlertLevel) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, a := range r.Alerts {
		if a.Level == level {
			n++
		}
	}
	return n
}
