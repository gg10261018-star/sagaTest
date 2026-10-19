// Package clock 把「現在幾點」抽成介面，測試時可以快轉時間，
// 不用真的等指數退避或補償時限。
package clock

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

// Real 使用系統時間（UTC，毫秒精度，對齊 DATETIME(3)）。
type Real struct{}

func (Real) Now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

// Fake 測試用時鐘，可手動前進。
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

func NewFake(t time.Time) *Fake { return &Fake{now: t.UTC().Truncate(time.Millisecond)} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
