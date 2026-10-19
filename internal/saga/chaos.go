package saga

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrInjected 是故障注入產生的「暫時性」錯誤（模擬網路逾時、下游服務暫時掛掉）。
var ErrInjected = errors.New("注入的故障：下游服務暫時無法回應")

// ChaosStep 包住真正的 Step，用來在測試 / Demo 時製造故障。
//
// 兩種故障模式：
//   - FailAfter = false：呼叫「之前」就失敗 → 下游確定沒執行
//   - FailAfter = true ：下游「已經執行成功」，但回應在路上遺失 → 協調器以為失敗
//
// 第二種最能考驗補償設計：協調器不知道這一步到底有沒有成功。
type ChaosStep struct {
	Inner     Step
	execFails atomic.Int64 // 剩餘要失敗的次數；-1 代表永遠失敗
	compFails atomic.Int64
	failAfter atomic.Bool
}

func NewChaos(inner Step) *ChaosStep { return &ChaosStep{Inner: inner} }

func (c *ChaosStep) SetExecFailures(n int64) { c.execFails.Store(n) }
func (c *ChaosStep) SetCompFailures(n int64) { c.compFails.Store(n) }
func (c *ChaosStep) SetFailAfter(b bool)     { c.failAfter.Store(b) }

func (c *ChaosStep) Name() string { return c.Inner.Name() }

func (c *ChaosStep) Execute(ctx context.Context, s *Saga) error {
	if !c.failAfter.Load() && take(&c.execFails) {
		return ErrInjected
	}
	if err := c.Inner.Execute(ctx, s); err != nil {
		return err
	}
	if c.failAfter.Load() && take(&c.execFails) {
		return ErrInjected // 其實成功了，只是回應遺失
	}
	return nil
}

func (c *ChaosStep) Compensate(ctx context.Context, s *Saga) error {
	if take(&c.compFails) {
		return ErrInjected
	}
	return c.Inner.Compensate(ctx, s)
}

func (c *ChaosStep) Probe(ctx context.Context, s *Saga) (string, error) {
	return c.Inner.Probe(ctx, s) // 查證不注入故障：它只讀流水
}

func take(counter *atomic.Int64) bool {
	for {
		n := counter.Load()
		switch {
		case n == 0:
			return false
		case n < 0:
			return true
		case counter.CompareAndSwap(n, n-1):
			return true
		}
	}
}
