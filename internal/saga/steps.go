package saga

import (
	"context"

	"saga-points-demo/internal/inventory"
	"saga-points-demo/internal/items"
	"saga-points-demo/internal/points"
)

// Step 是 Saga 裡的一個步驟：一個正向操作 + 一個補償操作 + 一個查證。
// 正向跟補償都必須具冪等性，因為協調器可能重試任何一個。
type Step interface {
	Name() string
	Execute(ctx context.Context, s *Saga) error
	Compensate(ctx context.Context, s *Saga) error
	// Probe 到下游服務的流水裡查證：這一步「實際上」有沒有生效？
	// 回傳 none / applied / compensated / voided
	Probe(ctx context.Context, s *Saga) (string, error)
}

// ForwardID 每一步的正向 request_id 由 saga 的 request_id 衍生，固定不變。
// 重試時帶同一個 ID，下游服務的冪等機制就能擋掉重複執行；
// 排程查證時也是拿這個 ID 去下游流水裡找。
func ForwardID(step Step, s *Saga) string { return step.Name() + ":" + s.RequestID }

// ── Step 1：扣點數 ────────────────────────────────────────────────────

type DeductPointsStep struct{ Svc *points.Service }

func (DeductPointsStep) Name() string { return "deduct_points" }

func (st DeductPointsStep) Execute(ctx context.Context, s *Saga) error {
	_, err := st.Svc.ApplyPointsChange(ctx, points.Change{
		RequestID:     ForwardID(st, s),
		SagaRequestID: s.RequestID,
		UserID:        s.UserID,
		Delta:         -s.Cost,
		BizType:       points.BizRedeemDeduct,
	})
	return err
}

func (st DeductPointsStep) Compensate(ctx context.Context, s *Saga) error {
	return st.Svc.Compensate(ctx, ForwardID(st, s), s.RequestID, s.UserID)
}

func (st DeductPointsStep) Probe(ctx context.Context, s *Saga) (string, error) {
	return st.Svc.Probe(ctx, ForwardID(st, s))
}

// ── Step 2：扣庫存 ────────────────────────────────────────────────────

type DeductStockStep struct{ Svc *inventory.Service }

func (DeductStockStep) Name() string { return "deduct_stock" }

func (st DeductStockStep) Execute(ctx context.Context, s *Saga) error {
	return st.Svc.Apply(ctx, ForwardID(st, s), s.ItemID, -1)
}

func (st DeductStockStep) Compensate(ctx context.Context, s *Saga) error {
	return st.Svc.Compensate(ctx, ForwardID(st, s), s.ItemID)
}

func (st DeductStockStep) Probe(ctx context.Context, s *Saga) (string, error) {
	return st.Svc.Probe(ctx, ForwardID(st, s))
}

// ── Step 3：發放道具 ──────────────────────────────────────────────────

type GrantItemStep struct{ Svc *items.Service }

func (GrantItemStep) Name() string { return "grant_item" }

func (st GrantItemStep) Execute(ctx context.Context, s *Saga) error {
	return st.Svc.Grant(ctx, ForwardID(st, s), s.UserID, s.ItemID)
}

func (st GrantItemStep) Compensate(ctx context.Context, s *Saga) error {
	return st.Svc.Compensate(ctx, ForwardID(st, s), s.UserID, s.ItemID)
}

func (st GrantItemStep) Probe(ctx context.Context, s *Saga) (string, error) {
	return st.Svc.Probe(ctx, ForwardID(st, s))
}
