package points_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"saga-points-demo/internal/clock"
	"saga-points-demo/internal/points"
	"saga-points-demo/internal/testdb"
)

func setup(t *testing.T) (*points.Service, context.Context) {
	svc, _, ctx := setupWithDB(t)
	return svc, ctx
}

func setupWithDB(t *testing.T) (*points.Service, *sql.DB, context.Context) {
	conn := testdb.Open(t, "points")
	return points.NewService(conn, clock.Real{}), conn, context.Background()
}

func topUp(t *testing.T, svc *points.Service, userID, amount int64) {
	t.Helper()
	_, err := svc.ApplyPointsChange(context.Background(), points.Change{
		RequestID: fmt.Sprintf("topup:%d:%d", userID, time.Now().UnixNano()),
		UserID:    userID, Delta: amount, BizType: points.BizAdminAdjust,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertReconciled(t *testing.T, svc *points.Service, userID int64) {
	t.Helper()
	bal, sum, err := svc.Reconcile(context.Background(), userID)
	if err != nil {
		t.Fatal(err)
	}
	if bal != sum {
		t.Fatalf("對帳失敗：餘額 %d ≠ 流水加總 %d", bal, sum)
	}
}

// 情境：同一個 request_id 被併發送了 20 次（使用者狂點 + 網路重送）。
// 餘額 100、每次扣 80 —— 刻意設計成「餘額只夠扣一次」。
// 期望：只扣一次，而且 20 個請求「全部」回成功（其中 19 個是重放），
// 不能有任何一個因為看到被扣過的餘額而回「點數不足」。
func TestConcurrentDuplicateRequest_DeductsOnce(t *testing.T) {
	svc, ctx := setup(t)
	topUp(t, svc, 1, 100)

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	var replayed, fresh int
	var errs []error

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := svc.ApplyPointsChange(ctx, points.Change{
				RequestID: "dup-req-1", UserID: 1, Delta: -80, BizType: points.BizRedeemDeduct,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			if e.Replayed {
				replayed++
			} else {
				fresh++
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("重複請求不該失敗，卻有 %d 個錯誤，例如：%v", len(errs), errs[0])
	}
	if fresh != 1 || replayed != n-1 {
		t.Fatalf("期望 1 次真正執行 + %d 次重放，實際 %d + %d", n-1, fresh, replayed)
	}
	if bal, _ := svc.Balance(ctx, 1); bal != 20 {
		t.Fatalf("餘額應為 20，實際 %d", bal)
	}
	assertReconciled(t, svc, 1)
}

// 情境：20 個「不同」的扣點請求同時打進來，每個扣 10，餘額只有 100。
// 期望：剛好 10 個成功、10 個點數不足，餘額歸零，不會扣成負數（悲觀鎖的作用）。
func TestConcurrentDifferentRequests_NoOverspend(t *testing.T) {
	svc, ctx := setup(t)
	topUp(t, svc, 2, 100)

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, insufficient := 0, 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.ApplyPointsChange(ctx, points.Change{
				RequestID: fmt.Sprintf("spend-%d", i), UserID: 2, Delta: -10, BizType: points.BizRedeemDeduct,
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, points.ErrInsufficient):
				insufficient++
			default:
				t.Errorf("非預期錯誤: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if ok != 10 || insufficient != 10 {
		t.Fatalf("期望 10 成功 / 10 點數不足，實際 %d / %d", ok, insufficient)
	}
	if bal, _ := svc.Balance(ctx, 2); bal != 0 {
		t.Fatalf("餘額應為 0，實際 %d", bal)
	}
	assertReconciled(t, svc, 2)
}

// 情境：全新使用者（user_points 還沒有資料列），20 個請求同時對他加點。
// FOR UPDATE 鎖不到不存在的列 → 大家同時決定 INSERT → 靠唯一鍵 + 重新上鎖收斂。
func TestNewUser_ConcurrentFirstOperation(t *testing.T) {
	svc, ctx := setup(t)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := svc.ApplyPointsChange(ctx, points.Change{
				RequestID: fmt.Sprintf("newbie-%d", i), UserID: 3, Delta: 10, BizType: points.BizAdminAdjust,
			}); err != nil {
				t.Errorf("新使用者併發加點失敗: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if bal, _ := svc.Balance(ctx, 3); bal != 200 {
		t.Fatalf("餘額應為 200，實際 %d", bal)
	}
	assertReconciled(t, svc, 3)
}

// 每一筆流水都要能單獨驗算：before + delta = after。
func TestLedger_EveryEntryIsSelfConsistent(t *testing.T) {
	svc, ctx := setup(t)
	topUp(t, svc, 4, 500)
	for i := 0; i < 5; i++ {
		_, _ = svc.ApplyPointsChange(ctx, points.Change{
			RequestID: fmt.Sprintf("l-%d", i), SagaRequestID: "saga-L", UserID: 4, Delta: -30, BizType: points.BizRedeemDeduct,
		})
	}
	entries, err := svc.ListBySaga(ctx, "saga-L")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("應有 5 筆流水，實際 %d", len(entries))
	}
	for _, e := range entries {
		if e.BalanceBefore+e.Delta != e.BalanceAfter {
			t.Fatalf("流水無法驗算: %+v", e)
		}
	}
}

// 情境：補償被重複觸發（背景 worker 不確定上一次有沒有成功），而且是併發觸發。
// 期望：只退一次點。
func TestCompensate_IsIdempotent(t *testing.T) {
	svc, ctx := setup(t)
	topUp(t, svc, 5, 300)
	if _, err := svc.ApplyPointsChange(ctx, points.Change{
		RequestID: "deduct_points:saga-C", SagaRequestID: "saga-C", UserID: 5, Delta: -100, BizType: points.BizRedeemDeduct,
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.Compensate(ctx, "deduct_points:saga-C", "saga-C", 5); err != nil {
				t.Errorf("補償失敗: %v", err)
			}
		}()
	}
	wg.Wait()

	if bal, _ := svc.Balance(ctx, 5); bal != 300 {
		t.Fatalf("應退回一次（餘額 300），實際 %d", bal)
	}
	state, _ := svc.Probe(ctx, "deduct_points:saga-C")
	if state != points.ProbeCompensated {
		t.Fatalf("查證結果應為 compensated，實際 %s", state)
	}
	assertReconciled(t, svc, 5)
}

// 情境（空補償 + 懸掛）：扣點請求卡在網路上，協調器逾時後先發了補償；
// 補償跑完之後，原本那個扣點請求才「晚到」。
// 期望：補償不能憑空退點（空補償），晚到的扣點也不能生效（懸掛）。
func TestCompensateBeforeForward_EmptyCompensationAndHanging(t *testing.T) {
	svc, ctx := setup(t)
	topUp(t, svc, 6, 300)

	// 補償先到：流水裡查不到正向紀錄 → 不退點，只寫作廢標記
	if err := svc.Compensate(ctx, "deduct_points:saga-H", "saga-H", 6); err != nil {
		t.Fatal(err)
	}
	if bal, _ := svc.Balance(ctx, 6); bal != 300 {
		t.Fatalf("空補償不該改變餘額，實際 %d", bal)
	}

	// 正向請求晚到
	_, err := svc.ApplyPointsChange(ctx, points.Change{
		RequestID: "deduct_points:saga-H", SagaRequestID: "saga-H", UserID: 6, Delta: -100, BizType: points.BizRedeemDeduct,
	})
	if !errors.Is(err, points.ErrCancelled) {
		t.Fatalf("晚到的正向請求應被擋下（ErrCancelled），實際 %v", err)
	}
	if bal, _ := svc.Balance(ctx, 6); bal != 300 {
		t.Fatalf("懸掛的扣點不該生效，實際餘額 %d", bal)
	}
	assertReconciled(t, svc, 6)
}
