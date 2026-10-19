package points_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
)

// naiveDeduct 是「沒有冪等性」的反面教材：收到請求就扣，完全不看 request_id。
// （扣點本身用 balance = balance - ? 是原子的，所以不會有 lost update，
//
//	問題純粹出在「同一個意圖被執行了很多次」。）
func naiveDeduct(ctx context.Context, conn *sql.DB, userID, amount int64) error {
	_, err := conn.ExecContext(ctx,
		`UPDATE user_points SET balance = balance - ? WHERE user_id = ? AND balance >= ?`,
		amount, userID, amount)
	return err
}

// 對照組：同一個扣點請求被重送 10 次，沒有冪等保護 → 被扣了 10 次。
// 跟 TestConcurrentDuplicateRequest_DeductsOnce 對照看。
func TestNaive_WithoutIdempotency_DeductsManyTimes(t *testing.T) {
	svc, conn, ctx := setupWithDB(t)
	topUp(t, svc, 99, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = naiveDeduct(ctx, conn, 99, 50) // 同一個「意圖」，送了 10 次
		}()
	}
	wg.Wait()

	bal, _ := svc.Balance(ctx, 99)
	t.Logf("沒有冪等性：使用者只按了一次兌換（50 點），實際被扣了 %d 點", 1000-bal)
	if bal != 500 {
		t.Fatalf("預期反面教材會扣 10 次（餘額 500），實際 %d", bal)
	}
}
