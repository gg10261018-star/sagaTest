// Package db 負責連線、建表，以及 MySQL 錯誤碼判斷。
package db

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

//go:embed schema.sql
var schemaSQL string

// MySQL 錯誤碼
const (
	ErrCodeDuplicateEntry = 1062 // 違反唯一鍵
	ErrCodeDeadlock       = 1213 // InnoDB 偵測到死鎖，已把本交易回滾
)

// Open 建立連線池。強制開啟幾個本專案依賴的參數：
//   - parseTime / loc=UTC：時間一律以 UTC 存取
//   - clientFoundRows：UPDATE 回傳「符合條件的列數」而不是「實際有變動的列數」，
//     租約判斷（WHERE lease_owner = ?）靠這個數字確認自己還持有租約
func Open(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析 DSN 失敗: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.ClientFoundRows = true

	conn, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(5 * time.Minute)
	return conn, nil
}

// Migrate 依序執行 schema.sql 裡的每一條 CREATE TABLE IF NOT EXISTS。
func Migrate(ctx context.Context, conn *sql.DB) error {
	for _, stmt := range splitStatements(schemaSQL) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("建表失敗: %w\nSQL: %s", err, stmt)
		}
	}
	return nil
}

// SeedDemo 放一些示範資料（已存在就略過）。
func SeedDemo(ctx context.Context, conn *sql.DB, now time.Time) error {
	stmts := []string{
		`INSERT IGNORE INTO user_points (user_id, balance, updated_at) VALUES (1001, 1000, ?)`,
		`INSERT IGNORE INTO inventory (item_id, stock, updated_at) VALUES (1, 5, ?)`,
	}
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s, now); err != nil {
			return err
		}
	}
	return nil
}

func splitStatements(script string) []string {
	var out []string
	var b strings.Builder
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			out = append(out, strings.TrimSuffix(strings.TrimSpace(b.String()), ";"))
			b.Reset()
		}
	}
	return out
}

// IsDuplicateKey 判斷是否為唯一鍵衝突（MySQL 1062）。
func IsDuplicateKey(err error) bool { return hasCode(err, ErrCodeDuplicateEntry) }

// IsDeadlock 判斷是否為死鎖（MySQL 1213）。死鎖不是程式壞掉，而是「請重試」的訊號。
func IsDeadlock(err error) bool { return hasCode(err, ErrCodeDeadlock) }

func hasCode(err error, code uint16) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == code
}

// Querier 讓同一個查詢函式同時能吃 *sql.DB 跟 *sql.Tx。
type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// WithDeadlockRetry 在遇到死鎖時重跑整個交易。
// 對應筆記 1.2：「程式碼必須針對 Deadlock 這個特定錯誤做重試，不能當一般錯誤放棄」。
func WithDeadlockRetry(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		if !IsDeadlock(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 5 * time.Millisecond):
		}
	}
	return err
}
