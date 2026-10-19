// Package testdb 提供整合測試用的真實 MySQL 資料庫。
//
// 每個測試套件使用自己的 database（saga_test_<名稱>），
// 因為 `go test ./...` 會平行執行不同套件，共用同一個 database 會互相干擾。
package testdb

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"

	"saga-points-demo/internal/db"
)

// Open 建立（或重用）測試資料庫並清空所有表。
// 環境變數 TEST_MYSQL_DSN 可覆寫連線字串（需要 CREATE DATABASE 權限）；
// 連不上時整個測試會 Skip，而不是失敗。
func Open(t *testing.T, name string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "saga:saga@tcp(127.0.0.1:3306)/"
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("TEST_MYSQL_DSN 格式錯誤: %v", err)
	}

	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.Ping(); err != nil {
		t.Skipf("沒有可用的 MySQL（%v），略過整合測試。可用 docker compose up -d mysql 啟動", err)
	}

	dbName := "saga_test_" + name
	if _, err := admin.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
		t.Fatal(err)
	}

	cfg.DBName = dbName
	conn, err := db.Open(cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	ctx := context.Background()
	if err := db.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"user_points", "points_ledger", "inventory",
		"inventory_transactions", "user_items", "sagas"} {
		if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
			t.Fatal(err)
		}
	}
	return conn
}
