package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"saga-points-demo/internal/api"
	"saga-points-demo/internal/clock"
	"saga-points-demo/internal/db"
	"saga-points-demo/internal/inventory"
	"saga-points-demo/internal/items"
	"saga-points-demo/internal/points"
	"saga-points-demo/internal/saga"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 監聽位址")
	workerInterval := flag.Duration("worker-interval", 2*time.Second, "背景補償 worker 掃描間隔")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "saga:saga@tcp(127.0.0.1:3306)/saga_demo"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := db.Open(dsn)
	if err != nil {
		log.Error("連線失敗", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	// 等資料庫就緒（docker compose 啟動時 MySQL 可能還沒準備好）
	for i := 0; i < 30; i++ {
		if err = conn.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Error("資料庫無法連線", "err", err)
		os.Exit(1)
	}

	clk := clock.Real{}
	if err := db.Migrate(ctx, conn); err != nil {
		log.Error("建表失敗", "err", err)
		os.Exit(1)
	}
	if err := db.SeedDemo(ctx, conn, clk.Now()); err != nil {
		log.Error("寫入示範資料失敗", "err", err)
		os.Exit(1)
	}

	pointsSvc := points.NewService(conn, clk)
	invSvc := inventory.NewService(conn, clk)
	itemSvc := items.NewService(conn, clk)

	// 每一步都包一層 ChaosStep，Demo 時可以用 /admin/chaos 現場製造故障
	chaos := map[string]*saga.ChaosStep{}
	var steps []saga.Step
	for _, st := range []saga.Step{
		saga.DeductPointsStep{Svc: pointsSvc},
		saga.DeductStockStep{Svc: invSvc},
		saga.GrantItemStep{Svc: itemSvc},
	} {
		c := saga.NewChaos(st)
		chaos[st.Name()] = c
		steps = append(steps, c)
	}

	cfg := saga.DefaultConfig()
	cfg.CompensationDeadline = 2 * time.Minute // Demo 用：縮短時限方便觀察升級人工
	orch := saga.NewOrchestrator(saga.NewStore(conn), steps, clk, cfg, saga.LogAlerter{Logger: log}, log)

	go orch.RunWorker(ctx, *workerInterval)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           (&api.Server{Orch: orch, Points: pointsSvc, Inventory: invSvc, Chaos: chaos}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("服務啟動", "addr", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("HTTP 服務異常結束", "err", err)
		os.Exit(1)
	}
}
