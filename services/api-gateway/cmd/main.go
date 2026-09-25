// api-gateway — единственная «дверь» снаружи: REST (JSON) → gRPC внутрь.
// main здесь только собирает зависимости по конфигу и запускает сервер:
// конфиг → verifier → limiter → gRPC-клиенты → HTTP-сервер → graceful shutdown.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
	customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
	"mini-core/services/api-gateway/internal/config"
	"mini-core/services/api-gateway/internal/httpapi"
	"mini-core/services/api-gateway/internal/ratelimit"
	"mini-core/services/api-gateway/internal/verifier"
)

// fatal — логируем и выходим явно (os.Exit не выполняет defer'ы!).
func fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}

func main() {
	// Контекст, который отменяется по Ctrl+C / SIGTERM — через него
	// мы узнаём, что пора останавливаться.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config.Load() // ← все настройки в одной структуре

	// Публичный ключ auth-service: им gateway ПРОВЕРЯЕТ подписи токенов локально.
	pubPEM, err := os.ReadFile(cfg.JWTPublicKeyPath)
	if err != nil {
		fatal(log, "public key", err)
	}
	ver, err := verifier.New(pubPEM)
	if err != nil {
		fatal(log, "verifier", err)
	}

	// Rate limit: token bucket в Redis.
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	limiter := ratelimit.New(rdb, cfg.RateLimitCapacity, cfg.RateLimitRefillPerMin/60.0)

	// gRPC-клиенты к внутренним сервисам (insecure — только для локальной разработки).
	authConn, err := grpc.NewClient(cfg.AuthAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(log, "auth conn", err)
	}
	defer authConn.Close()
	custConn, err := grpc.NewClient(cfg.CustomerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(log, "customer conn", err)
	}
	defer custConn.Close()

	h := httpapi.New(log, ver, limiter,
		authv1.NewAuthServiceClient(authConn),
		customerv1.NewCustomerServiceClient(custConn),
	)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown: по сигналу перестаём принимать новые запросы
	// и даём текущим 10 секунд на завершение.
	go func() {
		<-ctx.Done()
		log.Info("graceful shutdown...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	log.Info("api-gateway: http " + cfg.HTTPAddr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal(log, "http", err)
	}
	log.Info("остановлен аккуратно")
}
