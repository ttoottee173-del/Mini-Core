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

	authv1 "mini-core/pkg/api/bank/auth/v1"
	customerv1 "mini-core/pkg/api/bank/customer/v1"
	"mini-core/services/api-gateway/internal/httpapi"
	"mini-core/services/api-gateway/internal/ratelimit"
	"mini-core/services/api-gateway/internal/verifier"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// fatal — логируем и выходим явно (os.Exit не выполняет defer'ы!).
func fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	pubPEM, err := os.ReadFile(envOr("JWT_PUBLIC_KEY_PATH", "../auth-service/keys/public.pem"))
	if err != nil {
		fatal(log, "public key", err)
	}
	ver, err := verifier.New(pubPEM)
	if err != nil {
		fatal(log, "verifier", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	limiter := ratelimit.New(rdb, 10, 5.0/60.0)

	authConn, err := grpc.NewClient(envOr("AUTH_ADDR", "localhost:50051"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(log, "auth conn", err)
	}
	defer authConn.Close()
	custConn, err := grpc.NewClient(envOr("CUSTOMER_ADDR", "localhost:50052"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(log, "customer conn", err)
	}
	defer custConn.Close()

	h := httpapi.New(log, ver, limiter,
		authv1.NewAuthServiceClient(authConn),
		customerv1.NewCustomerServiceClient(custConn),
	)

	httpSrv := &http.Server{
		Addr:              envOr("GATEWAY_ADDR", ":8080"),
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Info("graceful shutdown...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	log.Info("api-gateway: http :8080")
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal(log, "http", err)
	}
	log.Info("остановлен аккуратно")
}
