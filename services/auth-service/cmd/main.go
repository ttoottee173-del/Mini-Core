// auth-service — учётные записи, JWT, события в Kafka.
// main только собирает зависимости по конфигу и запускает серверы:
// конфиг → Postgres → Kafka → ключи → JWKS (gin) → gRPC → graceful shutdown.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
	"mini-core/services/auth-service/internal/adapters/grpcapi"
	"mini-core/services/auth-service/internal/adapters/jwks"
	"mini-core/services/auth-service/internal/adapters/jwtauth"
	kafkaauth "mini-core/services/auth-service/internal/adapters/kafka"
	"mini-core/services/auth-service/internal/adapters/postgres"
	"mini-core/services/auth-service/internal/app"
	"mini-core/services/auth-service/internal/config"
)

func main() {
	// Контекст отменяется по Ctrl+C / SIGTERM — сигнал остановки для всего.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config.Load() // ← все настройки в одной структуре

	// fatal — логирует ошибку и завершает процесс с кодом 1.
	// (В slog нет Fatalf, как в старом log, — оборачиваем сами.)
	fatal := func(msg string, err error) {
		log.Error(msg, "err", err)
		os.Exit(1)
	}

	// --- Postgres: пул соединений со СВОЕЙ базой ---
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fatal("postgres", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fatal("postgres ping", err)
	}

	// --- Kafka: публикуем факт user_registered ---
	pub := kafkaauth.NewPublisher(cfg.KafkaBrokers)
	defer pub.Close()

	// --- Ключи: приватным подписываем токены, публичный отдаём через JWKS ---
	privPEM, err := os.ReadFile(cfg.PrivateKeyPath)
	if err != nil {
		fatal("jwt private key", err)
	}
	pubPEM, err := os.ReadFile(cfg.PublicKeyPath)
	if err != nil {
		fatal("jwt public key", err)
	}
	issuer, err := jwtauth.NewIssuer(privPEM)
	if err != nil {
		fatal("jwt issuer", err)
	}

	// --- JWKS: gin-роутер с одним эндпоинтом, отдающим публичный ключ ---
	// Тот же приём, что и в gateway: gin становится Handler'ом http.Server —
	// поэтому graceful shutdown ниже продолжает работать.
	jwksH, err := jwks.Handler(pubPEM)
	if err != nil {
		fatal("jwks", err)
	}
	gin.SetMode(gin.ReleaseMode) // тише логи gin: у эндпоинта один гость
	jwksAPI := gin.New()         // без middleware: логировать и recover тут нечего
	jwksAPI.GET("/jwks", jwksH)  // точный путь: всё остальное — 404
	httpSrv := &http.Server{Addr: cfg.JWKSAddr, Handler: jwksAPI}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("jwks http", "err", err)
		}
	}()

	// --- gRPC: сборка всех слоёв (adapters ← app ← main внедряет зависимости) ---
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		fatal("listen", err)
	}
	srv := grpc.NewServer()

	userRepo := postgres.NewUserRepo(pool)
	refreshRepo := postgres.NewRefreshRepo(pool)
	authv1.RegisterAuthServiceServer(srv, grpcapi.New(
		app.NewRegister(userRepo, pub),
		app.NewLogin(userRepo, issuer, refreshRepo),
		app.NewRefresh(refreshRepo, userRepo, issuer, refreshRepo), // refreshRepo в двух ролях
	))
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		log.Info("graceful shutdown...")
		srv.GracefulStop()
		_ = httpSrv.Shutdown(context.Background())
	}()

	log.Info("auth-service: gRPC " + cfg.GRPCAddr + ", JWKS " + cfg.JWKSAddr + "/jwks")
	if err := srv.Serve(lis); err != nil {
		fatal("serve", err)
	}
	log.Info("остановлен аккуратно")
}
