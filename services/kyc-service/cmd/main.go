// kyc-service — заявки на верификацию, статусная машина, авто-чеки.
// main только собирает зависимости по конфигу и запускает всё вместе:
// конфиг → Postgres → gRPC-клиент customer-service → use cases →
// → Kafka-паблишер → воркер авто-чеков (фон) → gRPC → graceful shutdown.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	kycv1 "mini-core/services/kyc-service/gen/bank/kyc/v1"
	"mini-core/services/kyc-service/internal/adapters/customers"
	doccheck "mini-core/services/kyc-service/internal/adapters/doccheck"
	"mini-core/services/kyc-service/internal/adapters/grpcapi"
	appkafka "mini-core/services/kyc-service/internal/adapters/kafka"
	"mini-core/services/kyc-service/internal/adapters/postgres"
	"mini-core/services/kyc-service/internal/app"
	"mini-core/services/kyc-service/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config.Load() // ← все настройки в одной структуре

	fatal := func(msg string, err error) {
		log.Error(msg, "err", err)
		os.Exit(1)
	}

	// --- Postgres: пул соединений со СВОЕЙ базой (kyc_db) ---
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fatal("postgres", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fatal("postgres ping", err)
	}

	// --- gRPC-клиент customer-service: проверка профиля перед подачей ---
	customersClient, err := customers.Dial(ctx, cfg.CustomerAddr)
	if err != nil {
		fatal("customer conn", err)
	}

	// --- Kafka: публикация kyc.* событий ---
	publisher := appkafka.NewPublisher(cfg.KafkaBrokers)
	defer publisher.Close()

	// --- Сценарии (use cases) поверх портов ---
	svc := app.New(postgres.NewRepo(pool), customersClient, doccheck.NewStub(), publisher)

	// --- Воркер авто-чеков: мок-санкции + возраст, отдельная горутина ---
	scanEvery, err := time.ParseDuration(cfg.ScanEvery)
	if err != nil {
		fatal("scan every", err)
	}
	go svc.RunAutoChecks(ctx, scanEvery)

	// --- gRPC-сервер ---
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		fatal("listen", err)
	}
	srv := grpc.NewServer()
	kycv1.RegisterKycServiceServer(srv, grpcapi.New(svc))
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		log.Info("graceful shutdown...")
		srv.GracefulStop()
	}()

	log.Info("kyc-service: gRPC " + cfg.GRPCAddr + ", авто-чеки каждые " + cfg.ScanEvery)
	if err := srv.Serve(lis); err != nil {
		fatal("serve", err)
	}
}
