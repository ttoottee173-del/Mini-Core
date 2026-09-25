// customer-service — профили клиентов, статусная машина, консьюмер Kafka.
// main только собирает зависимости по конфигу и запускает серверы:
// конфиг → Postgres → use cases → Kafka-консьюмер (фон) → gRPC → graceful shutdown.
package main

import (
        "context"
        "log/slog"
        "net"
        "os"
        "os/signal"
        "syscall"

        "github.com/jackc/pgx/v5/pgxpool"
        "google.golang.org/grpc"
        "google.golang.org/grpc/reflection"

        customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
        "mini-core/services/customer-service/internal/adapters/grpcapi"
        appkafka "mini-core/services/customer-service/internal/adapters/kafka"
        "mini-core/services/customer-service/internal/adapters/postgres"
        "mini-core/services/customer-service/internal/app"
        "mini-core/services/customer-service/internal/config"
)

func main() {
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

        // --- Сценарии (use cases) поверх репозитория ---
        svc := app.NewCustomer(postgres.NewCustomerRepo(pool))

        // --- Консьюмер Kafka: слушает user_registered и создаёт пустые профили ---
        consumer := appkafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, svc)
        go consumer.Run(ctx)

        // --- gRPC ---
        lis, err := net.Listen("tcp", cfg.GRPCAddr)
        if err != nil {
                fatal("listen", err)
        }
        srv := grpc.NewServer()
        customerv1.RegisterCustomerServiceServer(srv, grpcapi.New(svc))
        reflection.Register(srv)

        go func() {
                <-ctx.Done()
                log.Info("graceful shutdown...")
                srv.GracefulStop()
                _ = consumer.Close()
        }()

        log.Info("customer-service: gRPC " + cfg.GRPCAddr)
        if err := srv.Serve(lis); err != nil {
                fatal("serve", err)
        }
}
