package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	customerv1 "mini-core/pkg/api/bank/customer/v1"
	"mini-core/services/customer-service/internal/adapters/grpcapi"
	appkafka "mini-core/services/customer-service/internal/adapters/kafka"
	"mini-core/services/customer-service/internal/adapters/postgres"
	"mini-core/services/customer-service/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://dev:dev@localhost:5432/customer_db?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers[0] = "localhost:9092"
	}

	svc := app.NewCustomer(postgres.NewCustomerRepo(pool))

	// Консьюмер — в фоне
	consumer := appkafka.NewConsumer(brokers, svc)
	go consumer.Run(ctx)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	customerv1.RegisterCustomerServiceServer(srv, grpcapi.New(svc))
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		log.Println("graceful shutdown...")
		srv.GracefulStop()
		_ = consumer.Close()
	}()

	log.Println("customer-service: gRPC :50052")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
