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

	authv1 "mini-core/pkg/api/bank/auth/v1"
	"mini-core/services/auth-service/internal/adapters/grpcapi"
	"mini-core/services/auth-service/internal/adapters/postgres"
	"mini-core/services/auth-service/internal/app"
)

func main() {
	// Твоя догадка про «обработку сигнала» — вот она, штатным способом:
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://dev:dev@localhost:5432/auth_db?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	// Сборка конструктора: repo → use case → gRPC-сервер (внедрение зависимостей)
	authv1.RegisterAuthServiceServer(srv, grpcapi.New(app.NewRegister(postgres.NewUserRepo(pool))))
	reflection.Register(srv)

	go func() {
		<-ctx.Done() // поймали Ctrl+C
		log.Println("graceful shutdown...")
		srv.GracefulStop() // не принимаем новые запросы, дожидаемся текущих
	}()

	log.Println("auth-service слушает :50051")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	log.Println("остановлен аккуратно")
}
