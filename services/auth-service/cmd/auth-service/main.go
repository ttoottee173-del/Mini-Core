package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	authv1 "mini-core/pkg/api/bank/auth/v1"
	"mini-core/services/auth-service/internal/adapters/grpcapi"
	"mini-core/services/auth-service/internal/adapters/jwtauth"
	kafkaauth "mini-core/services/auth-service/internal/adapters/kafka"
	"mini-core/services/auth-service/internal/adapters/postgres"
	"mini-core/services/auth-service/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- Postgres ---
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

	pub := kafkaauth.NewPublisher("localhost:9092")
	defer pub.Close()

	// --- Ключи ---
	privPath := os.Getenv("JWT_PRIVATE_KEY_PATH")
	if privPath == "" {
		privPath = "keys/private.pem"
	}
	pubPath := os.Getenv("JWT_PUBLIC_KEY_PATH")
	if pubPath == "" {
		pubPath = "keys/public.pem"
	}
	privPEM, err := os.ReadFile(privPath)
	if err != nil {
		log.Fatalf("jwt private key: %v", err)
	}
	pubPEM, err := os.ReadFile(pubPath)
	if err != nil {
		log.Fatalf("jwt public key: %v", err)
	}
	issuer, err := jwtauth.NewIssuer(privPEM)
	if err != nil {
		log.Fatalf("jwt issuer: %v", err)
	}

	// --- JWKS (HTTP :8081) ---
	jwks, err := jwksHandler(pubPEM)
	if err != nil {
		log.Fatalf("jwks: %v", err)
	}
	httpSrv := &http.Server{Addr: ":8081", Handler: http.HandlerFunc(jwks)}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http: %v", err)
		}
	}()

	// --- gRPC (:50051) ---
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
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
		log.Println("graceful shutdown...")
		srv.GracefulStop()
		_ = httpSrv.Shutdown(context.Background())
	}()

	log.Println("auth-service: gRPC :50051, JWKS :8081/jwks")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	log.Println("остановлен аккуратно")
}
