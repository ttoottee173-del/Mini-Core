package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	authv1 "mini-core/pkg/api/bank/auth/v1"
	"mini-core/services/auth-service/internal/adapters/grpcapi"
)

func main() {
	// 1. Занимаем «квартиру» — порт 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("не могу занять порт 50051: %v", err)
	}

	// 2. Создаём gRPC-сервер (интерцепторы добавим на следующем шаге)
	srv := grpc.NewServer()

	// 3. Подключаем нашу реализацию контракта
	authv1.RegisterAuthServiceServer(srv, grpcapi.New())

	// 4. Reflection: сервер умеет рассказывать, какие у него методы (нужно для grpcurl)
	reflection.Register(srv)

	log.Println("auth-service слушает :50051")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
