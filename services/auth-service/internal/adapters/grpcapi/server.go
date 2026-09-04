package grpcapi

import (
	"context"

	authv1 "mini-core/pkg/api/bank/auth/v1"
)

// Server реализует контракт AuthServiceServer из proto.
type Server struct {
	authv1.UnimplementedAuthServiceServer // заглушка: нереализованное rpc ответит UNIMPLEMENTED
}

func New() *Server {
	return &Server{}
}

func (s *Server) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	// ВРЕМЕННО: заглушка без БД. Следующий шаг — Postgres, миграции, argon2id.
	return &authv1.RegisterResponse{
		UserId: "00000000-0000-0000-0000-000000000000",
		Email:  req.GetEmail(),
		Role:   "CLIENT",
	}, nil
}
