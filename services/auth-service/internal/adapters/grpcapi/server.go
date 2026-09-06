package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authv1 "mini-core/pkg/api/bank/auth/v1"
	"mini-core/services/auth-service/internal/app"
	"mini-core/services/auth-service/internal/domain"
)

type Server struct {
	authv1.UnimplementedAuthServiceServer
	reg *app.RegisterService // зависимость приходит снаружи, не создаётся внутри
}

func New(reg *app.RegisterService) *Server {
	return &Server{reg: reg}
}

func (s *Server) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	u, err := s.reg.Register(ctx, req.GetEmail(), req.GetPassword())
	switch {
	case errors.Is(err, domain.ErrEmailTaken):
		return nil, status.Error(codes.AlreadyExists, "email уже зарегистрирован")
	case errors.Is(err, domain.ErrInvalidInput):
		return nil, status.Error(codes.InvalidArgument, "некорректный email или пароль короче 8 символов")
	case err != nil:
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &authv1.RegisterResponse{UserId: u.ID, Email: u.Email, Role: string(u.Role)}, nil
}
