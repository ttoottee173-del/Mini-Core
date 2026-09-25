// Package grpcapi реализует gRPC-сервер для банковского auth-сервиса.
// Это адаптер, который:
// - принимает gRPC-запросы,
// - преобразует их в вызовы сценариев (use cases) из пакета app,
// - маппит доменные ошибки в gRPC статусы (codes).
// Бизнес-логики здесь нет — только преобразование данных и обработка ошибок.
package grpcapi

import (
        "context"
        "errors"

        "google.golang.org/grpc/codes"
        "google.golang.org/grpc/status"

        authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
        "mini-core/services/auth-service/internal/app"
        "mini-core/services/auth-service/internal/domain"
)

// Server реализует gRPC-контракт bank.auth.v1.AuthService.
// Роль — переводчик: proto-запрос → вызов use case (пакет app),
// доменная ошибка → код gRPC (AlreadyExists, InvalidArgument, ...).
// Бизнес-логики здесь нет и быть не должно.
type Server struct {
        authv1.UnimplementedAuthServiceServer
        reg     *app.RegisterService // зависимость приходит снаружи, не создаётся внутри
        login   *app.LoginService
        refresh *app.RefreshService
}

// New — конструктор Server.
// Принимает сценарий регистрации и возвращает gRPC-обработчик.
func New(reg *app.RegisterService, login *app.LoginService, refresh *app.RefreshService) *Server {
        return &Server{reg: reg, login: login, refresh: refresh}
}

// Register — реализация RPC-метода из proto-контракта.
// 1. Извлекает email и password из запроса.
// 2. Вызывает сценарий RegisterService.Register.
// 3. Преобразует доменные ошибки в соответствующие gRPC-статусы:
//   - domain.ErrEmailTaken → codes.AlreadyExists
//   - domain.ErrInvalidInput → codes.InvalidArgument
//   - другие ошибки → codes.Internal (с общим сообщением, чтобы не раскрывать детали).
//
// 4. В случае успеха формирует gRPC-ответ с данными пользователя.
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

// Login — реализация RPC-метода из proto-контракта.
// 1. Извлекает email и password из запроса.
// 2. Вызывает сценарий LoginService.Login.
// 3. Преобразует доменные ошибки в gRPC-статусы:
//   - domain.ErrInvalidCredentials → codes.Unauthenticated
//   - любые другие ошибки → codes.Internal (с общим сообщением).
//
// 4. В случае успеха возвращает access и refresh токены.
func (s *Server) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
        access, refresh, err := s.login.Login(ctx, req.GetEmail(), req.GetPassword())
        switch {
        case errors.Is(err, domain.ErrInvalidCredentials):
                return nil, status.Error(codes.Unauthenticated, "неверный email или пароль")
        case err != nil:
                return nil, status.Error(codes.Internal, "internal error")
        }
        return &authv1.LoginResponse{AccessToken: access, RefreshToken: refresh}, nil
}

func (s *Server) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.LoginResponse, error) {
        access, refresh, err := s.refresh.Refresh(ctx, req.GetRefreshToken())
        if err != nil {
                return nil, mapAuthErr(err)
        }
        return &authv1.LoginResponse{AccessToken: access, RefreshToken: refresh}, nil
}

// mapAuthErr — единый перевод доменных ошибок в gRPC-коды.
func mapAuthErr(err error) error {
        switch {
        case errors.Is(err, domain.ErrInvalidCredentials):
                return status.Error(codes.Unauthenticated, "неверные учётные данные")
        case err != nil:
                return status.Error(codes.Internal, "internal error")
        }
        return nil
}
