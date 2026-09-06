package app

import (
	"context"

	"github.com/alexedwards/argon2id" // хеширование паролей
	"github.com/google/uuid"          // UUID v7 — по требованию ТЗ

	"mini-core/services/auth-service/internal/domain"
)

// Порт: что домену нужно от внешнего мира. РЕАЛИЗАЦИЮ даст адаптер.
type UserRepository interface {
	Create(ctx context.Context, u *domain.User) error
}

type RegisterService struct {
	repo UserRepository
}

func NewRegister(repo UserRepository) *RegisterService {
	return &RegisterService{repo: repo}
}

func (s *RegisterService) Register(ctx context.Context, email, password string) (*domain.User, error) {
	if len(password) < 8 {
		return nil, domain.ErrInvalidInput
	}
	hash, err := argon2id.CreateHash(password, argon2id.DefaultParams) // argon2id из ТЗ
	if err != nil {
		return nil, err
	}
	u, err := domain.NewUser(email, hash)
	if err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	u.ID = id.String()
	if err := s.repo.Create(ctx, u); err != nil {
		return nil, err
	}
	return u, nil
}
