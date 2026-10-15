// Package app содержит бизнес-сценарии (use cases) для auth-сервиса.
// Реализованы сценарии:
//   - RegisterService — регистрация нового пользователя с хешированием пароля (argon2id).
//   - LoginService    — аутентификация, выпуск access и refresh токенов (JWT + SHA-256).
//
// Все зависимости (хранилища, генераторы токенов) внедряются через интерфейсы,
// определенные в этом же пакете (UserRepository, UserReader, TokenIssuer, RefreshWriter).
package app

import (
	"context"

	"github.com/alexedwards/argon2id" // хеширование паролей
	"github.com/google/uuid"          // UUID v7 — по требованию ТЗ

	"mini-core/services/auth-service/internal/domain"
)

// UserEvents — порт «рассказать миру о новых юзерах».
type UserEvents interface {
	UserRegistered(ctx context.Context, userID, email string)
}

// UserRepository — порт, который требуется сценарию RegisterService.
// Адаптер (например, postgres.UserRepo) реализует этот интерфейс.
// Определён здесь, чтобы сценарий не зависел от конкретной реализации.
type UserRepository interface {
	Create(ctx context.Context, u *domain.User) error
}

// RegisterService — структура, реализующая сценарий регистрации.
// Содержит зависимость от хранилища через интерфейс UserRepository.
type RegisterService struct {
	repo UserRepository
	pub  UserEvents
}

// NewRegister собирает сценарий регистрации.
// repo — порт хранилища: интерфейс объявлен здесь, реализацию
// (postgres.UserRepo) внедряет main при сборке.
func NewRegister(repo UserRepository, pub UserEvents) *RegisterService {
	return &RegisterService{repo: repo, pub: pub}
}

// Register — основной метод сценария.
// 1. Проверяет длину пароля (минимум 8 символов).
// 2. Хеширует пароль с помощью argon2id (медленный, но безопасный алгоритм).
// 3. Создаёт объект User через фабрику domain.NewUser (валидация email).
// 4. Генерирует UUID v7 для ID пользователя.
// 5. Сохраняет пользователя через репозиторий.
// Возвращает созданного пользователя или ошибку.
// Ошибки: domain.ErrInvalidInput (короткий пароль или невалидный email),
// domain.ErrEmailTaken (email уже существует), а также ошибки хеширования или генерации UUID.
func (s *RegisterService) Register(ctx context.Context, email, password string) (*domain.User, error) {
	if len(password) < 8 {
		return nil, domain.ErrInvalidInput
	}
	hash, err := argon2id.CreateHash(password, argon2id.DefaultParams)
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
	s.pub.UserRegistered(ctx, u.ID, u.Email) // факт после коммита в БД
	return u, nil
}
