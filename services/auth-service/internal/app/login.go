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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/alexedwards/argon2id"

	"mini-core/services/auth-service/internal/domain"
)

const (
	accessTTL  = 15 * time.Minute
	refreshTTL = 30 * 24 * time.Hour
)

// Три маленьких порта — по одному на каждую потребность.
// Интерфейс в том, кто ИСПОЛЬЗУЕТ, а не кто реализует (сейчас поймёшь зачем)
type UserReader interface {
	FindByEmail(ctx context.Context, email string) (*domain.User, error)
}

type TokenIssuer interface {
	IssueAccess(userID, role string, ttl time.Duration) (string, error)
}

type RefreshWriter interface {
	Create(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error
}

// LoginService — сценарий аутентификации пользователя.
// Содержит три зависимости: чтение пользователя, выпуск access-токена,
// запись refresh-токена. Не зависит от конкретных реализаций (Postgres, JWT-библиотека).
type LoginService struct {
	users   UserReader
	tokens  TokenIssuer
	refresh RefreshWriter
}

// NewLogin — конструктор LoginService.
// Принимает реализации портов и возвращает готовый сервис.
func NewLogin(users UserReader, tokens TokenIssuer, refresh RefreshWriter) *LoginService {
	return &LoginService{users: users, tokens: tokens, refresh: refresh}
}

// Login выполняет вход пользователя:
//  1. Нормализует email и ищет пользователя в БД.
//     Если пользователь не найден, возвращает ErrInvalidCredentials (без разглашения).
//  2. Сравнивает пароль с хешем через argon2id.
//  3. Выпускает access-токен через TokenIssuer (JWT с RS256).
//  4. Генерирует случайный refresh-токен (32 байта в base64url).
//  5. Сохраняет его SHA-256 хеш в хранилище через RefreshWriter.
//
// Возвращает:
//   - access — JWT-токен для доступа к API.
//   - refresh — случайный токен для обновления сессии.
//   - err    — ошибка (domain.ErrInvalidCredentials, domain.ErrNotFound и др.).
func (s *LoginService) Login(ctx context.Context, email, password string) (access, refresh string, err error) {
	u, err := s.users.FindByEmail(ctx, domain.NormalizeEmail(email))
	if errors.Is(err, domain.ErrNotFound) {
		return "", "", domain.ErrInvalidCredentials // ← ВНИМАНИЕ, вернёмся к этому в вопросах
	}
	if err != nil {
		return "", "", err
	}

	ok, err := argon2id.ComparePasswordAndHash(password, u.PasswordHash)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return "", "", domain.ErrInvalidCredentials
	}

	access, err = s.tokens.IssueAccess(u.ID, string(u.Role), accessTTL)
	if err != nil {
		return "", "", err
	}

	refresh, err = newRefreshToken()
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256([]byte(refresh))
	if err := s.refresh.Create(ctx, hex.EncodeToString(sum[:]), u.ID, time.Now().UTC().Add(refreshTTL)); err != nil {
		return "", "", err
	}
	return access, refresh, nil
}

// newRefreshToken генерирует криптостойкий случайный токен (32 байта)
// и кодирует его в base64url без дополнения (=).
// Используется внутри Login для создания refresh-токена.
func newRefreshToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
