package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"mini-core/services/auth-service/internal/domain"
)

// RefreshStore — порт: поиск refresh-токена по хешу и его отзыв.
type RefreshStore interface {
	FindByHash(ctx context.Context, hash string) (*domain.RefreshToken, error)
	Revoke(ctx context.Context, hash string, at time.Time) error
}

// UserByIDReader — порт: найти пользователя по id.
type UserByIDReader interface {
	FindByID(ctx context.Context, id string) (*domain.User, error)
}

// RefreshService — сценарий «обновить пару токенов» с ротацией.
type RefreshService struct {
	store   RefreshStore
	users   UserByIDReader
	tokens  TokenIssuer
	refresh RefreshWriter
}

func NewRefresh(store RefreshStore, users UserByIDReader, tokens TokenIssuer, refresh RefreshWriter) *RefreshService {
	return &RefreshService{store: store, users: users, tokens: tokens, refresh: refresh}
}

// Refresh проверяет refresh-токен, отзывает его и выдаёт новую пару.
// Использование уже отозванного токена — признак кражи: отказ.
func (s *RefreshService) Refresh(ctx context.Context, token string) (access, newRefresh string, err error) {
	sum := sha256.Sum256([]byte(token))
	hash := hex.EncodeToString(sum[:])

	rt, err := s.store.FindByHash(ctx, hash)
	if errors.Is(err, domain.ErrNotFound) {
		return "", "", domain.ErrInvalidCredentials
	}
	if err != nil {
		return "", "", err
	}

	// Порядок важен: сначала проверка отзыва, потом истечение.
	if rt.RevokedAt != nil {
		// TODO(прод): отозвать все refresh этого юзера + тревога
		return "", "", domain.ErrInvalidCredentials
	}
	if !rt.Active(time.Now().UTC()) {
		return "", "", domain.ErrInvalidCredentials
	}

	u, err := s.users.FindByID(ctx, rt.UserID)
	if err != nil {
		return "", "", err
	}

	access, err = s.tokens.IssueAccess(u.ID, string(u.Role), accessTTL)
	if err != nil {
		return "", "", err
	}

	newRefresh, err = newRefreshToken()
	if err != nil {
		return "", "", err
	}
	sum = sha256.Sum256([]byte(newRefresh))
	if err := s.refresh.Create(ctx, hex.EncodeToString(sum[:]), u.ID, time.Now().UTC().Add(refreshTTL)); err != nil {
		return "", "", err
	}

	// Отзыв последним: если Create упал, старый токен ещё жив — клиент не без всего.
	if err := s.store.Revoke(ctx, hash, time.Now().UTC()); err != nil {
		return "", "", err
	}
	return access, newRefresh, nil
}
