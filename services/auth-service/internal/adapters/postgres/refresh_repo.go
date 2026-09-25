package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mini-core/services/auth-service/internal/adapters/postgres/db"
	"mini-core/services/auth-service/internal/domain"
)

// RefreshRepo — репозиторий для работы с refresh-токенами в PostgreSQL.
// Хранит хеши токенов, идентификатор пользователя и время истечения.
// Как и UserRepo, это тонкая обёртка над sqlc-пакетом db.
type RefreshRepo struct {
	q *db.Queries
}

// NewRefreshRepo — конструктор RefreshRepo.
func NewRefreshRepo(pool *pgxpool.Pool) *RefreshRepo {
	return &RefreshRepo{q: db.New(pool)}
}

// Create сохраняет хеш refresh-токена в таблицу refresh_tokens.
// Параметры:
//   - tokenHash — SHA-256 хеш токена (в hex)
//   - userID    — идентификатор пользователя
//   - expiresAt — время истечения (UTC)
//
// Возвращает ошибку, если вставка не удалась (например, нарушение внешнего ключа).
func (r *RefreshRepo) Create(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error {
	return r.q.CreateRefreshToken(ctx, db.CreateRefreshTokenParams{
		TokenHash: tokenHash,
		UserID:    userID,
		ExpiresAt: expiresAt, // override в sqlc.yaml: timestamptz → time.Time
	})
}

// FindByHash возвращает токен по хешу. Обратите внимание: RevokedAt
// в БД допускает NULL, поэтому в сгенерированной структуре это *time.Time —
// ровно тот же тип, что и в доменной модели, маппинг получается тривиальным.
func (r *RefreshRepo) FindByHash(ctx context.Context, hash string) (*domain.RefreshToken, error) {
	row, err := r.q.GetRefreshToken(ctx, hash)
	if err != nil {
		return nil, mapUserErr(err) // тот же перевод ErrNoRows → ErrNotFound
	}
	return &domain.RefreshToken{
		Hash:      row.TokenHash,
		UserID:    row.UserID,
		ExpiresAt: row.ExpiresAt,
		RevokedAt: row.RevokedAt,
	}, nil
}

// Revoke помечает токен отозванным (записывает время отзыва).
func (r *RefreshRepo) Revoke(ctx context.Context, hash string, at time.Time) error {
	return r.q.RevokeRefreshToken(ctx, db.RevokeRefreshTokenParams{
		RevokedAt: &at, // nullable-колонка → *time.Time
		TokenHash: hash,
	})
}
