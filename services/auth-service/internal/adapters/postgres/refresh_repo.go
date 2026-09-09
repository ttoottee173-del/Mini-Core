package postgres

import (
	"context"
	"errors"
	"mini-core/services/auth-service/internal/domain"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RefreshRepo — репозиторий для работы с refresh-токенами в PostgreSQL.
// Хранит хеши токенов, идентификатор пользователя и время истечения.
type RefreshRepo struct {
	pool *pgxpool.Pool
}

// NewRefreshRepo — конструктор RefreshRepo.
// Принимает пул соединений и возвращает адаптер.
func NewRefreshRepo(pool *pgxpool.Pool) *RefreshRepo {
	return &RefreshRepo{pool: pool}
}

// Create сохраняет хеш refresh-токена в таблицу refresh_tokens.
// Параметры:
//   - tokenHash — SHA-256 хеш токена (в hex)
//   - userID    — идентификатор пользователя
//   - expiresAt — время истечения (UTC)
//
// Возвращает ошибку, если вставка не удалась (например, нарушение внешнего ключа).
func (r *RefreshRepo) Create(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO refresh_tokens (token_hash, user_id, expires_at) VALUES ($1, $2, $3)`,
		tokenHash, userID, expiresAt,
	)
	return err
}

func (r *RefreshRepo) FindByHash(ctx context.Context, hash string) (*domain.RefreshToken, error) {
	var rt domain.RefreshToken
	var revoked *time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT token_hash, user_id, expires_at, revoked_at FROM refresh_tokens WHERE token_hash = $1`, hash,
	).Scan(&rt.Hash, &rt.UserID, &rt.ExpiresAt, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rt.RevokedAt = revoked
	return &rt, nil
}

func (r *RefreshRepo) Revoke(ctx context.Context, hash string, at time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE refresh_tokens SET revoked_at = $1 WHERE token_hash = $2`, at, hash,
	)
	return err
}
