// Package postgres реализует адаптеры для работы с PostgreSQL.
// Содержит репозитории для пользователей (UserRepo) и refresh-токенов (RefreshRepo).
// Преобразует ошибки БД в доменные ошибки (domain.ErrNotFound, domain.ErrEmailTaken).
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"mini-core/services/auth-service/internal/domain"
)

// UserRepo — структура, реализующая интерфейс app.UserRepository.
// Хранит пул соединений с PostgreSQL.
type UserRepo struct {
	pool *pgxpool.Pool
}

// NewUserRepo — конструктор для UserRepo.
// Принимает пул соединений и возвращает готовый адаптер.
func NewUserRepo(pool *pgxpool.Pool) *UserRepo {
	return &UserRepo{pool: pool}
}

// Create сохраняет пользователя в таблицу users.
// Использует параметризованный запрос INSERT.
// Если возникает ошибка нарушения уникальности (код 23505),
// преобразует её в domain.ErrEmailTaken.
// Все остальные ошибки возвращаются как есть (они будут обработаны выше).
// Важно: мы не отдаём наружу сырую ошибку PostgreSQL, чтобы не раскрывать детали реализации.
func (r *UserRepo) Create(ctx context.Context, u *domain.User) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO users (id, email, password_hash, role) VALUES ($1, $2, $3, $4)`,
		u.ID, u.Email, u.PasswordHash, string(u.Role),
	)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // 23505 = unique violation в Postgres
		return domain.ErrEmailTaken // перевод: «язык БД» → «язык домена»
	}
	return err
}

// FindByEmail ищет пользователя по нормализованному email.
// Возвращает domain.ErrNotFound, если запись отсутствует.
// В остальных случаях возвращает ошибку БД (она будет обёрнута выше).
// Роль преобразуется из строки в domain.Role.
func (r *UserRepo) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
	var role string
	u := &domain.User{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, email, password_hash, role FROM users WHERE email = $1`, email,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound // перевод: «пусто в БД» → язык домена
	}
	if err != nil {
		return nil, err
	}
	u.Role = domain.Role(role)
	return u, nil
}

func (r *UserRepo) FindByID(ctx context.Context, id string) (*domain.User, error) {
	var role string
	u := &domain.User{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, email, password_hash, role FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	u.Role = domain.Role(role)
	return u, nil
}
