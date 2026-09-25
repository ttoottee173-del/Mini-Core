// Package postgres реализует адаптеры для работы с PostgreSQL.
// Содержит репозитории для пользователей (UserRepo) и refresh-токенов (RefreshRepo).
// Начиная с рефакторинга на sqlc сами SQL-запросы живут в queries/*.sql,
// а здесь остаётся только «перевод» между миром БД и миром домена:
// строки сгенерированных структур → доменные модели,
// ошибки Postgres → доменные ошибки (domain.ErrNotFound, domain.ErrEmailTaken).
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"mini-core/services/auth-service/internal/adapters/postgres/db"
	"mini-core/services/auth-service/internal/domain"
)

var ErrEmailAlreadyRegistered = "23505"

// UserRepo — структура, реализующая интерфейс app.UserRepository.
// Внутри — не пул, а объект Queries, сгенерированный sqlc:
// на каждый запрос из queries/users.sql у него есть типизированный метод.
type UserRepo struct {
	q *db.Queries
}

// NewUserRepo — конструктор для UserRepo.
// db.New принимает любой объект, умеющий Exec/Query/QueryRow —
// пул соединений pgxpool как раз такой, поэтому подходим мы им.
func NewUserRepo(pool *pgxpool.Pool) *UserRepo {
	return &UserRepo{q: db.New(pool)}
}

// Create сохраняет пользователя в таблицу users.
// Если возникает ошибка нарушения уникальности (код 23505),
// преобразует её в domain.ErrEmailTaken — выше по стеку никто
// не должен знать про коды ошибок Postgres.
func (r *UserRepo) Create(ctx context.Context, u *domain.User) error {
	err := r.q.CreateUser(ctx, db.CreateUserParams{
		ID:           u.ID,
		Email:        u.Email,
		PasswordHash: u.PasswordHash,
		Role:         string(u.Role), // доменный Role → строка для БД
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == ErrEmailAlreadyRegistered { // 23505 = unique violation в Postgres
		return domain.ErrEmailTaken // перевод: «язык БД» → «язык домена»
	}
	return err
}

// FindByEmail ищет пользователя по нормализованному email.
// pgx.ErrNoRows («в БД пусто») превращается в domain.ErrNotFound.
func (r *UserRepo) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
	row, err := r.q.GetUserByEmail(ctx, email)
	if err != nil {
		return nil, mapUserErr(err)
	}
	return rowToUser(row.ID, row.Email, row.PasswordHash, row.Role), nil
}

// FindByID — то же самое, но по первичному ключу.
func (r *UserRepo) FindByID(ctx context.Context, id string) (*domain.User, error) {
	row, err := r.q.GetUserByID(ctx, id)
	if err != nil {
		return nil, mapUserErr(err)
	}
	return rowToUser(row.ID, row.Email, row.PasswordHash, row.Role), nil
}

// rowToUser — маппинг плоской строки из БД в доменную модель.
// Роль хранится строкой, здесь она превращается обратно в domain.Role.
func rowToUser(id, email, passwordHash, role string) *domain.User {
	return &domain.User{
		ID:           id,
		Email:        email,
		PasswordHash: passwordHash,
		Role:         domain.Role(role),
	}
}

// mapUserErr — единый перевод ошибок драйвера в ошибки домена.
func mapUserErr(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound // перевод: «пусто в БД» → язык домена
	}
	return err
}
