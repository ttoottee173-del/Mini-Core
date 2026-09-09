package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mini-core/services/customer-service/internal/domain"
)

type CustomerRepo struct {
	pool *pgxpool.Pool
}

func NewCustomerRepo(pool *pgxpool.Pool) *CustomerRepo {
	return &CustomerRepo{pool: pool}
}

func scanCustomer(row pgx.Row) (*domain.Customer, error) {
	var c domain.Customer
	err := row.Scan(&c.ID, &c.UserID, &c.FirstName, &c.LastName, &c.Phone,
		&c.DateOfBirth, &c.Address, &c.Citizenship, &c.Status)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

const cols = `id, user_id, first_name, last_name, phone,
    coalesce(to_char(date_of_birth,'YYYY-MM-DD'),'') as date_of_birth,
    address, citizenship, status`

func (r *CustomerRepo) Create(ctx context.Context, c *domain.Customer) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO customers (id, user_id, status) VALUES ($1, $2, $3)
         ON CONFLICT (user_id) DO NOTHING`, // идемпотентность на уровне БД
		c.ID, c.UserID, string(c.Status),
	)
	return err
}

func (r *CustomerRepo) FindByUserID(ctx context.Context, userID string) (*domain.Customer, error) {
	c, err := scanCustomer(r.pool.QueryRow(ctx,
		`SELECT `+cols+` FROM customers WHERE user_id = $1`, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	return c, err
}

func (r *CustomerRepo) Update(ctx context.Context, c *domain.Customer) error {
	var dob *string
	if c.DateOfBirth != "" {
		dob = &c.DateOfBirth
	}
	_, err := r.pool.Exec(ctx,
		`UPDATE customers SET first_name=$2, last_name=$3, phone=$4,
         date_of_birth=to_date($5,'YYYY-MM-DD'), address=$6, citizenship=$7,
         status=$8, updated_at=now() WHERE user_id=$1`,
		c.UserID, c.FirstName, c.LastName, c.Phone, dob, c.Address, c.Citizenship, string(c.Status),
	)
	return err
}

func (r *CustomerRepo) List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Customer, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+cols+` FROM customers
         WHERE ($1 = '' OR status = $1)
         ORDER BY created_at LIMIT $2 OFFSET $3`,
		statusFilter, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Customer
	for rows.Next() {
		c, err := scanCustomer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}
