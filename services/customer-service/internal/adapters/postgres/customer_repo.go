// Package postgres реализует адаптер для работы с PostgreSQL.
// CustomerRepo — репозиторий профилей клиентов. Сам SQL живёт в
// queries/customers.sql и превращён sqlc в типизированные методы пакета db;
// здесь остаётся маппинг строк БД в доменные модели и ошибок — в доменные.
package postgres

import (
        "context"
        "errors"

        "github.com/jackc/pgx/v5"
        "github.com/jackc/pgx/v5/pgxpool"

        "mini-core/services/customer-service/internal/adapters/postgres/db"
        "mini-core/services/customer-service/internal/domain"
)

type CustomerRepo struct {
        q *db.Queries // сгенерированные sqlc-методы вместо «сырых» строк SQL
}

func NewCustomerRepo(pool *pgxpool.Pool) *CustomerRepo {
        return &CustomerRepo{q: db.New(pool)} // пул подходит: умеет Exec/Query/QueryRow
}

// toCustomer — маппинг плоской строки из БД в доменную модель.
// Все три Row-структуры sqlc (Get/Update/List) содержат одинаковые
// поля, поэтому одна функция-помощник подходит всем методам.
// Статус хранится строкой, здесь возвращаем ему доменный тип.
func toCustomer(id, userID, firstName, lastName, phone, dob, address, citizenship, status string) *domain.Customer {
        return &domain.Customer{
                ID:          id,
                UserID:      userID,
                FirstName:   firstName,
                LastName:    lastName,
                Phone:       phone,
                DateOfBirth: dob, // уже строка YYYY-MM-DD: to_char в SQL
                Address:     address,
                Citizenship: citizenship,
                Status:      domain.CustomerStatus(status),
        }
}

// Create вставляет пустой профиль (только id, user_id, статус NEW).
// Идемпотентность обеспечивает сам SQL: ON CONFLICT (user_id) DO NOTHING,
// поэтому повторное событие из Kafka не будет ошибкой.
func (r *CustomerRepo) Create(ctx context.Context, c *domain.Customer) error {
        return r.q.CreateCustomer(ctx, db.CreateCustomerParams{
                ID:     c.ID,
                UserID: c.UserID,
                Status: string(c.Status),
        })
}

func (r *CustomerRepo) FindByUserID(ctx context.Context, userID string) (*domain.Customer, error) {
        row, err := r.q.GetCustomerByUserID(ctx, userID)
        if err != nil {
                return nil, mapCustomerErr(err)
        }
        return toCustomer(row.ID, row.UserID, row.FirstName, row.LastName, row.Phone,
                row.DateOfBirth, row.Address, row.Citizenship, row.Status), nil
}

// Update перезаписывает анкету. Пустая дата рождения — не проблема:
// в SQL nullif превратит её в NULL ещё до to_date.
func (r *CustomerRepo) Update(ctx context.Context, c *domain.Customer) error {
        return r.q.UpdateCustomer(ctx, db.UpdateCustomerParams{
                UserID:      c.UserID,
                FirstName:   c.FirstName,
                LastName:    c.LastName,
                Phone:       c.Phone,
                DateOfBirth: c.DateOfBirth,
                Address:     c.Address,
                Citizenship: c.Citizenship,
                Status:      string(c.Status),
        })
}

// List возвращает страницу профилей. Если statusFilter пуст — берём
// запрос без фильтра, иначе с фильтром (два метода sqlc вместо одного
// с хитрым условием; см. комментарии в queries/customers.sql).
// Приложение считает limit/offset в int, sqlc сгенерировал int32 —
// конвертация происходит на границе, как и положено адаптеру.
func (r *CustomerRepo) List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Customer, error) {
        lim, off := int32(limit), int32(offset)
        var (
                rows []db.ListCustomersRow
                err  error
        )
        if statusFilter == "" {
                rows, err = r.q.ListCustomers(ctx, db.ListCustomersParams{Limit: lim, Offset: off})
        } else {
                var byStatus []db.ListCustomersByStatusRow
                if byStatus, err = r.q.ListCustomersByStatus(ctx, db.ListCustomersByStatusParams{
                        Status: statusFilter, Limit: lim, Offset: off,
                }); err != nil {
                        return nil, mapCustomerErr(err)
                }
                out := make([]domain.Customer, 0, len(byStatus))
                for _, row := range byStatus {
                        c := toCustomer(row.ID, row.UserID, row.FirstName, row.LastName, row.Phone,
                                row.DateOfBirth, row.Address, row.Citizenship, row.Status)
                        out = append(out, *c)
                }
                return out, nil
        }
        if err != nil {
                return nil, mapCustomerErr(err)
        }
        out := make([]domain.Customer, 0, len(rows))
        for _, row := range rows {
                c := toCustomer(row.ID, row.UserID, row.FirstName, row.LastName, row.Phone,
                        row.DateOfBirth, row.Address, row.Citizenship, row.Status)
                out = append(out, *c)
        }
        return out, nil
}

// mapCustomerErr — перевод ошибок драйвера в ошибки домена.
// pgx.ErrNoRows означает «запрос не нашёл ни одной строки» —
// для домена это просто «клиент не найден».
func mapCustomerErr(err error) error {
        if errors.Is(err, pgx.ErrNoRows) {
                return domain.ErrNotFound
        }
        return err
}
