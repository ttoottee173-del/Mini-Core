// Package postgres реализует порт app.Repo поверх PostgreSQL.
// Весь SQL живёт в queries/*.sql и превращён sqlc в типизированные
// методы пакета db; здесь — маппинг строк БД в доменные модели,
// ошибок драйвера — в ошибки домена, и транзакция для смены статуса.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"mini-core/services/kyc-service/internal/adapters/postgres/db"
	"mini-core/services/kyc-service/internal/domain"
)

// Repo — реализация app.Repo (см. internal/app/ports.go).
type Repo struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func NewRepo(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool, q: db.New(pool)}
}

// toApplication — маппинг плоской строки БД в доменную модель.
// Даты/время приходят из SQL уже строками (to_char в queries),
// статусу возвращаем доменный тип.
func toApplication(id, customerID, fullName, dob, reviewerID, status, rejectReason, createdAt, updatedAt string) *domain.Application {
	return &domain.Application{
		ID:           id,
		CustomerID:   customerID,
		FullName:     fullName,
		DateOfBirth:  dob,
		ReviewerID:   reviewerID,
		Status:       domain.ApplicationStatus(status),
		RejectReason: rejectReason,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}
}

// dateOrNull — строка YYYY-MM-DD → pgtype.Date (пустая строка → NULL).
// Конвертация живёт здесь, в адаптере: SQL остаётся простым,
// а sqlc не умеет выводить типы кастов внутри VALUES (см. queries).
func dateOrNull(s string) (pgtype.Date, error) {
	if s == "" {
		return pgtype.Date{}, nil // Valid=false → NULL в БД
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return pgtype.Date{}, fmt.Errorf("bad date %q: %w", s, err)
	}
	return pgtype.Date{Time: t, Valid: true}, nil
}

// uuidOrNull — строка UUID → pgtype.UUID для колонки reviewer_id.
// Парсим пакетом google/uuid: у pgtype нет своего ParseUUID.
func uuidOrNull(s string) (pgtype.UUID, error) {
	if s == "" {
		return pgtype.UUID{}, nil
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("bad uuid %q: %w", s, err)
	}
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

// mapRepoErr — перевод ошибок драйвера в ошибки домена.
func mapRepoErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		// unique_violation: сработал частичный индекс
		// «не более одной активной заявки на клиента».
		return domain.ErrAlreadyExists
	}
	return err
}

func (r *Repo) Create(ctx context.Context, a *domain.Application) error {
	dob, err := dateOrNull(a.DateOfBirth)
	if err != nil {
		return err
	}
	return mapRepoErr(r.q.CreateApplication(ctx, db.CreateApplicationParams{
		ID:                a.ID,
		CustomerID:        a.CustomerID,
		ApplicantFullName: a.FullName,
		ApplicantDob:      dob,
		Status:            string(a.Status),
	}))
}

func (r *Repo) FindByID(ctx context.Context, id string) (*domain.Application, error) {
	row, err := r.q.GetApplication(ctx, id)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	return toApplication(row.ID, row.CustomerID, row.ApplicantFullName, row.ApplicantDob,
		row.ReviewerID, row.Status, row.RejectReason, row.CreatedAt, row.UpdatedAt), nil
}

// FindActiveByCustomer — активная заявка или (nil, nil): «нет заявки»
// для подачи — нормальный ответ, а не ошибка.
func (r *Repo) FindActiveByCustomer(ctx context.Context, customerID string) (*domain.Application, error) {
	rows, err := r.q.FindActiveByCustomer(ctx, customerID)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &domain.Application{
		ID:     rows[0].ID,
		Status: domain.ApplicationStatus(rows[0].Status),
	}, nil
}

func (r *Repo) List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Application, error) {
	lim, off := int32(limit), int32(offset)
	var (
		rows []db.ListApplicationsRow
		err  error
	)
	if statusFilter == "" {
		rows, err = r.q.ListApplications(ctx, db.ListApplicationsParams{Limit: lim, Offset: off})
	} else {
		var byStatus []db.ListApplicationsByStatusRow
		if byStatus, err = r.q.ListApplicationsByStatus(ctx, db.ListApplicationsByStatusParams{
			Status: statusFilter, Limit: lim, Offset: off,
		}); err != nil {
			return nil, mapRepoErr(err)
		}
		out := make([]domain.Application, 0, len(byStatus))
		for _, row := range byStatus {
			out = append(out, *toApplication(row.ID, row.CustomerID, row.ApplicantFullName, row.ApplicantDob,
				row.ReviewerID, row.Status, row.RejectReason, row.CreatedAt, row.UpdatedAt))
		}
		return out, nil
	}
	if err != nil {
		return nil, mapRepoErr(err)
	}
	out := make([]domain.Application, 0, len(rows))
	for _, row := range rows {
		out = append(out, *toApplication(row.ID, row.CustomerID, row.ApplicantFullName, row.ApplicantDob,
			row.ReviewerID, row.Status, row.RejectReason, row.CreatedAt, row.UpdatedAt))
	}
	return out, nil
}

// Claim — оптимистичная блокировка из ТЗ: UPDATE ... WHERE reviewer_id IS NULL.
// 0 обновлённых строк = заявку уже забрал другой офицер.
func (r *Repo) Claim(ctx context.Context, id, officerID string) error {
	oid, err := uuidOrNull(officerID)
	if err != nil {
		return err
	}
	n, err := r.q.ClaimApplication(ctx, db.ClaimApplicationParams{ID: id, OfficerID: oid})
	if err != nil {
		return mapRepoErr(err)
	}
	if n == 0 {
		return domain.ErrAlreadyClaimed
	}
	return nil
}

// GuardedTransition — статус меняется и строка аудита пишется В ОДНОЙ
// транзакции: аудит не может «отстать» от статуса (требование ТЗ к полному
// аудиту). WHERE status = from_status в SQL — защита от гонок.
func (r *Repo) GuardedTransition(ctx context.Context, a *domain.Application, to domain.ApplicationStatus, actor, comment string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // после Commit — no-op (см. документацию pgx)

	q := r.q.WithTx(tx) // те же sqlc-методы, но внутри транзакции
	n, err := q.TransitionApplication(ctx, db.TransitionApplicationParams{
		ID:           a.ID,
		ToStatus:     string(to),
		RejectReason: a.RejectReason,
		FromStatus:   string(a.Status),
	})
	if err != nil {
		return mapRepoErr(err)
	}
	if n == 0 {
		// Кто-то обогнал нас: заявка уже не в статусе a.Status.
		return domain.ErrInvalidTransition
	}
	// Повторная подача: заявка возвращается в общую очередь.
	if to == domain.StatusSubmitted {
		if err := q.ResetReviewer(ctx, a.ID); err != nil {
			return mapRepoErr(err)
		}
	}
	eventID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if err := q.AppendEvent(ctx, db.AppendEventParams{
		ID:            eventID.String(),
		ApplicationID: a.ID,
		Actor:         actor,
		FromStatus:    string(a.Status),
		ToStatus:      string(to),
		Comment:       comment,
	}); err != nil {
		return mapRepoErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Держим структуру у вызывающего актуальной.
	a.Status = to
	if to != domain.StatusRejected && to != domain.StatusAutoRejected {
		a.RejectReason = ""
	}
	return nil
}

func (r *Repo) SaveCheck(ctx context.Context, c *domain.Check) error {
	return mapRepoErr(r.q.InsertCheck(ctx, db.InsertCheckParams{
		ID:            c.ID,
		ApplicationID: c.ApplicationID,
		CheckType:     c.Type,
		Passed:        c.Passed,
		Details:       c.Details,
	}))
}

func (r *Repo) SanctionsMatch(ctx context.Context, fullName, dob string) (bool, error) {
	d, err := dateOrNull(dob)
	if err != nil {
		return false, err
	}
	return r.q.SanctionsMatch(ctx, db.SanctionsMatchParams{Lower: fullName, DateOfBirth: d})
}

// NextSubmitted — следующая заявка из очереди авто-чеков:
// сначала свежеподанные (SUBMITTED), потом застрявшие (AUTO_CHECKS).
func (r *Repo) NextSubmitted(ctx context.Context) (*domain.Application, error) {
	rows, err := r.q.FindNextSubmitted(ctx)
	if err != nil {
		return nil, mapRepoErr(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &domain.Application{
		ID:     rows[0].ID,
		Status: domain.ApplicationStatus(rows[0].Status),
	}, nil
}
