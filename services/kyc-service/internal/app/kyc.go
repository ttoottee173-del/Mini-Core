package app

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"

	"mini-core/services/kyc-service/internal/domain"
)

// Kyc — сценарии верификации (use cases). Знает про домен и порты,
// не знает про SQL, Kafka и gRPC — за это отвечают адаптеры.
type Kyc struct {
	repo      Repo
	customers Customers
	documents Documents
	events    Events
	now       func() time.Time // время для авто-чеков (в тестах подменяем)
}

func New(repo Repo, customers Customers, documents Documents, events Events) *Kyc {
	return &Kyc{
		repo:      repo,
		customers: customers,
		documents: documents,
		events:    events,
		now:       time.Now,
	}
}

// SubmitApplication — клиент подаёт заявку (ТЗ 5.4).
// Порядок проверок: профиль заполнен → документы на месте → нет активной
// заявки → создаём SUBMITTED и публикуем kyc.application_submitted.
// Повторный вызов при DOCS_REQUESTED — это досыл документов: заявка
// возвращается в очередь (переход DOCS_REQUESTED → SUBMITTED).
func (s *Kyc) SubmitApplication(ctx context.Context, userID string) (*domain.Application, error) {
	profile, err := s.customers.GetByUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	// Профиль готов к KYC только в статусе PROFILE_FILLED:
	// NEW — анкета пуста, ACTIVE/REJECTED — верификация уже пройдена,
	// ON_KYC — заявка уже в работе (поймается ограничением ниже).
	if profile.Status != "PROFILE_FILLED" {
		return nil, domain.ErrProfileNotFilled
	}
	if err := s.documents.HasRequired(ctx, profile.CustomerID); err != nil {
		return nil, err
	}

	existing, err := s.repo.FindActiveByCustomer(ctx, profile.CustomerID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Status != domain.StatusDocsRequested {
			return nil, domain.ErrAlreadyExists
		}
		// Досыл документов: обратно в очередь, офицер сбрасывается.
		if err := s.repo.GuardedTransition(ctx, existing, domain.StatusSubmitted, userID, ""); err != nil {
			return nil, err
		}
		s.events.Publish(ctx, "application_submitted", existing.ID, existing.CustomerID, "")
		return s.repo.FindByID(ctx, existing.ID)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	a := &domain.Application{
		ID:          id.String(),
		CustomerID:  profile.CustomerID,
		FullName:    profile.FullName,
		DateOfBirth: profile.DateOfBirth,
		Status:      domain.StatusSubmitted,
	}
	if err := s.repo.Create(ctx, a); err != nil {
		return nil, err
	}
	s.events.Publish(ctx, "application_submitted", a.ID, a.CustomerID, "")
	// Перечитываем, чтобы вернуть клиенту то, что реально в БД
	// (created_at/updated_at проставил Postgres).
	return s.repo.FindByID(ctx, a.ID)
}

func (s *Kyc) Get(ctx context.Context, applicationID string) (*domain.Application, error) {
	return s.repo.FindByID(ctx, applicationID)
}

// List — очередь заявок для офицера. Пагинация offset-токеном,
// как в customer-service (для учёбы достаточно).
func (s *Kyc) List(ctx context.Context, pageSize int, pageToken, statusFilter string) ([]domain.Application, int, error) {
	offset := 0
	if pageToken != "" {
		n, err := strconv.Atoi(pageToken)
		if err != nil || n < 0 {
			return nil, 0, domain.ErrNotFound
		}
		offset = n
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	items, err := s.repo.List(ctx, pageSize, offset, statusFilter)
	return items, offset + len(items), err
}

// Claim — офицер берёт заявку в работу. Гонку двух офицеров гасит
// оптимистичная блокировка в SQL: проигравший получит ErrAlreadyClaimed.
func (s *Kyc) Claim(ctx context.Context, applicationID, officerID string) (*domain.Application, error) {
	a, err := s.repo.FindByID(ctx, applicationID)
	if err != nil {
		return nil, err
	}
	// Сразу честно объясним «не тот статус», не доводя до гонки.
	if a.Status != domain.StatusInReview {
		return nil, domain.ErrInvalidTransition
	}
	if err := s.repo.Claim(ctx, applicationID, officerID); err != nil {
		return nil, err
	}
	return s.repo.FindByID(ctx, applicationID)
}

// Approve — офицер одобряет заявку. Вердикт выносит только тот,
// кто забрал заявку (a.ReviewerID == officerID).
func (s *Kyc) Approve(ctx context.Context, applicationID, officerID, comment string) (*domain.Application, error) {
	a, err := s.repo.FindByID(ctx, applicationID)
	if err != nil {
		return nil, err
	}
	if a.ReviewerID == "" || a.ReviewerID != officerID {
		return nil, domain.ErrNotReviewer
	}
	if err := s.repo.GuardedTransition(ctx, a, domain.StatusApproved, officerID, comment); err != nil {
		return nil, err
	}
	s.events.Publish(ctx, "application_approved", a.ID, a.CustomerID, "")
	return s.repo.FindByID(ctx, applicationID)
}

// Reject — офицер отклоняет заявку с обязательной причиной.
func (s *Kyc) Reject(ctx context.Context, applicationID, officerID, reason string) (*domain.Application, error) {
	if reason == "" {
		return nil, domain.ErrRejectReasonRequired
	}
	a, err := s.repo.FindByID(ctx, applicationID)
	if err != nil {
		return nil, err
	}
	if a.ReviewerID == "" || a.ReviewerID != officerID {
		return nil, domain.ErrNotReviewer
	}
	a.RejectReason = reason // репозиторий сохранит её вместе со статусом
	if err := s.repo.GuardedTransition(ctx, a, domain.StatusRejected, officerID, reason); err != nil {
		return nil, err
	}
	s.events.Publish(ctx, "application_rejected", a.ID, a.CustomerID, reason)
	return s.repo.FindByID(ctx, applicationID)
}

// RequestMoreDocuments — офицеру не хватило документов: заявка уходит
// в DOCS_REQUESTED, клиент досылает и подаёт заявку заново.
func (s *Kyc) RequestMoreDocuments(ctx context.Context, applicationID, officerID, comment string) (*domain.Application, error) {
	a, err := s.repo.FindByID(ctx, applicationID)
	if err != nil {
		return nil, err
	}
	if a.ReviewerID == "" || a.ReviewerID != officerID {
		return nil, domain.ErrNotReviewer
	}
	if err := s.repo.GuardedTransition(ctx, a, domain.StatusDocsRequested, officerID, comment); err != nil {
		return nil, err
	}
	s.events.Publish(ctx, "docs_requested", a.ID, a.CustomerID, comment)
	return s.repo.FindByID(ctx, applicationID)
}
