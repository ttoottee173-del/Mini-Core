package app

import (
        "context"
        "strconv"

        "github.com/google/uuid"

        "mini-core/services/customer-service/internal/domain"
)

// CustomerRepo — порт хранилища профилей.
type CustomerRepo interface {
        Create(ctx context.Context, c *domain.Customer) error // идемпотентен: повтор — не ошибка
        FindByUserID(ctx context.Context, userID string) (*domain.Customer, error)
        FindByCustomerID(ctx context.Context, customerID string) (*domain.Customer, error)
        Update(ctx context.Context, c *domain.Customer) error
        List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Customer, error)
        UpdateStatusByID(ctx context.Context, customerID, status string) error
}

// Events — порт публикации событий профиля (internal/adapters/kafka).
type Events interface {
        // ProfileFilled — анкета заполнена, можно подавать KYC-заявку (ТЗ 5.3).
        ProfileFilled(ctx context.Context, customerID, userID string)
}

// CustomerService — сценарии профиля.
type CustomerService struct {
        repo   CustomerRepo
        events Events // может быть nil в тестах — публикация тогда просто пропускается
}

func NewCustomer(repo CustomerRepo, events Events) *CustomerService {
        return &CustomerService{repo: repo, events: events}
}

// EnsureProfile — создаёт пустой профиль по событию user_registered.
// Идемпотентно: дубликат события не ошибка (at-least-once!).
func (s *CustomerService) EnsureProfile(ctx context.Context, userID string) error {
        if _, err := s.repo.FindByUserID(ctx, userID); err == nil {
                return nil // уже есть — нормально
        }
        id, err := uuid.NewV7()
        if err != nil {
                return err
        }
        return s.repo.Create(ctx, &domain.Customer{
                ID: id.String(), UserID: userID, Status: domain.StatusNew,
        })
}

func (s *CustomerService) Get(ctx context.Context, userID string) (*domain.Customer, error) {
        return s.repo.FindByUserID(ctx, userID)
}

func (s *CustomerService) UpdateProfile(ctx context.Context, userID, first, last, phone, dob, addr, citizenship string) (*domain.Customer, error) {
        c, err := s.repo.FindByUserID(ctx, userID)
        if err != nil {
                return nil, err
        }
        if err := c.UpdateProfile(first, last, phone, dob, addr, citizenship); err != nil {
                return nil, err // ErrProfileLocked и т.п. — наверх
        }
        if err := s.repo.Update(ctx, c); err != nil {
                return nil, err
        }
        // Анкета готова — оповещаем шину (ТЗ 5.3: customer.profile_filled).
        // Первый раз клиент переходит NEW → PROFILE_FILLED; повторные
        // правки (пока не подана KYC) тоже публикуем — событие факта.
        if s.events != nil {
                s.events.ProfileFilled(ctx, c.ID, c.UserID)
        }
        return c, nil
}

// SetStatusByCustomerID — сменить статус профиля по событию от kyc-service
// (ТЗ 5.3: слушает kyc.application_approved / application_rejected).
// Идемпотентно: повторное событие (at-least-once!) или уже нужный статус —
// не ошибка. Незапланированный переход — ошибка наверх (поймает лог).
func (s *CustomerService) SetStatusByCustomerID(ctx context.Context, customerID string, to domain.CustomerStatus) error {
        c, err := s.repo.FindByCustomerID(ctx, customerID)
        if err != nil {
                return err
        }
        if c.Status == to {
                return nil // уже там — дубликат события
        }
        if !domain.CanTransition(c.Status, to) {
                return domain.ErrBadTransition
        }
        return s.repo.UpdateStatusByID(ctx, customerID, string(to))
}

func (s *CustomerService) List(ctx context.Context, pageSize int, pageToken, statusFilter string) ([]domain.Customer, int, error) {
        offset := 0
        if pageToken != "" {
                // в реале — курсор; для учёбы offset, но с защитой
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
