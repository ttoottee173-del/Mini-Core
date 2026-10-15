package app

import (
	"context"
	"errors"

	"mini-core/services/kyc-service/internal/domain"
)

// Ошибки, специфичные для внешних зависимостей KYC.
var (
	// ErrCustomerNotFound — customer-service не нашёл профиль по user_id.
	ErrCustomerNotFound = errors.New("customer profile not found")
)

// Applicant — «выжимка» из профиля клиента, нужная kyc-service.
// Заполняется адаптером customers (gRPC-вызов customer-service).
type Applicant struct {
	CustomerID  string // id профиля из customer-service
	UserID      string // владелец профиля
	Status      string // статус профиля строкой ("PROFILE_FILLED", ...)
	FullName    string // first + last — для санкционного чека
	DateOfBirth string // YYYY-MM-DD — для санкционного чека и проверки возраста
}

// Repo — порт хранилища заявок. Реализация — internal/adapters/postgres
// (sqlc + маппинг ошибок). Здесь перечислено ровно то, что нужно сценариям.
type Repo interface {
	// Create вставляет новую заявку (статус SUBMITTED).
	// «Не более одной активной» защищает частичный уникальный индекс:
	// нарушение приходит ошибкой unique_violation → домен ErrAlreadyExists.
	Create(ctx context.Context, a *domain.Application) error
	FindByID(ctx context.Context, id string) (*domain.Application, error)
	// FindActiveByCustomer — активная заявка клиента или nil, nil (нет — не ошибка).
	FindActiveByCustomer(ctx context.Context, customerID string) (*domain.Application, error)
	List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Application, error)
	// Claim — офицер забирает заявку IN_REVIEW в работу.
	// Оптимистическая блокировка: UPDATE ... WHERE reviewer_id IS NULL.
	// 0 строк → ErrAlreadyClaimed (кто-то успел раньше).
	Claim(ctx context.Context, id, officerID string) error
	// GuardedTransition — сменить статус заявки a (a.Status — ожидаемый
	// «from») на to и записать строку аудита В ОДНОЙ транзакции БД.
	// 0 обновлённых строк → ErrInvalidTransition (гонка или неверный статус).
	GuardedTransition(ctx context.Context, a *domain.Application, to domain.ApplicationStatus, actor, comment string) error
	// SaveCheck — результат авто-чека (идемпотентно по application_id + type).
	SaveCheck(ctx context.Context, c *domain.Check) error
	// SanctionsMatch — есть ли в мок-списке человек с таким ФИО и датой рождения.
	SanctionsMatch(ctx context.Context, fullName, dob string) (bool, error)
	// NextSubmitted — следующая заявка на обработку (SUBMITTED, а при
	// застрявшей в прошлом запуске — и AUTO_CHECKS). nil, nil — очередь пуста.
	NextSubmitted(ctx context.Context) (*domain.Application, error)
}

// Customers — порт «спросить customer-service о профиле».
// Реализация — internal/adapters/customers (gRPC-клиент).
type Customers interface {
	GetByUser(ctx context.Context, userID string) (*Applicant, error)
}

// Documents — порт «есть ли обязательные документы» (паспорт + селфи).
// Реализация — internal/adapters/doccheck: пока document-service не написан,
// там стоит заглушка; когда появится — заменится на gRPC-клиент без
// изменений в app и domain.
type Documents interface {
	HasRequired(ctx context.Context, customerID string) error // nil = всё на месте
}

// Events — порт публикации доменных событий в Kafka (топик kyc.events).
// Реализация — internal/adapters/kafka. В тестах — фейк-записыватель.
type Events interface {
	Publish(ctx context.Context, eventType, applicationID, customerID, reason string)
}
