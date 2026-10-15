// Package domain — чистая бизнес-логика kyc-service: статусная машина
// заявки, правила авто-чеков. Никаких зависимостей от БД, Kafka, gRPC —
// только язык и стандартная библиотека. Всё остальное (хранение, шина,
// транспорт) — детали в internal/app и internal/adapters.
package domain

import (
	"errors"
	"fmt"
	"time"
)

// ApplicationStatus — статус заявки на верификацию.
type ApplicationStatus string

const (
	// StatusDraft — заявка создана, но ещё не отправлена.
	// В текущем флоу не используется (SubmitApplication создаёт заявку
	// сразу в SUBMITTED), но переход DRAFT → SUBMITTED описан в ТЗ,
	// поэтому машина его знает.
	StatusDraft ApplicationStatus = "DRAFT"
	// StatusSubmitted — подана, ждёт авто-чеков.
	StatusSubmitted ApplicationStatus = "SUBMITTED"
	// StatusAutoChecks — воркер прогоняет мок-проверки.
	StatusAutoChecks ApplicationStatus = "AUTO_CHECKS"
	// StatusInReview — авто-чеки пройдены, заявку можно забрать в работу.
	StatusInReview ApplicationStatus = "IN_REVIEW"
	// StatusApproved — верификация пройдена.
	StatusApproved ApplicationStatus = "APPROVED"
	// StatusAutoRejected — отклонена автоматически (санкции/возраст).
	StatusAutoRejected ApplicationStatus = "AUTO_REJECTED"
	// StatusRejected — отклонена офицером.
	StatusRejected ApplicationStatus = "REJECTED"
	// StatusDocsRequested — офицер запросил дополнительные документы.
	StatusDocsRequested ApplicationStatus = "DOCS_REQUESTED"
)

// activeStatuses — статусы, в которых заявка считается «живой».
// Активная заявка может быть только одна (частичный уникальный индекс в БД).
var activeStatuses = map[ApplicationStatus]bool{
	StatusSubmitted:     true,
	StatusAutoChecks:    true,
	StatusInReview:      true,
	StatusDocsRequested: true,
}

// IsActive — заявка ещё в работе (запрет повторной подачи и т.п.).
func (s ApplicationStatus) IsActive() bool { return activeStatuses[s] }

// allowedTransitions — таблица переходов статусной машины из ТЗ 5.4:
//
//	DRAFT → SUBMITTED → AUTO_CHECKS → IN_REVIEW → APPROVED
//	                        │              ├→ REJECTED
//	                        └→ AUTO_REJECTED └→ DOCS_REQUESTED → SUBMITTED
//
// Это ЕДИНСТВЕННОЕ место, где описано, «что за чем может». Хочешь новый
// переход — добавляй здесь и правь тесты; остальной код менять не нужно.
var allowedTransitions = map[ApplicationStatus][]ApplicationStatus{
	StatusDraft:         {StatusSubmitted},
	StatusSubmitted:     {StatusAutoChecks},
	StatusAutoChecks:    {StatusInReview, StatusAutoRejected},
	StatusInReview:      {StatusApproved, StatusRejected, StatusDocsRequested},
	StatusDocsRequested: {StatusSubmitted},
	// Терминальные статусы: из них выходов нет.
	StatusApproved:     {},
	StatusRejected:     {},
	StatusAutoRejected: {},
}

// CanTransition — разрешён ли переход между статусами.
func CanTransition(from, to ApplicationStatus) bool {
	for _, s := range allowedTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Ошибки домена. grpcapi переводит их в gRPC-коды,
// app и адаптеры возвращают их как есть.
var (
	// ErrNotFound — заявки с таким id нет.
	ErrNotFound = errors.New("application not found")
	// ErrAlreadyExists — у клиента уже есть активная заявка.
	ErrAlreadyExists = errors.New("active application already exists")
	// ErrInvalidTransition — переход запрещён машиной статусов или кто-то
	// обогнал нас (гонка): заявка уже не в том статусе, из которого мы шли.
	ErrInvalidTransition = errors.New("transition not allowed")
	// ErrAlreadyClaimed — заявку уже забрал другой офицер.
	ErrAlreadyClaimed = errors.New("application already claimed")
	// ErrNotReviewer — вердикт выносит только офицер, забравший заявку.
	ErrNotReviewer = errors.New("application is claimed by another officer")
	// ErrProfileNotFilled — профиль не готов к подаче (не заполнен или заблокирован).
	ErrProfileNotFilled = errors.New("customer profile is not ready for KYC")
	// ErrMissingDocuments — не загружены обязательные документы (паспорт + селфи).
	ErrMissingDocuments = errors.New("required documents are missing")
	// ErrRejectReasonRequired — отклонить заявку без причины нельзя.
	ErrRejectReasonRequired = errors.New("reject reason is required")
)

// Application — заявка на верификацию.
type Application struct {
	ID           string
	CustomerID   string
	FullName     string // «снимок» ФИО на момент подачи
	DateOfBirth  string // «снимок» даты рождения, YYYY-MM-DD
	ReviewerID   string // пусто — заявку никто не взял в работу
	Status       ApplicationStatus
	RejectReason string
	CreatedAt    string // RFC3339 (строкой приходит из БД)
	UpdatedAt    string
}

// Transition — сменить статус заявки. Домен сам защищает инварианты:
// переход должен быть разрешён таблицей, а REJECTED требует причину.
// Само сохранение делает репозиторий (см. порт app.Repo).
func (a *Application) Transition(to ApplicationStatus) error {
	if !CanTransition(a.Status, to) {
		return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, a.Status, to)
	}
	if to == StatusRejected && a.RejectReason == "" {
		return ErrRejectReasonRequired
	}
	if to != StatusRejected && to != StatusAutoRejected {
		a.RejectReason = "" // новая жизнь — старые причины стираем
	}
	a.Status = to
	return nil
}

// Check — результат одного авто-чека (SANCTIONS / AGE).
type Check struct {
	ID            string
	ApplicationID string
	Type          string
	Passed        bool
	Details       string
}

// AgeAtLeast — исполнилось ли человеку min лет на момент now.
// Отдельная функция в домене, потому что это бизнес-правило
// («возраст ≥ 18» из ТЗ), а не техническая деталь.
func AgeAtLeast(dob string, now time.Time, min int) (bool, error) {
	b, err := time.Parse("2006-01-02", dob)
	if err != nil {
		return false, fmt.Errorf("bad date of birth %q: %w", dob, err)
	}
	age := now.Year() - b.Year()
	// День рождения в этом году ещё не наступил — минус один год.
	// Сравниваем месяц/день, а не YearDay: на високосных годах YearDay
	// одной и той же календарной даты различается.
	if now.Month() < b.Month() || (now.Month() == b.Month() && now.Day() < b.Day()) {
		age--
	}
	return age >= min, nil
}
