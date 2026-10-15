package app

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"

	"mini-core/services/kyc-service/internal/domain"
)

// RunAutoChecks — воркер асинхронного шага AUTO_CHECKS (ТЗ 5.4).
// Раз в every перебирает очередь заявок и обрабатывает одну:
// санкционный мок-чек + чек возраста ≥ 18, результаты — в kyc_checks.
// Блокируется до отмены ctx, поэтому запускать в отдельной горутине.
func (s *Kyc) RunAutoChecks(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return // штатная остановка
		case <-ticker.C:
			s.processOne(ctx)
		}
	}
}

// processOne — один такт воркера. Ошибки логируются, но не роняют воркер:
// упавшая заявка останется в очереди и будет обработана на следующем такте.
func (s *Kyc) processOne(ctx context.Context) {
	a, err := s.repo.NextSubmitted(ctx)
	if err != nil {
		log.Printf("kyc worker: очередь: %v", err)
		return
	}
	if a == nil {
		return // очередь пуста
	}
	// Заявка ещё не в работе? Переводим SUBMITTED → AUTO_CHECKS.
	// Застрявшая в AUTO_CHECKS (падение прошлого запуска) идёт дальше сразу.
	if a.Status == domain.StatusSubmitted {
		if err := s.repo.GuardedTransition(ctx, a, domain.StatusAutoChecks, "auto-checks", ""); err != nil {
			// ErrInvalidTransition = другой воркер успел раньше — это норма.
			return
		}
	}
	s.runChecks(ctx, a)
}

// runChecks — сами проверки. Каждый чек сохраняется в kyc_checks
// (идемпотентно), вердикты:
//   - санкции совпали → AUTO_REJECTED + событие application_rejected;
//   - возраст < 18 → AUTO_REJECTED + событие application_rejected;
//   - всё чисто → IN_REVIEW (очередь офицеров).
func (s *Kyc) runChecks(ctx context.Context, a *domain.Application) {
	// Чек 1: санкционный список (по снимку ФИО + дата рождения из заявки).
	hit, err := s.repo.SanctionsMatch(ctx, a.FullName, a.DateOfBirth)
	if err != nil {
		// Инфраструктурная ошибка: не выдумываем вердикт, оставляем заявку
		// в AUTO_CHECKS — воркер вернётся к ней на следующем такте.
		log.Printf("kyc worker: sanctions check %s: %v", a.ID, err)
		return
	}
	s.saveCheck(ctx, a.ID, "SANCTIONS", !hit, sanctionsDetails(hit, a.FullName))
	if hit {
		s.autoReject(ctx, a, "совпадение с санкционным списком")
		return
	}

	// Чек 2: возраст ≥ 18 на момент проверки.
	oldEnough, err := domain.AgeAtLeast(a.DateOfBirth, s.now(), 18)
	if err != nil {
		// Битая дата в снимке — данные профиля некорректны, это отказ.
		log.Printf("kyc worker: age check %s: %v", a.ID, err)
		s.autoReject(ctx, a, "некорректная дата рождения в профиле")
		return
	}
	s.saveCheck(ctx, a.ID, "AGE", oldEnough, ageDetails(oldEnough, a.DateOfBirth))
	if !oldEnough {
		s.autoReject(ctx, a, "возраст менее 18 лет")
		return
	}

	// Всё чисто: заявка ждёт офицера (ClaimApplication).
	if err := s.repo.GuardedTransition(ctx, a, domain.StatusInReview, "auto-checks", ""); err != nil {
		log.Printf("kyc worker: %s: %v", a.ID, err)
	}
}

// autoReject — отклонить автоматически и оповестить шину.
// Событие то же, что при отказе офицера (application_rejected):
// customer-service и notification-service не обязаны различать подвиды.
func (s *Kyc) autoReject(ctx context.Context, a *domain.Application, reason string) {
	a.RejectReason = reason
	if err := s.repo.GuardedTransition(ctx, a, domain.StatusAutoRejected, "auto-checks", reason); err != nil {
		log.Printf("kyc worker: auto-reject %s: %v", a.ID, err)
		return
	}
	s.events.Publish(ctx, "application_rejected", a.ID, a.CustomerID, reason)
}

func (s *Kyc) saveCheck(ctx context.Context, applicationID, checkType string, passed bool, details string) {
	id, err := uuid.NewV7()
	if err != nil {
		log.Printf("kyc worker: uuid: %v", err)
		return
	}
	err = s.repo.SaveCheck(ctx, &domain.Check{
		ID:            id.String(),
		ApplicationID: applicationID,
		Type:          checkType,
		Passed:        passed,
		Details:       details,
	})
	if err != nil {
		log.Printf("kyc worker: save check %s/%s: %v", applicationID, checkType, err)
	}
}

func sanctionsDetails(hit bool, fullName string) string {
	if hit {
		return "совпадение найдено: " + fullName
	}
	return "совпадений нет: " + fullName
}

func ageDetails(passed bool, dob string) string {
	if passed {
		return "возраст ок (дата рождения " + dob + ")"
	}
	return "меньше 18 лет (дата рождения " + dob + ")"
}
