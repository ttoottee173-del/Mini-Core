package app

// Тесты сценариев на фейковых реализациях портов: без БД, Kafka и gRPC.
// Фейковый репозиторий честно имитирует «оптимистичную блокировку» SQL:
// GuardedTransition проверяет, что заявка всё ещё в ожидаемом статусе.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"mini-core/services/kyc-service/internal/domain"
)

// --- фейки ---

type fakeRepo struct {
	mu   sync.Mutex
	apps map[string]*domain.Application
}

func newFakeRepo() *fakeRepo { return &fakeRepo{apps: map[string]*domain.Application{}} }

func (f *fakeRepo) Create(_ context.Context, a *domain.Application) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range f.apps {
		if x.CustomerID == a.CustomerID && x.Status.IsActive() {
			return domain.ErrAlreadyExists
		}
	}
	cp := *a
	f.apps[a.ID] = &cp
	return nil
}

func (f *fakeRepo) FindByID(_ context.Context, id string) (*domain.Application, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.apps[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *a
	return &cp, nil
}

func (f *fakeRepo) FindActiveByCustomer(_ context.Context, customerID string) (*domain.Application, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range f.apps {
		if x.CustomerID == customerID && x.Status.IsActive() {
			cp := *x
			return &cp, nil
		}
	}
	return nil, nil
}

func (f *fakeRepo) List(_ context.Context, _, _ int, _ string) ([]domain.Application, error) {
	return nil, nil
}

func (f *fakeRepo) Claim(_ context.Context, id, officerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.apps[id]
	if !ok {
		return domain.ErrNotFound
	}
	if a.Status != domain.StatusInReview {
		return domain.ErrInvalidTransition
	}
	if a.ReviewerID != "" {
		return domain.ErrAlreadyClaimed
	}
	a.ReviewerID = officerID
	return nil
}

func (f *fakeRepo) GuardedTransition(_ context.Context, a *domain.Application, to domain.ApplicationStatus, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.apps[a.ID]
	if !ok {
		return domain.ErrNotFound
	}
	// Имитация UPDATE ... WHERE id AND status = from: если реальный статус
	// не совпал с ожидаемым — «гонка», 0 строк обновлено.
	if stored.Status != a.Status {
		return domain.ErrInvalidTransition
	}
	stored.RejectReason = a.RejectReason
	if to == domain.StatusSubmitted {
		stored.ReviewerID = "" // ResetReviewer той же транзакцией
	}
	if err := stored.Transition(to); err != nil {
		return err
	}
	return nil
}

func (f *fakeRepo) SaveCheck(_ context.Context, _ *domain.Check) error { return nil }

func (f *fakeRepo) SanctionsMatch(_ context.Context, fullName, dob string) (bool, error) {
	return fullName == "Ivan Ivanov" && dob == "1990-01-01", nil
}

func (f *fakeRepo) NextSubmitted(_ context.Context) (*domain.Application, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, want := range []domain.ApplicationStatus{domain.StatusSubmitted, domain.StatusAutoChecks} {
		for _, x := range f.apps {
			if x.Status == want {
				cp := *x
				return &cp, nil
			}
		}
	}
	return nil, nil
}

type fakeCustomers struct{ a *Applicant }

func (f *fakeCustomers) GetByUser(_ context.Context, _ string) (*Applicant, error) {
	if f.a == nil {
		return nil, ErrCustomerNotFound
	}
	return f.a, nil
}

type fakeDocuments struct{ err error }

func (f *fakeDocuments) HasRequired(_ context.Context, _ string) error { return f.err }

type eventRecord struct {
	eventType, applicationID, customerID, reason string
}

type fakeEvents struct{ got []eventRecord }

func (f *fakeEvents) Publish(_ context.Context, eventType, applicationID, customerID, reason string) {
	f.got = append(f.got, eventRecord{eventType, applicationID, customerID, reason})
}

// --- помощники ---

func readyProfile() *Applicant {
	return &Applicant{
		CustomerID:  "c-1",
		UserID:      "u-1",
		Status:      "PROFILE_FILLED",
		FullName:    "Giorgi Meladze",
		DateOfBirth: "1995-04-12",
	}
}

func newService(repo *fakeRepo, cust *fakeCustomers, ev *fakeEvents) *Kyc {
	svc := New(repo, cust, &fakeDocuments{}, ev)
	svc.now = func() time.Time { return time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC) }
	return svc
}

// --- тесты подачи ---

func TestSubmit_HappyPath(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, ev)

	a, err := svc.SubmitApplication(context.Background(), "u-1")
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if a.Status != domain.StatusSubmitted {
		t.Errorf("статус = %s, ожидали SUBMITTED", a.Status)
	}
	if a.FullName != "Giorgi Meladze" || a.DateOfBirth != "1995-04-12" {
		t.Errorf("снимок профиля не сохранился: %+v", a)
	}
	if len(ev.got) != 1 || ev.got[0].eventType != "application_submitted" {
		t.Errorf("ожидали событие application_submitted, получили %+v", ev.got)
	}
}

func TestSubmit_ProfileNotFilled(t *testing.T) {
	prof := readyProfile()
	prof.Status = "NEW"
	svc := newService(newFakeRepo(), &fakeCustomers{a: prof}, &fakeEvents{})

	if _, err := svc.SubmitApplication(context.Background(), "u-1"); !errors.Is(err, domain.ErrProfileNotFilled) {
		t.Fatalf("ожидали ErrProfileNotFilled, получили: %v", err)
	}
}

func TestSubmit_NoDocuments(t *testing.T) {
	svc := New(newFakeRepo(), &fakeCustomers{a: readyProfile()}, &fakeDocuments{err: domain.ErrMissingDocuments}, &fakeEvents{})
	svc.now = func() time.Time { return time.Now() }

	if _, err := svc.SubmitApplication(context.Background(), "u-1"); !errors.Is(err, domain.ErrMissingDocuments) {
		t.Fatalf("ожидали ErrMissingDocuments, получили: %v", err)
	}
}

func TestSubmit_DuplicateActive(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, ev)
	ctx := context.Background()

	if _, err := svc.SubmitApplication(ctx, "u-1"); err != nil {
		t.Fatalf("первая подача должна пройти: %v", err)
	}
	if _, err := svc.SubmitApplication(ctx, "u-1"); !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("ожидали ErrAlreadyExists, получили: %v", err)
	}
}

func TestSubmit_ResubmitAfterDocsRequested(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, ev)
	ctx := context.Background()

	a, _ := svc.SubmitApplication(ctx, "u-1")
	// Доходим до DOCS_REQUESTED вручную (воркер + офицер в тесте не участвуют).
	repo.apps[a.ID].Status = domain.StatusInReview
	if err := repo.apps[a.ID].Transition(domain.StatusDocsRequested); err != nil {
		t.Fatalf("не удалось поставить DOCS_REQUESTED: %v", err)
	}

	again, err := svc.SubmitApplication(ctx, "u-1")
	if err != nil {
		t.Fatalf("повторная подача должна пройти: %v", err)
	}
	if again.Status != domain.StatusSubmitted {
		t.Errorf("статус = %s, ожидали SUBMITTED", again.Status)
	}
	if ev.got[len(ev.got)-1].eventType != "application_submitted" {
		t.Errorf("ожидали повторное application_submitted, получили %+v", ev.got)
	}
}

// --- тесты офицерских сценариев ---

func reviewApplication(t *testing.T, repo *fakeRepo, svc *Kyc) *domain.Application {
	t.Helper()
	ctx := context.Background()
	a, err := svc.SubmitApplication(ctx, "u-1")
	if err != nil {
		t.Fatalf("подача: %v", err)
	}
	// Воркер перевёл в IN_REVIEW (авто-чеки пройдены).
	repo.apps[a.ID].Status = domain.StatusAutoChecks
	if err := repo.apps[a.ID].Transition(domain.StatusInReview); err != nil {
		t.Fatalf("авто-чеки: %v", err)
	}
	a, _ = svc.Claim(ctx, a.ID, "officer-1")
	return a
}

func TestClaim_Contention(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, &fakeEvents{})
	ctx := context.Background()
	a, _ := svc.SubmitApplication(ctx, "u-1")
	repo.apps[a.ID].Status = domain.StatusAutoChecks
	_ = repo.apps[a.ID].Transition(domain.StatusInReview)

	if _, err := svc.Claim(ctx, a.ID, "officer-1"); err != nil {
		t.Fatalf("первый офицер должен выиграть: %v", err)
	}
	if _, err := svc.Claim(ctx, a.ID, "officer-2"); !errors.Is(err, domain.ErrAlreadyClaimed) {
		t.Fatalf("ожидали ErrAlreadyClaimed, получили: %v", err)
	}
}

func TestClaim_WrongStatus(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, &fakeEvents{})
	ctx := context.Background()
	a, _ := svc.SubmitApplication(ctx, "u-1") // ещё SUBMITTED, авто-чеки не прошли

	if _, err := svc.Claim(ctx, a.ID, "officer-1"); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("ожидали ErrInvalidTransition, получили: %v", err)
	}
}

func TestApprove_HappyPath(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, ev)
	a := reviewApplication(t, repo, svc)

	got, err := svc.Approve(context.Background(), a.ID, "officer-1", "ок")
	if err != nil {
		t.Fatalf("одобрение: %v", err)
	}
	if got.Status != domain.StatusApproved {
		t.Errorf("статус = %s, ожидали APPROVED", got.Status)
	}
	last := ev.got[len(ev.got)-1]
	if last.eventType != "application_approved" {
		t.Errorf("ожидали application_approved, получили %s", last.eventType)
	}
}

func TestApprove_WrongOfficer(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, &fakeEvents{})
	a := reviewApplication(t, repo, svc)

	if _, err := svc.Approve(context.Background(), a.ID, "officer-2", ""); !errors.Is(err, domain.ErrNotReviewer) {
		t.Fatalf("ожидали ErrNotReviewer, получили: %v", err)
	}
}

func TestReject_RequiresReason(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, &fakeEvents{})
	a := reviewApplication(t, repo, svc)

	if _, err := svc.Reject(context.Background(), a.ID, "officer-1", ""); !errors.Is(err, domain.ErrRejectReasonRequired) {
		t.Fatalf("ожидали ErrRejectReasonRequired, получили: %v", err)
	}
}

func TestReject_HappyPath(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, ev)
	a := reviewApplication(t, repo, svc)

	got, err := svc.Reject(context.Background(), a.ID, "officer-1", "паспорт поддельный")
	if err != nil {
		t.Fatalf("отказ: %v", err)
	}
	if got.Status != domain.StatusRejected || got.RejectReason != "паспорт поддельный" {
		t.Errorf("неожиданный результат: %+v", got)
	}
	last := ev.got[len(ev.got)-1]
	if last.eventType != "application_rejected" || last.reason != "паспорт поддельный" {
		t.Errorf("ожидали application_rejected с причиной, получили %+v", last)
	}
}

func TestRequestMoreDocuments_HappyPath(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, ev)
	a := reviewApplication(t, repo, svc)

	got, err := svc.RequestMoreDocuments(context.Background(), a.ID, "officer-1", "пришлите селфи с паспортом")
	if err != nil {
		t.Fatalf("запрос документов: %v", err)
	}
	if got.Status != domain.StatusDocsRequested {
		t.Errorf("статус = %s, ожидали DOCS_REQUESTED", got.Status)
	}
	if last := ev.got[len(ev.got)-1]; last.eventType != "docs_requested" {
		t.Errorf("ожидали docs_requested, получили %s", last.eventType)
	}
}

// --- тесты воркера ---

func runWorkerOnce(t *testing.T, svc *Kyc) {
	t.Helper()
	// processOne неэкспортирован — запускаем через один короткий тик.
	ctx, cancel := context.WithCancel(context.Background())
	go svc.RunAutoChecks(ctx, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	cancel()
}

func TestWorker_SanctionsHit(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, &fakeEvents{})
	profile := readyProfile()
	profile.FullName = "Ivan Ivanov" // есть в фейковом санкционном списке
	profile.DateOfBirth = "1990-01-01"
	svc2 := New(repo, &fakeCustomers{a: profile}, &fakeDocuments{}, ev)
	svc2.now = svc.now

	a, _ := svc2.SubmitApplication(context.Background(), "u-1")
	runWorkerOnce(t, svc2)

	got, _ := repo.FindByID(context.Background(), a.ID)
	if got.Status != domain.StatusAutoRejected {
		t.Fatalf("статус = %s, ожидали AUTO_REJECTED", got.Status)
	}
	if got.RejectReason == "" {
		t.Error("должна быть причина отказа")
	}
	last := ev.got[len(ev.got)-1]
	if last.eventType != "application_rejected" {
		t.Errorf("ожидали application_rejected, получили %s", last.eventType)
	}
}

func TestWorker_Underage(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	prof := readyProfile()
	prof.DateOfBirth = "2010-01-01" // 15 лет
	svc := New(repo, &fakeCustomers{a: prof}, &fakeDocuments{}, ev)
	svc.now = func() time.Time { return time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC) }

	a, _ := svc.SubmitApplication(context.Background(), "u-1")
	runWorkerOnce(t, svc)

	got, _ := repo.FindByID(context.Background(), a.ID)
	if got.Status != domain.StatusAutoRejected {
		t.Fatalf("статус = %s, ожидали AUTO_REJECTED", got.Status)
	}
}

func TestWorker_PassToReview(t *testing.T) {
	repo, ev := newFakeRepo(), &fakeEvents{}
	svc := newService(repo, &fakeCustomers{a: readyProfile()}, ev)

	a, _ := svc.SubmitApplication(context.Background(), "u-1")
	runWorkerOnce(t, svc)

	got, _ := repo.FindByID(context.Background(), a.ID)
	if got.Status != domain.StatusInReview {
		t.Fatalf("статус = %s, ожидали IN_REVIEW", got.Status)
	}
	for _, r := range ev.got {
		if r.eventType == "application_rejected" {
			t.Errorf("чистая заявка не должна отклоняться: %+v", ev.got)
		}
	}
}
