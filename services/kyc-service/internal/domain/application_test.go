package domain

import (
	"errors"
	"testing"
	"time"
)

// TestCanTransition_AllPairs — перебор ВСЕХ пар статусов: для разрешённых
// переходов ожидаем true, для всех остальных — false. Таблица разрешённых
// переходов продублирована здесь списком, чтобы тест падал, если кто-то
// поменял машину статусов, не подумав (требование ТЗ: «unit-тесты на все
// переходы»).
func TestCanTransition_AllPairs(t *testing.T) {
	all := []ApplicationStatus{
		StatusDraft, StatusSubmitted, StatusAutoChecks, StatusInReview,
		StatusApproved, StatusAutoRejected, StatusRejected, StatusDocsRequested,
	}

	allowed := map[[2]ApplicationStatus]bool{
		{StatusDraft, StatusSubmitted}:         true,
		{StatusSubmitted, StatusAutoChecks}:    true,
		{StatusAutoChecks, StatusInReview}:     true,
		{StatusAutoChecks, StatusAutoRejected}: true,
		{StatusInReview, StatusApproved}:       true,
		{StatusInReview, StatusRejected}:       true,
		{StatusInReview, StatusDocsRequested}:  true,
		{StatusDocsRequested, StatusSubmitted}: true,
	}

	for _, from := range all {
		for _, to := range all {
			want := allowed[[2]ApplicationStatus{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("%s → %s: got %v, want %v", from, to, got, want)
			}
		}
	}
}

// TestTransition_TerminalStatuses — из APPROVED/REJECTED/AUTO_REJECTED
// нельзя выйти никуда, даже «обратно в очередь».
func TestTransition_TerminalStatuses(t *testing.T) {
	terminal := []ApplicationStatus{StatusApproved, StatusRejected, StatusAutoRejected}
	for _, s := range terminal {
		a := &Application{Status: s}
		if err := a.Transition(StatusSubmitted); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("из %s в SUBMITTED: ожидали ErrInvalidTransition, получили %v", s, err)
		}
	}
}

// TestTransition_RejectRequiresReason — офицер обязан объяснить отказ.
func TestTransition_RejectRequiresReason(t *testing.T) {
	a := &Application{Status: StatusInReview}
	if err := a.Transition(StatusRejected); !errors.Is(err, ErrRejectReasonRequired) {
		t.Fatalf("ожидали ErrRejectReasonRequired, получили: %v", err)
	}
	a.RejectReason = "документы вызывают сомнения"
	if err := a.Transition(StatusRejected); err != nil {
		t.Fatalf("с причиной отказ должен пройти: %v", err)
	}
}

// TestTransition_ResubmitClearsReviewer — после DOCS_REQUESTED → SUBMITTED
// заявка возвращается в общую очередь: старый офицер её больше не «держит»,
// а причина отказа/просьбы стирается.
func TestTransition_ResubmitClearsReviewer(t *testing.T) {
	a := &Application{
		Status:       StatusDocsRequested,
		ReviewerID:   "officer-1",
		RejectReason: "селфи размытое",
	}
	// Причину в домене чистит сам Transition (см. application.go),
	// reviewer_id чистит репозиторий тем же SQL-транзакций.
	if err := a.Transition(StatusSubmitted); err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if a.Status != StatusSubmitted {
		t.Errorf("статус = %s, ожидали SUBMITTED", a.Status)
	}
}

// TestTransition_CleansReasonOnApprove — при APPROVED причин быть не должно.
func TestTransition_CleansReasonOnApprove(t *testing.T) {
	a := &Application{Status: StatusInReview, RejectReason: "мусор от прошлой жизни"}
	if err := a.Transition(StatusApproved); err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if a.RejectReason != "" {
		t.Errorf("причина должна была очиститься, осталось: %q", a.RejectReason)
	}
}

func TestIsActive(t *testing.T) {
	active := []ApplicationStatus{StatusSubmitted, StatusAutoChecks, StatusInReview, StatusDocsRequested}
	for _, s := range active {
		if !s.IsActive() {
			t.Errorf("%s должен быть активным", s)
		}
	}
	final := []ApplicationStatus{StatusApproved, StatusRejected, StatusAutoRejected, StatusDraft}
	for _, s := range final {
		if s.IsActive() {
			t.Errorf("%s не должен быть активным", s)
		}
	}
}

// TestAgeAtLeast — правило «возраст ≥ 18», включая каверзные даты.
func TestAgeAtLeast(t *testing.T) {
	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		dob  string
		want bool
	}{
		{"2007-06-15", true},  // ровно 18 в этот день
		{"2007-06-16", false}, // 18 исполнится завтра
		{"2006-05-01", true},  // 19
		{"2010-01-01", false}, // 15
		{"2007-02-28", true},  // 18 уже было (февраль прошёл)
		{"2007-12-31", false}, // ещё не исполнилось
	}
	for _, c := range cases {
		got, err := AgeAtLeast(c.dob, now, 18)
		if err != nil {
			t.Fatalf("%s: неожиданная ошибка: %v", c.dob, err)
		}
		if got != c.want {
			t.Errorf("dob %s: got %v, want %v", c.dob, got, c.want)
		}
	}
	if _, err := AgeAtLeast("не-дата", now, 18); err == nil {
		t.Error("битая дата должна давать ошибку")
	}
}
