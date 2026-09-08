package app_test

import (
	"context"
	"errors"
	"testing"

	"mini-core/services/auth-service/internal/app"
	"mini-core/services/auth-service/internal/domain"
)

// Фейковая вилка: вместо БД запоминает юзера в памяти.
// err — «притвориться», что репозиторий ответил ошибкой.
type fakeRepo struct {
	saved *domain.User
	err   error
}

func (f *fakeRepo) Create(_ context.Context, u *domain.User) error {
	if f.err != nil {
		return f.err // симулируем сбой БД, ничего не сохраняем
	}
	f.saved = u
	return nil
}

func TestRegister_Ok(t *testing.T) {
	repo := &fakeRepo{} // 1. ARRANGE — подготовка
	svc := app.NewRegister(repo)

	u, err := svc.Register(context.Background(), "  Ivan@Example.COM ", "secret123") // 2. ACT — действие

	if err != nil { // 3. ASSERT — проверки
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if u.Email != "ivan@example.com" {
		t.Errorf("email не нормализован: %q", u.Email)
	}
	if u.PasswordHash == "secret123" {
		t.Error("ПАРОЛЬ СОХРАНЁН ОТКРЫТЫМ ТЕКСТОМ!")
	}
	if repo.saved == nil {
		t.Error("юзер не дошёл до репозитория")
	}
}

func TestRegister_ShortPassword(t *testing.T) {
	repo := &fakeRepo{}
	svc := app.NewRegister(repo)

	_, err := svc.Register(context.Background(), "ivan@example.com", "123")

	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("ожидали ErrInvalidInput, получили: %v", err)
	}
	if repo.saved != nil {
		t.Error("юзер с коротким паролем утёк в «БД»!")
	}
}

func TestRegister_EmailTaken(t *testing.T) {
	repo := &fakeRepo{err: domain.ErrEmailTaken} // вилка притворяется базой с занятым email
	svc := app.NewRegister(repo)

	_, err := svc.Register(context.Background(), "ivan@example.com", "secret123")

	if !errors.Is(err, domain.ErrEmailTaken) {
		t.Fatalf("ожидали ErrEmailTaken, получили: %v", err)
	}
}

func TestRegister_BadEmail(t *testing.T) {
	repo := &fakeRepo{}
	svc := app.NewRegister(repo)

	_, err := svc.Register(context.Background(), "No-dogs", "secret123")

	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("ожидали ErrInvalidInput, получили: %v", err)
	}
	if repo.saved != nil {
		t.Error("юзер с некорректным email утёк в «БД»!")
	}
}
