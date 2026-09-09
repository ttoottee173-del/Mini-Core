package app_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/golang-jwt/jwt/v5"

	"mini-core/services/auth-service/internal/adapters/jwtauth"
	"mini-core/services/auth-service/internal/app"
	"mini-core/services/auth-service/internal/domain"
)

type fakeUsers struct {
	u   *domain.User
	err error
}

func (f *fakeUsers) FindByEmail(_ context.Context, _ string) (*domain.User, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.u, nil
}

type fakeTokens struct{ called bool }

func (f *fakeTokens) IssueAccess(userID, role string, _ time.Duration) (string, error) {
	f.called = true
	return "test.access." + userID, nil
}

type fakeRefresh struct{ hashes []string }

func (f *fakeRefresh) Create(_ context.Context, hash, _ string, _ time.Time) error {
	f.hashes = append(f.hashes, hash)
	return nil
}

func loginFixture(t *testing.T) (*fakeTokens, *fakeRefresh, *app.LoginService) {
	t.Helper()
	hash, err := argon2id.CreateHash("secret123", argon2id.DefaultParams) // ~80мс, один раз
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	users := &fakeUsers{u: &domain.User{
		ID: "u-1", Email: "ivan@example.com", PasswordHash: hash, Role: domain.RoleClient,
	}}
	tokens := &fakeTokens{}
	refresh := &fakeRefresh{}
	return tokens, refresh, app.NewLogin(users, tokens, refresh)
}

func TestLogin_Ok(t *testing.T) {
	tokens, refresh, svc := loginFixture(t)

	access, refreshTok, err := svc.Login(context.Background(), " Ivan@Example.COM ", "secret123")

	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if !tokens.called {
		t.Error("IssueAccess не вызван — откуда access?")
	}
	if access == "" || refreshTok == "" {
		t.Error("токены пустые")
	}
	if len(refresh.hashes) != 1 {
		t.Errorf("ожидали 1 запись refresh, получили %d", len(refresh.hashes))
	}
	if len(refresh.hashes) == 1 && refresh.hashes[0] == refreshTok {
		t.Error("в БД лежит сам refresh-токен, а не хеш!")
	}
}

func TestLogin_WrongPassword(t *testing.T) {
	tokens, refresh, svc := loginFixture(t)

	_, _, err := svc.Login(context.Background(), "ivan@example.com", "wrong-pass")

	if !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatalf("ожидали ErrInvalidCredentials, получили: %v", err)
	}
	if tokens.called {
		t.Error("токен выдан при неверном пароле!")
	}
	if len(refresh.hashes) != 0 {
		t.Error("refresh записан при неверном пароле!")
	}
}

func TestLogin_UnknownEmail(t *testing.T) {
	users := &fakeUsers{err: domain.ErrNotFound} // репозиторий вернёт "not found"
	tokens := &fakeTokens{}
	refresh := &fakeRefresh{}
	svc := app.NewLogin(users, tokens, refresh)

	_, _, err := svc.Login(context.Background(), "unknown@example.com", "anypass")

	if !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatalf("ожидали ErrInvalidCredentials, получили: %v", err)
	}
	if tokens.called {
		t.Error("токен выдан для неизвестного email!")
	}
	if len(refresh.hashes) != 0 {
		t.Error("refresh записан для неизвестного email!")
	}
}

func TestLogin_RealIssuer(t *testing.T) {
	// 1. Генерируем RSA-ключ 2048 бит
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("не удалось сгенерировать RSA-ключ: %v", err)
	}

	// 2. Сериализуем приватный ключ в байты (формат PKCS1)
	privPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY", // PKCS1; для PKCS8 было бы "PRIVATE KEY"
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	// 3. Создаём Issuer (конструктор принимает только []byte)
	issuer, err := jwtauth.NewIssuer(privPEM)
	if err != nil {
		t.Fatalf("не удалось создать Issuer: %v", err)
	}

	// 4. Выдаём токен
	userID := "u-123"
	role := "CLIENT"
	tokenString, err := issuer.IssueAccess(userID, role, 5*time.Minute)
	if err != nil {
		t.Fatalf("IssueAccess вернул ошибку: %v", err)
	}
	if tokenString == "" {
		t.Fatal("токен пустой")
	}

	// 5. Парсим токен с публичным ключом
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))

	// Проверяем стандартные claims (например, subject)
	var stdClaims jwt.RegisteredClaims
	_, err = parser.ParseWithClaims(tokenString, &stdClaims, func(token *jwt.Token) (interface{}, error) {
		return &privateKey.PublicKey, nil
	})
	if err != nil {
		t.Fatalf("не удалось распарсить стандартные claims: %v", err)
	}
	if stdClaims.Subject != userID {
		t.Errorf("subject = %q, ожидалось %q", stdClaims.Subject, userID)
	}

	// Проверяем кастомный claim "role" через MapClaims
	var customClaims jwt.MapClaims
	_, err = parser.ParseWithClaims(tokenString, &customClaims, func(token *jwt.Token) (interface{}, error) {
		return &privateKey.PublicKey, nil
	})
	if err != nil {
		t.Fatalf("не удалось распарсить кастомные claims: %v", err)
	}
	if roleClaim, ok := customClaims["role"]; !ok || roleClaim != role {
		t.Errorf("role claim = %v, ожидалось %q", roleClaim, role)
	}
}
