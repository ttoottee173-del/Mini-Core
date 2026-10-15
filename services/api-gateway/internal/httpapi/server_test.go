package httpapi

// Тесты роутера на Gin: проверяем механику конвейера (роутинг, порядок
// middleware, Abort, preflight, recover) без Redis и без gRPC-серверов.
// Лимитер здесь работает в fail-open: Redis недоступен → ошибка → пропуск.
// До gRPC-вызовов дело доходит один раз — в тесте на recover, где
// nil-клиент специально паникует.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"

	"mini-core/services/api-gateway/internal/ratelimit"
	"mini-core/services/api-gateway/internal/verifier"
)

// deadRedis — клиент в никуда: порт 1 на localhost закрыт, любой вызов
// мгновенно получает «connection refused».
func deadRedis() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "localhost:1"})
}

// newTestRouter — роутер с настоящим verifier (одноразовый RSA-ключ) и
// nil вместо gRPC-клиентов.
func newTestRouter(t *testing.T) (*gin.Engine, *rsa.PrivateKey) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	ver, err := verifier.New(pubPEM)
	if err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(log, ver, ratelimit.New(deadRedis(), 10, 1), nil, nil, nil)
	return srv.Routes(), key
}

// token — подписывает access-токен тем же приватным ключом, чья публичная
// часть отдана verifier'у. Успех этого круга — то, ради чего всё затевалось.
func token(t *testing.T, key *rsa.PrivateKey) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": "user-1", "role": "user", "exp": time.Now().Add(time.Hour).Unix(),
	})
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// do — прогнать запрос через роутер; таймаут 2с, чтобы тест не завис.
func do(t *testing.T, r *gin.Engine, method, target string, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := httptest.NewRequest(method, target, strings.NewReader(body)).WithContext(ctx)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// Битый JSON → 400 INVALID_ARGUMENT; request-id сгенерирован и отдан в заголовке.
func TestRegisterBadJSON(t *testing.T) {
	r, _ := newTestRouter(t)
	rec := do(t, r, http.MethodPost, "/auth/register", nil, "{oops")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, хочу 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("нет INVALID_ARGUMENT в теле: %s", rec.Body.String())
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("X-Request-ID не отдан")
	}
}

// Нет Authorization → authMW остановил цепочку: 401 и НЕ 500 от nil-клиента.
func TestMeWithoutToken(t *testing.T) {
	r, _ := newTestRouter(t)
	rec := do(t, r, http.MethodGet, "/customers/me", nil, "")

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d, хочу 401", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "UNAUTHENTICATED") {
		t.Fatalf("нет UNAUTHENTICATED в теле: %s", rec.Body.String())
	}
}

// Валидный токен проходит authMW, лимитер пропускает (fail-open), и запрос
// доходит до хендлера, где nil-клиент паникует — recoverMW отвечает 500.
// Так мы одним тестом проверяем и authMW, и fail-open, и recover.
func TestPanicRecovered(t *testing.T) {
	r, key := newTestRouter(t)
	rec := do(t, r, http.MethodGet, "/customers/me",
		map[string]string{"Authorization": "Bearer " + token(t, key)}, "")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d, хочу 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "INTERNAL") {
		t.Fatalf("нет INTERNAL в теле: %s", rec.Body.String())
	}
}

// Preflight OPTIONS → 204 без прохода к хендлерам.
func TestPreflight(t *testing.T) {
	r, _ := newTestRouter(t)
	rec := do(t, r, http.MethodOptions, "/auth/login", map[string]string{"Origin": "http://localhost:3000"}, "")

	if rec.Code != http.StatusNoContent {
		t.Fatalf("статус = %d, хочу 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, хочу *", got)
	}
}

// Несовпадающий метод → 404/405 Gin'а, но глобальные middleware всё равно
// отработали: заголовки CORS и request-id на месте.
func TestWrongMethodStillGlobals(t *testing.T) {
	r, _ := newTestRouter(t)
	rec := do(t, r, http.MethodGet, "/auth/login",
		map[string]string{"X-Request-ID": "rid-fixed-42", "Origin": "http://x.test"}, "")

	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatal("нет CORS-заголовков на 404")
	}
	if got := rec.Header().Get("X-Request-ID"); got != "rid-fixed-42" {
		t.Fatalf("X-Request-ID = %q, хочу переданный rid-fixed-42", got)
	}
}
