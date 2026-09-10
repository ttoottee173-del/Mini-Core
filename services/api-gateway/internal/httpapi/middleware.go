package httpapi

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Middleware — обёртка над хендлером.
type Middleware func(http.Handler) http.Handler

// Chain наворачивает в порядке перечисления: первый — самый внешний.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// recoverMW — паника в хендлере не роняет весь gateway (500 вместо смерти).
func (s *Server) recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic", "err", p, "path", r.URL.Path)
				s.writeError(w, r, http.StatusInternalServerError, "INTERNAL", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// requestIDMW — берём X-Request-ID или генерим; отдаём в заголовке ответа.
func (s *Server) requestIDMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", rid)
		next.ServeHTTP(w, r.WithContext(withRequestID(r.Context(), rid)))
	})
}

// statusRecorder — запоминает статус ответа для access-лога.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// logMW — access-лог: метод, путь, статус, длительность, request-id.
func (s *Server) logMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("access",
			"method", r.Method, "path", r.URL.Path,
			"status", rec.status, "dur", time.Since(start).Round(time.Microsecond),
			"request_id", requestIDFromCtx(r.Context()),
		)
	})
}

// corsMW — для разработки разрешаем всё; в проде так нельзя (долговая книжка).
func (s *Server) corsMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authMW — «Bearer <token>» → проверка подписи → claims в контекст.
func (s *Server) authMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, prefix) {
			s.writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "нужен Authorization: Bearer <token>")
			return
		}
		claims, err := s.verifier.Verify(strings.TrimPrefix(h, prefix))
		if err != nil {
			s.writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "токен невалиден или истёк")
			return
		}
		next.ServeHTTP(w, r.WithContext(withClaims(r.Context(), claims)))
	})
}

// rateLimitMW — лимит на user_id (если уже авторизован), иначе на IP.
func (s *Server) rateLimitMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, _, err := net.SplitHostPort(r.RemoteAddr) // "[::1]:54321" → "[::1]"
		if err != nil {
			key = r.RemoteAddr // на всякий случай
		}
		if c := claimsFromCtx(r.Context()); c != nil {
			key = c.UserID // авторизованных считаем по user_id, не по IP
		}
		ok, err := s.limiter.Allow(r.Context(), key)
		if err != nil {
			// fail-open: Redis упал — пропускаем, но орём в лог
			s.log.Error("rate limit недоступен, пропускаю (fail-open)", "err", err)
			next.ServeHTTP(w, r)
			return
		}
		if !ok {
			s.writeError(w, r, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "слишком много запросов, притормози")
			return
		}
		next.ServeHTTP(w, r)
	})
}
