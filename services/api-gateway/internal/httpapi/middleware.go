package httpapi

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// В Gin middleware — это обычный gin.HandlerFunc (та же сигнатура, что и у
// хендлера), который в конце зовёт c.Next() — «передай управление дальше».
// Каждый конструктор ниже — фабрика: настраивает прослойку и возвращает её.
// Порядок прослоек задаётся в Routes(), здесь только их механика.

// recoverMW — паника в хендлере не роняет весь gateway (500 вместо смерти).
func (s *Server) recoverMW() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic", "err", p, "path", c.Request.URL.Path)
				s.writeError(c, http.StatusInternalServerError, "INTERNAL", "internal error")
			}
		}()
		c.Next()
	}
}

// requestIDMW — берём X-Request-ID или генерим; отдаём в заголовке ответа.
func (s *Server) requestIDMW() gin.HandlerFunc {
	return func(c *gin.Context) {
		rid := c.GetHeader("X-Request-ID")
		if rid == "" {
			rid = uuid.NewString()
		}
		c.Header("X-Request-ID", rid)
		c.Set(keyRequestID, rid)
		c.Next()
	}
}

// logMW — access-лог: метод, путь, статус, длительность, request-id.
// В Gin итоговый статус уже доступен через c.Writer.Status(), поэтому
// отдельный recorder-тип (как это делается на голом net/http) не нужен.
func (s *Server) logMW() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next() // сначала запрос идёт дальше; лог пишем ПОСЛЕ — статус уже известен
		s.log.Info("access",
			"method", c.Request.Method, "path", c.Request.URL.Path,
			"status", c.Writer.Status(), "dur", time.Since(start).Round(time.Microsecond),
			"request_id", requestIDOf(c),
		)
	}
}

// corsMW — для разработки разрешаем всё; в проде так нельзя (долговая книжка).
func (s *Server) corsMW() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.Writer.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
		if c.Request.Method == http.MethodOptions {
			// preflight: ответить 204 с заголовками и НЕ пускать запрос дальше
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// authMW — «Bearer <token>» → проверка подписи → claims в контекст Gin.
// writeError уже прерывает цепочку (Abort внутри), поэтому после него
// достаточно простого return.
func (s *Server) authMW() gin.HandlerFunc {
	return func(c *gin.Context) {
		const prefix = "Bearer "
		h := c.GetHeader("Authorization")
		if !strings.HasPrefix(h, prefix) {
			s.writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "нужен Authorization: Bearer <token>")
			return
		}
		claims, err := s.verifier.Verify(strings.TrimPrefix(h, prefix))
		if err != nil {
			s.writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "токен невалиден или истёк")
			return
		}
		c.Set(keyClaims, claims) // дальше по цепочке их достанет claimsOf
		c.Next()
	}
}

// rateLimitMW — лимит на user_id (если уже авторизован), иначе на IP.
func (s *Server) rateLimitMW() gin.HandlerFunc {
	return func(c *gin.Context) {
		key, _, err := net.SplitHostPort(c.Request.RemoteAddr) // "[::1]:54321" → "[::1]"
		if err != nil {
			key = c.Request.RemoteAddr // на всякий случай
		}
		if cl := claimsOf(c); cl != nil {
			key = cl.UserID // авторизованных считаем по user_id, не по IP
		}
		ok, err := s.limiter.Allow(c.Request.Context(), key)
		if err != nil {
			// fail-open: Redis упал — пропускаем, но орём в лог
			s.log.Error("rate limit недоступен, пропускаю (fail-open)", "err", err)
			c.Next()
			return
		}
		if !ok {
			s.writeError(c, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "слишком много запросов, притормози")
			return
		}
		c.Next()
	}
}

// requireRole — гейт по роли из JWT (role кладёт auth-service при выпуске
// токена). Навешивается на офицерские роуты KYC: у CLIENT доступ к
// очереди заявок и вердиктам закрыт ещё до входа в хендлер.
func (s *Server) requireRole(role string) gin.HandlerFunc {
	return func(c *gin.Context) {
		cl := claimsOf(c) // authMW уже отработал и положил claims
		if cl == nil || cl.Role != role {
			s.writeError(c, http.StatusForbidden, "PERMISSION_DENIED", "требуется роль "+role)
			return
		}
		c.Next()
	}
}
