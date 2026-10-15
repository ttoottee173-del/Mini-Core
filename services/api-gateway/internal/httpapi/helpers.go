package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"mini-core/services/api-gateway/internal/verifier"
)

type Claims = verifier.Claims

// Ключи для c.Set/c.Get — значения, которые middleware оставляют в контексте
// запроса для последующих слоёв и хендлеров. Константы, а не сырые строки:
// опечатку в строке компилятор не заметит, опечатку в константе — сразу.
const (
	keyClaims    = "claims"
	keyRequestID = "request_id"
)

// claimsOf — claims, положенные authMW. На публичных роутах authMW не
// выполнялся, поэтому nil — штатная ситуация, а не ошибка.
func claimsOf(c *gin.Context) *Claims {
	v, ok := c.Get(keyClaims)
	if !ok {
		return nil
	}
	claims, _ := v.(*Claims)
	return claims
}

// requestIDOf — идентификатор, положенный requestIDMW.
func requestIDOf(c *gin.Context) string {
	v, ok := c.Get(keyRequestID)
	if !ok {
		return ""
	}
	id, _ := v.(string)
	return id
}

// errBody — единый формат ошибок наружу (требование ТЗ).
type errBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Details   string `json:"details,omitempty"`
	RequestID string `json:"request_id"`
}

// writeError — отправить errBody И прервать цепочку обработки.
// Почему Abort внутри: в Gin отличие middleware от хендлера условное, и
// забытый c.Abort() превращает 401 в «ответили ошибкой, но хендлер всё
// равно выполнился». Прерывание здесь делает это невозможным по построению:
// ответили ошибкой — цепочка остановлена. Для хендлеров Abort безвреден:
// после них идти дальше всё равно некому.
func (s *Server) writeError(c *gin.Context, httpStatus int, code, msg string) {
	c.AbortWithStatusJSON(httpStatus, errBody{
		Code:      code,
		Message:   msg,
		RequestID: requestIDOf(c),
	})
}

// httpFromGRPC — перевод кодов gRPC в HTTP-статусы.
func httpFromGRPC(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return 400
	case codes.Unauthenticated:
		return 401
	case codes.PermissionDenied:
		return 403
	case codes.NotFound:
		return 404
	case codes.AlreadyExists:
		return 409
	case codes.FailedPrecondition:
		return 412
	case codes.ResourceExhausted:
		return 429
	case codes.Unavailable:
		return 503
	default:
		return 500
	}
}

// grpcError — ошибка от gRPC-вызова → единый JSON.
func (s *Server) grpcError(c *gin.Context, err error) {
	st, ok := status.FromError(err)
	if !ok {
		s.writeError(c, http.StatusBadGateway, "UPSTREAM", "сервис недоступен")
		return
	}
	s.writeError(c, httpFromGRPC(st.Code()), st.Code().String(), st.Message())
}
