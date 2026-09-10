package httpapi

import (
	"context"
	"encoding/json"
	"mini-core/services/api-gateway/internal/verifier"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ctxKey int

type Claims = verifier.Claims

const (
	ctxKeyClaims ctxKey = iota
	ctxKeyRequestID
)

func withClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, ctxKeyClaims, c)
}

func claimsFromCtx(ctx context.Context) *Claims {
	c, _ := ctx.Value(ctxKeyClaims).(*Claims)
	return c
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

func requestIDFromCtx(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyRequestID).(string)
	return id
}

func writeJSON(w http.ResponseWriter, httpStatus int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(v)
}

// errBody — единый формат ошибок наружу (требование ТЗ).
type errBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Details   string `json:"details,omitempty"`
	RequestID string `json:"request_id"`
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, httpStatus int, code, msg string) {
	writeJSON(w, httpStatus, errBody{
		Code:      code,
		Message:   msg,
		RequestID: requestIDFromCtx(r.Context()),
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
func (s *Server) grpcError(w http.ResponseWriter, r *http.Request, err error) {
	st, ok := status.FromError(err)
	if !ok {
		s.writeError(w, r, http.StatusBadGateway, "UPSTREAM", "сервис недоступен")
		return
	}
	s.writeError(w, r, httpFromGRPC(st.Code()), st.Code().String(), st.Message())
}
