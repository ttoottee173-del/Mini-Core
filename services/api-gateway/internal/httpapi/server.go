package httpapi

import (
        "context"
        "encoding/json"
        "log/slog"
        "net/http"

        "google.golang.org/grpc/metadata"

        authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
        customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
        "mini-core/services/api-gateway/internal/ratelimit"
        "mini-core/services/api-gateway/internal/verifier"
)

// Server — только перевод форматов. Бизнес-логики здесь нет (ТЗ запрещает).
type Server struct {
        log       *slog.Logger
        verifier  *verifier.Verifier
        limiter   *ratelimit.Limiter
        auth      authv1.AuthServiceClient
        customers customerv1.CustomerServiceClient
}

func New(log *slog.Logger, v *verifier.Verifier, l *ratelimit.Limiter,
        auth authv1.AuthServiceClient, customers customerv1.CustomerServiceClient) *Server {
        return &Server{log: log, verifier: v, limiter: l, auth: auth, customers: customers}
}

// Routes — вся маршрутизация. Go 1.22+ умеет паттерны «МЕТОД /путь» в штатном mux.
func (s *Server) Routes() http.Handler {
        mux := http.NewServeMux()

        // публичные: лимит по IP
        mux.Handle("POST /auth/register", Chain(http.HandlerFunc(s.register), s.rateLimitMW))
        mux.Handle("POST /auth/login", Chain(http.HandlerFunc(s.login), s.rateLimitMW))
        mux.Handle("POST /auth/refresh", Chain(http.HandlerFunc(s.refresh), s.rateLimitMW))

        // защищённые: сначала auth (claims в контекст), потом лимит по user_id
        mux.Handle("GET /customers/me", Chain(http.HandlerFunc(s.getMe), s.authMW, s.rateLimitMW))
        mux.Handle("PUT /customers/me", Chain(http.HandlerFunc(s.updateMe), s.authMW, s.rateLimitMW))

        // общий конвейер вокруг всего
        return Chain(mux, s.recoverMW, s.requestIDMW, s.logMW, s.corsMW)
}

// outgoing — контекст для gRPC: прокидываем request-id и claims в metadata.
func (s *Server) outgoing(r *http.Request) context.Context {
        pairs := []string{"x-request-id", requestIDFromCtx(r.Context())}
        if c := claimsFromCtx(r.Context()); c != nil {
                pairs = append(pairs, "x-user-id", c.UserID, "x-user-role", c.Role)
        }
        return metadata.NewOutgoingContext(r.Context(), metadata.Pairs(pairs...))
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
        var in struct {
                Email    string `json:"email"`
                Password string `json:"password"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        resp, err := s.auth.Register(s.outgoing(r), &authv1.RegisterRequest{Email: in.Email, Password: in.Password})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
        var in struct {
                Email    string `json:"email"`
                Password string `json:"password"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        resp, err := s.auth.Login(s.outgoing(r), &authv1.LoginRequest{Email: in.Email, Password: in.Password})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
        var in struct {
                RefreshToken string `json:"refresh_token"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        resp, err := s.auth.Refresh(s.outgoing(r), &authv1.RefreshRequest{RefreshToken: in.RefreshToken})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getMe(w http.ResponseWriter, r *http.Request) {
        c := claimsFromCtx(r.Context()) // user_id из ПРОВЕРЕННОГО токена, не из запроса!
        resp, err := s.customers.GetCustomer(s.outgoing(r), &customerv1.GetCustomerRequest{UserId: c.UserID})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}

func (s *Server) updateMe(w http.ResponseWriter, r *http.Request) {
        var in struct {
                FirstName   string `json:"first_name"`
                LastName    string `json:"last_name"`
                Phone       string `json:"phone"`
                DateOfBirth string `json:"date_of_birth"`
                Address     string `json:"address"`
                Citizenship string `json:"citizenship"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        c := claimsFromCtx(r.Context())
        resp, err := s.customers.UpdateProfile(s.outgoing(r), &customerv1.UpdateProfileRequest{
                UserId:    c.UserID,
                FirstName: in.FirstName, LastName: in.LastName, Phone: in.Phone,
                DateOfBirth: in.DateOfBirth, Address: in.Address, Citizenship: in.Citizenship,
        })
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}
