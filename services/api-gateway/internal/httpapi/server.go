package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/metadata"

	"mini-core/services/api-gateway/internal/ratelimit"
	"mini-core/services/api-gateway/internal/verifier"
	authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
	customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
	kycv1 "mini-core/services/kyc-service/gen/bank/kyc/v1"
)

// Server — только перевод форматов. Бизнес-логики здесь нет (ТЗ запрещает).
type Server struct {
	log       *slog.Logger
	verifier  *verifier.Verifier
	limiter   *ratelimit.Limiter
	auth      authv1.AuthServiceClient
	customers customerv1.CustomerServiceClient
	kyc       kycv1.KycServiceClient
}

func New(log *slog.Logger, v *verifier.Verifier, l *ratelimit.Limiter,
	auth authv1.AuthServiceClient, customers customerv1.CustomerServiceClient,
	kyc kycv1.KycServiceClient) *Server {
	return &Server{log: log, verifier: v, limiter: l, auth: auth, customers: customers, kyc: kyc}
}

// Routes — вся маршрутизация. gin.New() — роутер без дефолтных middleware:
// свои recover/log уже дают и защиту от паник, и access-лог в общем
// формате ошибок проекта ( Gin'овские Logger/Recovery дублировали бы их).
// Возвращаем *gin.Engine — он реализует http.Handler, поэтому main
// оборачивает его в http.Server без изменений.
func (s *Server) Routes() *gin.Engine {
	r := gin.New()

	// общий конвейер вокруг ВСЕХ роутов; порядок = порядок прохождения запроса:
	// recover → requestID → log → cors → (группа) → хендлер
	r.Use(s.recoverMW(), s.requestIDMW(), s.logMW(), s.corsMW())

	// публичные: лимит по IP. Группа — способ навесить middleware пачкой
	// на несколько роутов (фигурные скобки — просто визуальная группировка).
	pub := r.Group("/auth", s.rateLimitMW())
	pub.POST("/register", s.register)
	pub.POST("/login", s.login)
	pub.POST("/refresh", s.refresh)

	// защищённые: сначала auth (claims в контекст), потом лимит по user_id
	me := r.Group("/customers", s.authMW(), s.rateLimitMW())
	me.GET("/me", s.getMe)
	me.PUT("/me", s.updateMe)

	// KYC: подача/просмотр заявки — клиенту; очередь и вердикты — OFFICER.
	kycGroup := r.Group("/kyc", s.authMW(), s.rateLimitMW())
	kycGroup.POST("/applications", s.submitKyc) // клиент подаёт заявку
	kycGroup.GET("/applications/:id", s.getKyc) // клиент смотрит свою

	officer := kycGroup.Group("", s.requireRole("OFFICER")) // проверка роли ДО хендлеров
	officer.GET("/applications", s.listKyc)                 // очередь
	officer.POST("/applications/:id/claim", s.claimKyc)
	officer.POST("/applications/:id/approve", s.approveKyc)
	officer.POST("/applications/:id/reject", s.rejectKyc)
	officer.POST("/applications/:id/request-docs", s.requestDocsKyc)

	return r
}

// outgoing — контекст для gRPC: прокидываем request-id и claims в metadata.
func (s *Server) outgoing(c *gin.Context) context.Context {
	pairs := []string{"x-request-id", requestIDOf(c)}
	if cl := claimsOf(c); cl != nil {
		pairs = append(pairs, "x-user-id", cl.UserID, "x-user-role", cl.Role)
	}
	return metadata.NewOutgoingContext(c.Request.Context(), metadata.Pairs(pairs...))
}

func (s *Server) register(c *gin.Context) {
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		s.writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
		return
	}
	resp, err := s.auth.Register(s.outgoing(c), &authv1.RegisterRequest{Email: in.Email, Password: in.Password})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (s *Server) login(c *gin.Context) {
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		s.writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
		return
	}
	resp, err := s.auth.Login(s.outgoing(c), &authv1.LoginRequest{Email: in.Email, Password: in.Password})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) refresh(c *gin.Context) {
	var in struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		s.writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
		return
	}
	resp, err := s.auth.Refresh(s.outgoing(c), &authv1.RefreshRequest{RefreshToken: in.RefreshToken})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) getMe(c *gin.Context) {
	cl := claimsOf(c) // user_id из ПРОВЕРЕННОГО токена, не из запроса!
	resp, err := s.customers.GetCustomer(s.outgoing(c), &customerv1.GetCustomerRequest{UserId: cl.UserID})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) updateMe(c *gin.Context) {
	var in struct {
		FirstName   string `json:"first_name"`
		LastName    string `json:"last_name"`
		Phone       string `json:"phone"`
		DateOfBirth string `json:"date_of_birth"`
		Address     string `json:"address"`
		Citizenship string `json:"citizenship"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		s.writeError(c, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
		return
	}
	cl := claimsOf(c)
	resp, err := s.customers.UpdateProfile(s.outgoing(c), &customerv1.UpdateProfileRequest{
		UserId:    cl.UserID,
		FirstName: in.FirstName, LastName: in.LastName, Phone: in.Phone,
		DateOfBirth: in.DateOfBirth, Address: in.Address, Citizenship: in.Citizenship,
	})
	if err != nil {
		s.grpcError(c, err)
		return
	}
	c.JSON(http.StatusOK, resp)
}
