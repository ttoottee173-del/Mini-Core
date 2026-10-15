// Package grpcapi реализует gRPC-контракт KycService (gen/bank/kyc/v1).
// Сервер — «переводчик»: proto-сообщения ↔ use cases, доменные ошибки —
// в gRPC-коды. Бизнес-логики здесь нет по определению.
package grpcapi

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kycv1 "mini-core/services/kyc-service/gen/bank/kyc/v1"
	"mini-core/services/kyc-service/internal/app"
	"mini-core/services/kyc-service/internal/domain"
)

// Server реализует контракт KycService.
type Server struct {
	kycv1.UnimplementedKycServiceServer
	svc *app.Kyc
}

func New(svc *app.Kyc) *Server {
	return &Server{svc: svc}
}

// mapErr — доменные ошибки → gRPC-коды (ТЗ §5: осмысленные коды).
func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, app.ErrCustomerNotFound):
		return status.Error(codes.NotFound, "application or customer not found")
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, "активная заявка уже существует")
	case errors.Is(err, domain.ErrAlreadyClaimed):
		return status.Error(codes.FailedPrecondition, "заявку уже забрал другой офицер")
	case errors.Is(err, domain.ErrNotReviewer):
		return status.Error(codes.FailedPrecondition, "заявку ревьюит другой офицер")
	case errors.Is(err, domain.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, "переход запрещён текущим статусом заявки")
	case errors.Is(err, domain.ErrProfileNotFilled):
		return status.Error(codes.FailedPrecondition, "профиль не заполнен: сначала анкета в customer-service")
	case errors.Is(err, domain.ErrMissingDocuments):
		return status.Error(codes.FailedPrecondition, "не загружены обязательные документы (паспорт + селфи)")
	case errors.Is(err, domain.ErrRejectReasonRequired):
		return status.Error(codes.InvalidArgument, "reason обязателен для отказа")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

// toProto — доменная модель → proto-сообщение.
func toProto(a *domain.Application) *kycv1.Application {
	return &kycv1.Application{
		ApplicationId: a.ID,
		CustomerId:    a.CustomerID,
		Status:        string(a.Status),
		ReviewerId:    a.ReviewerID,
		RejectReason:  a.RejectReason,
		CreatedAt:     a.CreatedAt,
		UpdatedAt:     a.UpdatedAt,
	}
}

// SubmitApplication — клиент подаёт заявку. Кто подаёт, определяем по
// user_id из запроса (gateway кладёт туда sub из JWT).
func (s *Server) SubmitApplication(ctx context.Context, req *kycv1.SubmitApplicationRequest) (*kycv1.Application, error) {
	a, err := s.svc.SubmitApplication(ctx, req.GetUserId())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(a), nil
}

func (s *Server) GetApplication(ctx context.Context, req *kycv1.GetApplicationRequest) (*kycv1.Application, error) {
	a, err := s.svc.Get(ctx, req.GetApplicationId())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(a), nil
}

func (s *Server) ListApplications(ctx context.Context, req *kycv1.ListApplicationsRequest) (*kycv1.ListApplicationsResponse, error) {
	items, nextOffset, err := s.svc.List(ctx, int(req.GetPageSize()), req.GetPageToken(), req.GetStatusFilter())
	if err != nil {
		return nil, mapErr(err)
	}
	resp := &kycv1.ListApplicationsResponse{NextPageToken: strconv.Itoa(nextOffset)}
	for i := range items {
		resp.Items = append(resp.Items, toProto(&items[i]))
	}
	return resp, nil
}

func (s *Server) ClaimApplication(ctx context.Context, req *kycv1.ClaimApplicationRequest) (*kycv1.Application, error) {
	a, err := s.svc.Claim(ctx, req.GetApplicationId(), req.GetOfficerId())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(a), nil
}

func (s *Server) ApproveApplication(ctx context.Context, req *kycv1.ApproveApplicationRequest) (*kycv1.Application, error) {
	a, err := s.svc.Approve(ctx, req.GetApplicationId(), req.GetOfficerId(), req.GetComment())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(a), nil
}

func (s *Server) RejectApplication(ctx context.Context, req *kycv1.RejectApplicationRequest) (*kycv1.Application, error) {
	a, err := s.svc.Reject(ctx, req.GetApplicationId(), req.GetOfficerId(), req.GetReason())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(a), nil
}

func (s *Server) RequestMoreDocuments(ctx context.Context, req *kycv1.RequestMoreDocumentsRequest) (*kycv1.Application, error) {
	a, err := s.svc.RequestMoreDocuments(ctx, req.GetApplicationId(), req.GetOfficerId(), req.GetComment())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(a), nil
}
