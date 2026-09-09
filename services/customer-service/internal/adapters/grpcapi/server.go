package grpcapi

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	customerv1 "mini-core/pkg/api/bank/customer/v1"
	"mini-core/services/customer-service/internal/app"
	"mini-core/services/customer-service/internal/domain"
)

// Server реализует контракт CustomerService. Переводчик proto ↔ use case.
type Server struct {
	customerv1.UnimplementedCustomerServiceServer
	svc *app.CustomerService
}

func New(svc *app.CustomerService) *Server {
	return &Server{svc: svc}
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "customer not found")
	case errors.Is(err, domain.ErrProfileLocked):
		return status.Error(codes.FailedPrecondition, "нельзя менять профиль после подачи на KYC")
	case err != nil:
		return status.Error(codes.Internal, "internal error")
	}
	return nil
}

func toProto(c *domain.Customer) *customerv1.Customer {
	return &customerv1.Customer{
		CustomerId:  c.ID,
		UserId:      c.UserID,
		FirstName:   c.FirstName,
		LastName:    c.LastName,
		Phone:       c.Phone,
		DateOfBirth: c.DateOfBirth,
		Address:     c.Address,
		Citizenship: c.Citizenship,
		Status:      string(c.Status),
	}
}

func (s *Server) GetCustomer(ctx context.Context, req *customerv1.GetCustomerRequest) (*customerv1.Customer, error) {
	c, err := s.svc.Get(ctx, req.GetUserId())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(c), nil
}

func (s *Server) UpdateProfile(ctx context.Context, req *customerv1.UpdateProfileRequest) (*customerv1.Customer, error) {
	c, err := s.svc.UpdateProfile(ctx, req.GetUserId(),
		req.GetFirstName(), req.GetLastName(), req.GetPhone(),
		req.GetDateOfBirth(), req.GetAddress(), req.GetCitizenship())
	if err != nil {
		return nil, mapErr(err)
	}
	return toProto(c), nil
}

func (s *Server) GetCustomerStatus(ctx context.Context, req *customerv1.GetCustomerStatusRequest) (*customerv1.GetCustomerStatusResponse, error) {
	c, err := s.svc.Get(ctx, req.GetUserId())
	if err != nil {
		return nil, mapErr(err)
	}
	return &customerv1.GetCustomerStatusResponse{UserId: c.UserID, Status: string(c.Status)}, nil
}

func (s *Server) ListCustomers(ctx context.Context, req *customerv1.ListCustomersRequest) (*customerv1.ListCustomersResponse, error) {
	items, nextOffset, err := s.svc.List(ctx, int(req.GetPageSize()), req.GetPageToken(), req.GetStatusFilter())
	if err != nil {
		return nil, mapErr(err)
	}
	resp := &customerv1.ListCustomersResponse{NextPageToken: strconv.Itoa(nextOffset)}
	for i := range items {
		resp.Items = append(resp.Items, toProto(&items[i]))
	}
	return resp, nil
}
