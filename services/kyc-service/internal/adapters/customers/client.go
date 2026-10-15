// Package customers — gRPC-клиент customer-service, реализация порта
// app.Customers. KYC по ТЗ 5.4 обязан перед подачей заявки проверить,
// что профиль клиента заполнен; здесь же берём ФИО и дату рождения
// для авто-чеков (снимок сохраняется в самой заявке).
package customers

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
	"mini-core/services/kyc-service/internal/app"
)

// Client — порт app.Customers поверх gRPC.
type Client struct {
	stub customerv1.CustomerServiceClient
}

// Dial подключается к customer-service (insecure — только локальная разработка).
func Dial(ctx context.Context, addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	return &Client{stub: customerv1.NewCustomerServiceClient(conn)}, nil
}

// GetByUser возвращает профиль по user_id владельца.
// Ошибки переводим в ошибки домена: NOT_FOUND от customer-service — это
// ErrCustomerNotFound, всё остальное уходит наверх как есть.
func (c *Client) GetByUser(ctx context.Context, userID string) (*app.Applicant, error) {
	resp, err := c.stub.GetCustomer(ctx, &customerv1.GetCustomerRequest{UserId: userID})
	if err != nil {
		// status.Code достаёт gRPC-код из ошибки — каноничный способ.
		if status.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("%w: %v", app.ErrCustomerNotFound, err)
		}
		return nil, fmt.Errorf("customer-service: %w", err)
	}
	return &app.Applicant{
		CustomerID:  resp.GetCustomerId(),
		UserID:      resp.GetUserId(),
		Status:      resp.GetStatus(),
		FullName:    strings.TrimSpace(resp.GetFirstName() + " " + resp.GetLastName()),
		DateOfBirth: resp.GetDateOfBirth(),
	}, nil
}
