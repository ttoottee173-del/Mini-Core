package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"mini-core/services/customer-service/internal/app"
	"mini-core/services/customer-service/internal/domain"
)

// kycPayload — полезная нагрузка событий kyc-service.
// customer_id ищем в СВОЕЙ таблице customers (id = customer_id):
// чужую БД читать нельзя, но id клиента в событии — наши данные.
type kycPayload struct {
	ApplicationID string `json:"application_id"`
	CustomerID    string `json:"customer_id"`
	Reason        string `json:"reason"`
}

type Consumer struct {
	reader    *kafkago.Reader
	customers *app.CustomerService
}

func NewConsumer(brokers, group string, svc *app.CustomerService) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: []string{brokers},
			GroupID: group, // группа: офсеты хранятся на брокере
			// Два топика одной группой: registration от auth и
			// вердикты от kyc приходят в один обработчик.
			GroupTopics: []string{"auth.events", "kyc.events"},
		}),
		customers: svc,
	}
}

// Run блокируется до отмены ctx. Обязателен в отдельной горутине.
func (c *Consumer) Run(ctx context.Context) {
	for {
		m, err := c.reader.FetchMessage(ctx) // читаем БЕЗ коммита
		if err != nil {
			if ctx.Err() != nil {
				return // штатная остановка
			}
			log.Printf("kafka fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if err := c.handle(ctx, m.Value); err != nil {
			// В учебном проекте: логируем и коммитим, чтобы не зависнуть.
			// TODO(прод): retry с backoff + DLQ-топик (требование ТЗ к консьюмерам)
			log.Printf("handle event: %v", err)
		}
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("kafka commit: %v", err)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, data []byte) error {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return err // ядовитое сообщение
	}
	log.Printf("событие: %s (id=%s)", env.EventType, env.EventID)

	switch env.EventType {
	case "user_registered": // из auth.events
		var p struct {
			UserID string `json:"user_id"`
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return c.customers.EnsureProfile(ctx, p.UserID)
	case "application_submitted", "application_approved", "application_rejected": // из kyc.events
		var p kycPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return c.handleKycEvent(ctx, env.EventType, &p)
	default:
		return nil // не наше — пропускаем
	}
}

// handleKycEvent — статус клиента двигается вслед за заявкой
// (ТЗ 5.3: PROFILE_FILLED → ON_KYC → ACTIVE / REJECTED).
// AUTO_REJECTED публикуется как application_rejected — отдельного
// события для него в ТЗ нет, а клиент в обоих случаях отклонён.
func (c *Consumer) handleKycEvent(ctx context.Context, eventType string, p *kycPayload) error {
	var to domain.CustomerStatus
	switch eventType {
	case "application_submitted":
		to = domain.StatusOnKyc
	case "application_approved":
		to = domain.StatusActive
	default: // application_rejected
		to = domain.StatusRejected
	}
	return c.customers.SetStatusByCustomerID(ctx, p.CustomerID, to)
}

func (c *Consumer) Close() error { return c.reader.Close() }
