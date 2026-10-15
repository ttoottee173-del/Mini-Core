package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// Envelope — единый формат событий (ТЗ §6): конверт с event_id для
// дедупликации у консьюмеров. Тот же JSON, что в auth/customer.
type Envelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt string          `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

// TopicKyc — топик событий kyc-service (ТЗ §6).
const TopicKyc = "kyc.events"

// Publisher публикует события жизненного цикла заявки.
// TODO(прод): outbox-паттерн (ТЗ требует outbox для статусных событий).
type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(broker string) *Publisher {
	return &Publisher{writer: &kafkago.Writer{
		Addr:                   kafkago.TCP(broker),
		Topic:                  TopicKyc,
		AllowAutoTopicCreation: true, // учебный проект: топик создаётся сам
	}}
}

// Publish отправляет событие о заявке. Key = application_id: события одной
// заявки попадают в одну партицию и читаются по порядку.
// Ошибка не возвращается: падение шины не должно останавливать сценарий
// (в учебном проекте логируем; в проде — outbox).
func (p *Publisher) Publish(ctx context.Context, eventType, applicationID, customerID, reason string) {
	payload, _ := json.Marshal(map[string]string{
		"application_id": applicationID,
		"customer_id":    customerID,
		"reason":         reason,
	})
	env := Envelope{
		EventID:    uuid.NewString(),
		EventType:  eventType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Payload:    payload,
	}
	body, _ := json.Marshal(env)
	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(applicationID),
		Value: body,
	}); err != nil {
		log.Printf("kafka publish %s: %v", eventType, err)
	}
}

func (p *Publisher) Close() error { return p.writer.Close() }
