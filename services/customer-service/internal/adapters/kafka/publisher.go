package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// Envelope — единый формат событий (см. ТЗ).
type Envelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt string          `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

// TopicCustomer — топик событий customer-service (ТЗ §6).
const TopicCustomer = "customer.events"

// Publisher публикует события профиля.
// TODO(прод): outbox-паттерн (требование ТЗ для статусных событий).
type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(broker string) *Publisher {
	return &Publisher{writer: &kafkago.Writer{
		Addr:                   kafkago.TCP(broker),
		Topic:                  TopicCustomer,
		AllowAutoTopicCreation: true,
	}}
}

// ProfileFilled — клиент заполнил анкету. По этому событию фронт/клиент
// может подавать KYC-заявку (ТЗ 5.3). Key = customer_id: все события
// одного профиля идут в одну партицию по порядку.
func (p *Publisher) ProfileFilled(ctx context.Context, customerID, userID string) {
	payload, _ := json.Marshal(map[string]string{
		"customer_id": customerID,
		"user_id":     userID,
	})
	env := Envelope{
		EventID:    uuid.NewString(),
		EventType:  "profile_filled",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Payload:    payload,
	}
	body, _ := json.Marshal(env)
	if err := p.writer.WriteMessages(ctx, kafkago.Message{Key: []byte(customerID), Value: body}); err != nil {
		log.Printf("kafka publish profile_filled: %v", err) // не роняем сценарий из-за шины
	}
}

func (p *Publisher) Close() error { return p.writer.Close() }
