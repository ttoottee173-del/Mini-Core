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

// Publisher — публикация событий в топик auth.events.
// TODO(прод): перевод на outbox-паттерн (требование ТЗ для статусных событий).
type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(broker string) *Publisher {
	return &Publisher{writer: &kafkago.Writer{
		Addr:                   kafkago.TCP(broker),
		Topic:                  "auth.events",
		AllowAutoTopicCreation: true,
	}}
}

func (p *Publisher) UserRegistered(ctx context.Context, userID, email string) {
	payload, _ := json.Marshal(map[string]string{"user_id": userID, "email": email})
	env := Envelope{
		EventID:    uuid.NewString(),
		EventType:  "user_registered",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Payload:    payload,
	}
	body, _ := json.Marshal(env)
	if err := p.writer.WriteMessages(ctx, kafkago.Message{Key: []byte(userID), Value: body}); err != nil {
		log.Printf("kafka publish: %v", err) // не роняем регистрацию из-за шины
	}
}

func (p *Publisher) Close() error { return p.writer.Close() }
