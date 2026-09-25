package kafka

import (
        "context"
        "encoding/json"
        "log"
        "time"

        kafkago "github.com/segmentio/kafka-go"

        "mini-core/services/customer-service/internal/app"
)

// Envelope — формат событий из ТЗ.
type Envelope struct {
        EventID    string          `json:"event_id"`
        EventType  string          `json:"event_type"`
        OccurredAt string          `json:"occurred_at"`
        Payload    json.RawMessage `json:"payload"`
}

type userRegisteredPayload struct {
        UserID string `json:"user_id"`
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
                        Topic:   "auth.events",
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
        if env.EventType != "user_registered" {
                return nil // не наше — пропускаем
        }
        var p userRegisteredPayload
        if err := json.Unmarshal(env.Payload, &p); err != nil {
                return err
        }
        return c.customers.EnsureProfile(ctx, p.UserID)
}

func (c *Consumer) Close() error { return c.reader.Close() }
