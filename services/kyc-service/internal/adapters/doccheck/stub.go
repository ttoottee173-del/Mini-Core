// Package doccheck — реализация порта app.Documents.
// ПОКА заглушка: document-service (№5 по ТЗ) ещё не написан, поэтому
// считаем, что обязательные документы (паспорт + селфи) загружены.
// Когда появится document-service, здесь появится gRPC-клиент метода
// HasRequiredDocuments — app и domain менять не придётся.
package doccheck

import (
	"context"
	"log"
)

// Stub всегда отвечает «документы на месте».
type Stub struct{}

func NewStub() *Stub { return &Stub{} }

func (s *Stub) HasRequired(_ context.Context, customerID string) error {
	log.Printf("doccheck: заглушка пропускает проверку документов для %s", customerID)
	return nil
}
