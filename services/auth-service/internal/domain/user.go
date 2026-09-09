// Package domain — сущности и правила предметной области auth-service.
// Здесь живёт «словарь» ошибок и инварианты (например, нормализация email).
// Пакет НЕ зависит ни от gRPC, ни от Postgres, ни от чего внешнего —
// это фундамент, на который смотрят все остальные слои.
package domain

import (
	"errors"
	"strings"
)

// Role — роль пользователя в системе.
// Базовый тип string, ограниченный константами.
type Role string

const (
	RoleClient  Role = "CLIENT"
	RoleOfficer Role = "OFFICER"
	RoleAdmin   Role = "ADMIN"
)

// Предопределённые ошибки, которые могут возвращать сценарии (use cases).
var (
	ErrInvalidInput       = errors.New("invalid input")
	ErrEmailTaken         = errors.New("email already taken")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrNotFound           = errors.New("not found")
)

// User — агрегат, представляющий пользователя.
// Все поля экспортируемые, так как они используются в других пакетах.
type User struct {
	ID           string
	Email        string
	PasswordHash string
	Role         Role
}

// NormalizeEmail приводит email к каноническому виду:
// удаляет пробелы по краям и приводит к нижнему регистру.
// Используется при создании пользователя и при поиске.
func NormalizeEmail(e string) string {
	return strings.ToLower(strings.TrimSpace(e))
}

// NewUser — фабрика для создания User.
// Выполняет валидацию email (должен содержать '@').
// При успехе возвращает указатель на User с ролью CLIENT по умолчанию.
// В случае ошибки возвращает nil и ErrInvalidInput.
func NewUser(email, passwordHash string) (*User, error) {
	email = NormalizeEmail(email)
	if !strings.Contains(email, "@") {
		return nil, ErrInvalidInput
	}
	return &User{Email: email, PasswordHash: passwordHash, Role: RoleClient}, nil
}
