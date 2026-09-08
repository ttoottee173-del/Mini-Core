package domain

import (
	"errors"
	"strings"
)

type Role string

const (
	RoleClient  Role = "CLIENT"
	RoleOfficer Role = "OFFICER"
	RoleAdmin   Role = "ADMIN"
)

var (
	ErrInvalidInput       = errors.New("invalid input")
	ErrEmailTaken         = errors.New("email already taken")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrNotFound           = errors.New("not found")
)

type User struct {
	ID           string
	Email        string
	PasswordHash string
	Role         Role
}

func NormalizeEmail(e string) string {
	return strings.ToLower(strings.TrimSpace(e))
}

func NewUser(email, passwordHash string) (*User, error) {
	email = NormalizeEmail(email)
	if !strings.Contains(email, "@") {
		return nil, ErrInvalidInput
	}
	return &User{Email: email, PasswordHash: passwordHash, Role: RoleClient}, nil
}
