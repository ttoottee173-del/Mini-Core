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
	ErrInvalidInput = errors.New("invalid input")
	ErrEmailTaken   = errors.New("email already taken")
)

type User struct {
	ID           string
	Email        string
	PasswordHash string
	Role         Role
}

func NewUser(email, passwordHash string) (*User, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if !strings.Contains(email, "@") {
		return nil, ErrInvalidInput
	}
	return &User{Email: email, PasswordHash: passwordHash, Role: RoleClient}, nil
}
