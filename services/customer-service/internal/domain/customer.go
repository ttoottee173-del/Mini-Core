package domain

import (
	"errors"
	"time"
)

type CustomerStatus string

const (
	StatusNew           CustomerStatus = "NEW"
	StatusProfileFilled CustomerStatus = "PROFILE_FILLED"
	StatusOnKyc         CustomerStatus = "ON_KYC"
	StatusActive        CustomerStatus = "ACTIVE"
	StatusRejected      CustomerStatus = "REJECTED"
	StatusBlocked       CustomerStatus = "BLOCKED"
)

var ErrNotFound = errors.New("customer not found")
var ErrProfileLocked = errors.New("profile locked: KYC started")

// allowedTransitions — таблица переходов статусной машины.
var allowedTransitions = map[CustomerStatus][]CustomerStatus{
	StatusNew:           {StatusProfileFilled, StatusBlocked},
	StatusProfileFilled: {StatusOnKyc, StatusBlocked},
	StatusOnKyc:         {StatusActive, StatusRejected, StatusBlocked},
	StatusActive:        {StatusBlocked},
	StatusRejected:      {},
	StatusBlocked:       {},
}

// CanTransition — можно ли перейти из одного статуса в другой.
func CanTransition(from, to CustomerStatus) bool {
	for _, s := range allowedTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Customer — профиль клиента.
type Customer struct {
	ID          string
	UserID      string
	FirstName   string
	LastName    string
	Phone       string
	DateOfBirth string // YYYY-MM-DD
	Address     string
	Citizenship string
	Status      CustomerStatus
}

// UpdateProfile — доменное правило: заполнять можно только до ON_KYC.
func (c *Customer) UpdateProfile(first, last, phone, dob, addr, citizenship string) error {
	if c.Status != StatusNew && c.Status != StatusProfileFilled {
		return ErrProfileLocked
	}
	c.FirstName, c.LastName, c.Phone = first, last, phone
	c.DateOfBirth, c.Address, c.Citizenship = dob, addr, citizenship
	if c.Status == StatusNew {
		if !CanTransition(c.Status, StatusProfileFilled) {
			return errors.New("bad transition") // защита на будущее
		}
		c.Status = StatusProfileFilled
	}
	return nil
}

// BirthDate — распарсенная дата рождения (для будущих проверок KYC).
func (c *Customer) BirthDate() (time.Time, error) {
	return time.Parse("2006-01-02", c.DateOfBirth)
}
