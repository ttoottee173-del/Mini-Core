package domain

import "time"

// RefreshToken — refresh-токен в виде, как он лежит в БД (только хеш).
type RefreshToken struct {
	Hash      string
	UserID    string
	ExpiresAt time.Time
	RevokedAt *time.Time // nil = активен
}

// Active — доменное правило: токен жив, пока не отозван и не истёк.
// Проверяемо юнит-тестом без БД — в этом и смысл слоя.
func (t *RefreshToken) Active(now time.Time) bool {
	return t.RevokedAt == nil && now.Before(t.ExpiresAt)
}
