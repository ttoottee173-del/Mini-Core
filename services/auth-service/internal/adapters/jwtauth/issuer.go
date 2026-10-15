// Package jwtauth предоставляет реализацию порта TokenIssuer,
// используя JWT с подписью RS256 (RSA).
// Содержит структуру Issuer, которая умеет выпускать access-токены
// с claims: sub (user ID), role, iat, exp.
package jwtauth

import (
	"crypto/rsa"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Issuer — реализация TokenIssuer для выпуска JWT.
// Хранит приватный RSA-ключ для подписи токенов.
type Issuer struct {
	key *rsa.PrivateKey
}

// NewIssuer создаёт новый Issuer из PEM-закодированного приватного ключа RSA.
// Ожидается блок "RSA PRIVATE KEY". Возвращает ошибку, если ключ невалидный.
func NewIssuer(privateKeyPEM []byte) (*Issuer, error) {
	key, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	return &Issuer{key: key}, nil
}

// IssueAccess генерирует JWT-токен с подписью RS256.
// Устанавливает claims:
//   - sub  — userID
//   - role — роль пользователя (для RBAC)
//   - iat  — время выпуска (Unix)
//   - exp  — время истечения (Unix) = now + ttl.
//
// Возвращает строку токена или ошибку.
func (i *Issuer) IssueAccess(userID, role string, ttl time.Duration) (string, error) {
	now := time.Now().UTC()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub":  userID,
		"role": role,
		"iat":  now.Unix(),
		"exp":  now.Add(ttl).Unix(),
	})
	return tok.SignedString(i.key)
}
