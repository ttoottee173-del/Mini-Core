// Package jwks — HTTP-адаптер: отдаёт публичный ключ auth-service в формате JWKS
// (JSON Web Key Set — стандартный формат публикации ключей подписи).
// Gateway читает этот эндпоинт, чтобы проверять подпись access-токенов локально.
package jwks

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
)

// Handler собирает JWKS-документ из PEM-файла публичного ключа
// и возвращает http.HandlerFunc, который его отдаёт.
// Делаем это один раз при старте: ключ в процессе работы не меняется.
func Handler(publicPEM []byte) (http.HandlerFunc, error) {
	pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			// n и e — «сырьё» RSA-ключа (модуль и экспонента) в base64url.
			"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(bigIntBytes(pub.E)),
		}},
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}, nil
}

// bigIntBytes — экспонента (маленькое число) в big-endian байты.
// В JWKS экспонента передаётся именно как байты, поэтому конвертируем вручную.
func bigIntBytes(i int) []byte {
	b := make([]byte, 0)
	for i > 0 {
		b = append([]byte{byte(i & 0xff)}, b...)
		i >>= 8
	}
	return b
}
