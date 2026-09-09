package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
)

// jwksHandler отдаёт публичный ключ в формате JWKS — его читает gateway.
func jwksHandler(publicPEM []byte) (http.HandlerFunc, error) {
	pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(bigIntBytes(pub.E)),
		}},
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}, nil
}

// bigIntBytes — экспонента (маленькое число) в big-endian байты.
func bigIntBytes(i int) []byte {
	b := make([]byte, 0)
	for i > 0 {
		b = append([]byte{byte(i & 0xff)}, b...)
		i >>= 8
	}
	return b
}
