// Package config — единственное место, где auth-service «знает про окружение».
// Все настройки собираются в структуру Config один раз при старте (функция Load),
// дальше main и остальной код читают только её. Источник — переменные окружения;
// для каждой есть разумное значение по умолчанию, чтобы сервис поднимался локально
// без единой переменной окружения.
package config

import "os"

// Config — все настройки auth-service в одном месте.
type Config struct {
        // GRPCAddr — адрес gRPC-сервера (основной API сервиса).
        GRPCAddr string
        // JWKSAddr — адрес HTTP-сервера с публичным ключом (эндпоинт /jwks).
        JWKSAddr string
        // DatabaseURL — строка подключения к своей БД (database per service).
        DatabaseURL string
        // KafkaBrokers — адреса брокеров Kafka через запятую.
        KafkaBrokers string
        // PrivateKeyPath — PEM-файл приватного RSA-ключа (подпись access-токенов).
        PrivateKeyPath string
        // PublicKeyPath — PEM-файл публичного RSA-ключа (для JWKS-эндпоинта).
        PublicKeyPath string
}

// Load читает переменные окружения и возвращает готовый Config.
func Load() Config {
        return Config{
                GRPCAddr:       envOr("AUTH_GRPC_ADDR", ":50051"),
                JWKSAddr:       envOr("AUTH_JWKS_ADDR", ":8081"),
                DatabaseURL:    envOr("DATABASE_URL", "postgres://dev:dev@localhost:5432/auth_db?sslmode=disable"),
                KafkaBrokers:   envOr("KAFKA_BROKERS", "localhost:9092"),
                PrivateKeyPath: envOr("JWT_PRIVATE_KEY_PATH", "keys/private.pem"),
                PublicKeyPath:  envOr("JWT_PUBLIC_KEY_PATH", "keys/public.pem"),
        }
}

// envOr возвращает значение переменной окружения или значение по умолчанию.
func envOr(key, def string) string {
        if v := os.Getenv(key); v != "" {
                return v
        }
        return def
}
