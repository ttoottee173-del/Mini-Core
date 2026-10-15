// Package config — единственное место, где kyc-service «знает про окружение».
// Все настройки собираются в структуру Config один раз при старте (Load),
// дальше main и остальной код читают только её. Источник — переменные
// окружения; у каждой есть дефолт для локальной разработки.
package config

import "os"

// Config — все настройки kyc-service в одном месте.
type Config struct {
	// GRPCAddr — адрес gRPC-сервера (основной API сервиса).
	GRPCAddr string
	// DatabaseURL — строка подключения к СВОЕЙ базе (database per service).
	DatabaseURL string
	// KafkaBrokers — адреса брокеров Kafka через запятую.
	KafkaBrokers string
	// CustomerAddr — адрес gRPC customer-service (проверка профиля).
	CustomerAddr string
	// ScanEvery — период опроса очереди авто-чеков воркером.
	ScanEvery string // длительность Go (например, "2s"), парсится в main
}

// Load читает переменные окружения и возвращает готовый Config.
func Load() Config {
	return Config{
		GRPCAddr:     envOr("KYC_GRPC_ADDR", ":50053"),
		DatabaseURL:  envOr("DATABASE_URL", "postgres://dev:dev@localhost:5432/kyc_db?sslmode=disable"),
		KafkaBrokers: envOr("KAFKA_BROKERS", "localhost:9092"),
		CustomerAddr: envOr("CUSTOMER_GRPC_ADDR", "localhost:50052"),
		ScanEvery:    envOr("KYC_SCAN_EVERY", "2s"),
	}
}

// envOr возвращает значение переменной окружения или значение по умолчанию.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
