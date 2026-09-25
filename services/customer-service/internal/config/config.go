// Package config — единственное место, где customer-service «знает про окружение».
// Все настройки собираются в структуру Config один раз при старте (функция Load),
// дальше main и остальной код читают только её. Источник — переменные окружения;
// для каждой есть значение по умолчанию для локальной разработки.
package config

import "os"

// Config — все настройки customer-service в одном месте.
type Config struct {
	// GRPCAddr — адрес gRPC-сервера (основной API сервиса).
	GRPCAddr string
	// DatabaseURL — строка подключения к своей БД (database per service).
	DatabaseURL string
	// KafkaBrokers — адреса брокеров Kafka через запятую.
	KafkaBrokers string
	// ConsumerGroup — имя consumer-группы Kafka (по нему хранятся офсеты).
	ConsumerGroup string
}

// Load читает переменные окружения и возвращает готовый Config.
func Load() Config {
	return Config{
		GRPCAddr:      envOr("CUSTOMER_GRPC_ADDR", ":50052"),
		DatabaseURL:   envOr("DATABASE_URL", "postgres://dev:dev@localhost:5432/customer_db?sslmode=disable"),
		KafkaBrokers:  envOr("KAFKA_BROKERS", "localhost:9092"),
		ConsumerGroup: envOr("KAFKA_GROUP", "customer-service"),
	}
}

// envOr возвращает значение переменной окружения или значение по умолчанию.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
