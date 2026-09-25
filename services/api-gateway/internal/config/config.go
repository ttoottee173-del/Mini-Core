// Package config — единственное место, где api-gateway «знает про окружение».
// Все настройки собираются в одну структуру Config один раз при старте
// (функция Load), а дальше весь код работает только с этой структурой.
// Источник настроек — переменные окружения; если переменная не задана,
// используется значение по умолчанию, чтобы проект поднимался без конфигов.
package config

import (
        "os"
        "strconv"
)

// Config — все настройки gateway в одном месте.
// Структура заполняется один раз при старте и дальше только читается.
type Config struct {
        // HTTPAddr — адрес, который слушает HTTP-сервер (REST наружу).
        HTTPAddr string
        // AuthAddr — адрес gRPC-сервера auth-service.
        AuthAddr string
        // CustomerAddr — адрес gRPC-сервера customer-service.
        CustomerAddr string
        // RedisAddr — адрес Redis (нужен rate limit'у).
        RedisAddr string
        // JWTPublicKeyPath — путь к публичному ключу auth-service (PEM-файл).
        JWTPublicKeyPath string
        // RateLimitCapacity — «размер ведра»: сколько запросов можно сделать подряд.
        RateLimitCapacity int
        // RateLimitRefillPerMin — сколько токенов возвращается в ведро за минуту.
        RateLimitRefillPerMin float64
}

// Load читает переменные окружения и возвращает готовый Config.
func Load() Config {
        return Config{
                HTTPAddr:              envOr("GATEWAY_ADDR", ":8080"),
                AuthAddr:              envOr("AUTH_ADDR", "localhost:50051"),
                CustomerAddr:          envOr("CUSTOMER_ADDR", "localhost:50052"),
                RedisAddr:             envOr("REDIS_ADDR", "localhost:6379"),
                JWTPublicKeyPath:      envOr("JWT_PUBLIC_KEY_PATH", "../auth-service/keys/public.pem"),
                RateLimitCapacity:     envInt("RL_CAPACITY", 10),
                RateLimitRefillPerMin: envFloat("RL_REFILL_PER_MIN", 5),
        }
}

// envOr возвращает значение переменной окружения или значение по умолчанию.
func envOr(key, def string) string {
        if v := os.Getenv(key); v != "" {
                return v
        }
        return def
}

// envInt — то же, но с разбором целого числа (настройки-лимиты).
func envInt(key string, def int) int {
        if v := os.Getenv(key); v != "" {
                if n, err := strconv.Atoi(v); err == nil {
                        return n
                }
        }
        return def
}

// envFloat — то же, но с разбором дробного числа (скорость пополнения).
func envFloat(key string, def float64) float64 {
        if v := os.Getenv(key); v != "" {
                if f, err := strconv.ParseFloat(v, 64); err == nil {
                        return f
                }
        }
        return def
}
