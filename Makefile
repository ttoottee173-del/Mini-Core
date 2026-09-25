# Makefile — шпаргалка по частым командам.
# Запускать из корня репозитория: make <цель>

# --- Инфраструктура (Postgres + Kafka + Redis) ---

up:
	docker compose up -d

down:
	docker compose down

# --- Генерация кода ---
# proto-файлы живут внутри каждого сервиса (services/*/proto),
# сгенерированный Go-код падает в services/*/gen.
# buf ищет свой конфиг (buf.yaml / buf.gen.yaml) в текущей папке,
# поэтому команды выполняются из services/*/proto.

proto-lint:
	cd services/auth-service/proto && buf lint
	cd services/customer-service/proto && buf lint

proto-gen:
	cd services/auth-service/proto && buf generate
	cd services/customer-service/proto && buf generate

# sqlc превращает SQL из services/*/queries в типизированный Go-код
# в services/*/internal/adapters/postgres/db.

sqlc-gen:
	cd services/auth-service && sqlc generate
	cd services/customer-service && sqlc generate

# Всё сразу: сгенерировать gRPC-код и код базы данных.
gen: proto-gen sqlc-gen

# --- Зависимости ---
# Прогнать go mod tidy во ВСЕХ модулях workspace.
# Нужен после добавления новых импортов в коде.
# ВАЖНО: рецепты только с ТАБАМИ, не с пробелами!

tidy:
	cd services/auth-service && go mod tidy
	cd services/customer-service && go mod tidy
	cd services/api-gateway && go mod tidy
	@echo "все модули tidied ✓"

deps: tidy

# --- Миграции (goose) ---

migrate-auth:
	goose -dir services/auth-service/migrations postgres "postgres://dev:dev@localhost:5432/auth_db?sslmode=disable" up

migrate-customer:
	goose -dir services/customer-service/migrations postgres "postgres://dev:dev@localhost:5432/customer_db?sslmode=disable" up

migrate: migrate-auth migrate-customer

# --- Сборка и запуск ---

build:
	mkdir -p bin
	cd services/auth-service && go build -o ../../bin/auth-service ./cmd
	cd services/customer-service && go build -o ../../bin/customer-service ./cmd
	cd services/api-gateway && go build -o ../../bin/api-gateway ./cmd

test:
	cd services/auth-service && go test ./...
	cd services/customer-service && go test ./...
	cd services/api-gateway && go vet ./...

run-auth:
	cd services/auth-service && go run ./cmd

run-customer:
	cd services/customer-service && go run ./cmd

run-gateway:
	cd services/api-gateway && go run ./cmd

# --- Kafka: подсмотреть, что происходит в топике ---

kafka-topics:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-lag:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group customer-service

kafka-read:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic auth.events --from-beginning

# --- Диагностика ---

# Быстрый smoke-тест: уникальный email, можно дёргать сколько угодно
smoke:
	curl -s -X POST localhost:8080/auth/register -H 'Content-Type: application/json' -d '{"email":"smoke-$$(date +%s)@example.com","password":"secret123"}'

.PHONY: up down proto-lint proto-gen sqlc-gen gen tidy deps migrate-auth migrate-customer migrate build test run-auth run-customer run-gateway kafka-topics kafka-lag kafka-read smoke