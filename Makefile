# Makefile — шпаргалка по частым командам.
# Запускать из корня репозитория: make <цель>

# --- Инфраструктура (Postgres + Kafka + Redis) ---

up:
	docker compose up -d

down:
	docker compose down

# --- Генерация кода ---
# proto-файлы теперь живут внутри каждого сервиса (services/*/proto),
# сгенерированный Go-код падает в services/*/gen.
# buf ищет свой конфиг (buf.yaml / buf.gen.yaml) в текущей папке,
# поэтому команды выполняются из services/*/proto.

proto-lint:
	cd services/auth-service/proto && buf lint
	cd services/customer-service/proto && buf lint
	cd services/kyc-service/proto && buf lint

proto-gen:
	cd services/auth-service/proto && buf generate
	cd services/customer-service/proto && buf generate
	cd services/kyc-service/proto && buf generate

# sqlc превращает SQL из services/*/queries в типизированный Go-код
# в services/*/internal/adapters/postgres/db.

sqlc-gen:
	cd services/auth-service && sqlc generate
	cd services/customer-service && sqlc generate
	cd services/kyc-service && sqlc generate

# Всё сразу: сгенерировать gRPC-код и код базы данных.
gen: proto-gen sqlc-gen

# --- Сборка и запуск ---

# go build ./... собирает все пакеты модуля; бинарник кладём в bin/
# (в git не попадает, см. .gitignore).

build:
	mkdir -p bin
	cd services/auth-service && go build -o ../../bin/auth-service ./cmd
	cd services/customer-service && go build -o ../../bin/customer-service ./cmd
	cd services/kyc-service && go build -o ../../bin/kyc-service ./cmd
	cd services/api-gateway && go build -o ../../bin/api-gateway ./cmd

test:
	cd services/auth-service && go test ./...
	cd services/customer-service && go test ./...
	cd services/kyc-service && go test ./...
	cd services/api-gateway && go test ./...

run-auth:
	cd services/auth-service && go run ./cmd

run-customer:
	cd services/customer-service && go run ./cmd

run-gateway:
	cd services/api-gateway && go run ./cmd

run-kyc:
	cd services/kyc-service && go run ./cmd

# --- Kafka: подсмотреть, что происходит в топике ---

kafka-topics:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-lag:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group customer-service

kafka-read:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic auth.events --from-beginning

kafka-read-kyc:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kyc.events --from-beginning
