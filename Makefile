up:
	docker compose up -d

down:
	docker compose down

proto-lint:
	cd proto && buf lint

proto-gen:
	cd proto && buf generate

kafka-topics:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-lag:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group customer-service

kafka-read:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic auth.events --from-beginning

run-auth:
	cd services/auth-service && go run ./cmd/auth-service

run-customer:
	cd services/customer-service && go run ./cmd/customer-service

run-gateway:
	cd services/api-gateway && go run ./cmd/api-gateway