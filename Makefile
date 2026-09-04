up:
    docker compose up -d

down:
    docker compose down

proto-lint:
    cd proto && buf lint

proto-gen:
    cd proto && buf generate