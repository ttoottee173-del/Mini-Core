-- +goose Up
CREATE TABLE customers (
    id            UUID PRIMARY KEY,
    user_id       UUID NOT NULL UNIQUE,
    first_name    TEXT NOT NULL DEFAULT '',
    last_name     TEXT NOT NULL DEFAULT '',
    phone         TEXT NOT NULL DEFAULT '',
    date_of_birth DATE,
    address       TEXT NOT NULL DEFAULT '',
    citizenship   TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'NEW',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE customers;