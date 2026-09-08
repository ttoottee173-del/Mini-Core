-- +goose Up
CREATE TABLE refresh_tokens (
    token_hash TEXT PRIMARY KEY,          -- хеш токена, не сам токен!
    user_id    UUID NOT NULL REFERENCES users(id),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ                 -- для будущего Logout
);

-- +goose Down
DROP TABLE refresh_tokens;