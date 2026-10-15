-- Запросы к таблице refresh_tokens (храним только SHA-256 хеши токенов).

-- name: CreateRefreshToken :exec
INSERT INTO refresh_tokens (token_hash, user_id, expires_at)
VALUES ($1, $2, $3);

-- name: GetRefreshToken :one
SELECT token_hash, user_id, expires_at, revoked_at
FROM refresh_tokens
WHERE token_hash = $1;

-- name: RevokeRefreshToken :exec
UPDATE refresh_tokens
SET revoked_at = $1
WHERE token_hash = $2;
