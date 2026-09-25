-- Запросы к таблице users. Каждая аннотация «-- name: Имя :тип» — это
-- генерируемый метод в Go (:exec — ничего не возвращает, :one — одна строка).

-- name: CreateUser :exec
INSERT INTO users (id, email, password_hash, role)
VALUES ($1, $2, $3, $4);

-- name: GetUserByEmail :one
SELECT id, email, password_hash, role
FROM users
WHERE email = $1;

-- name: GetUserByID :one
SELECT id, email, password_hash, role
FROM users
WHERE id = $1;
