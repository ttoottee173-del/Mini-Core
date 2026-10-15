-- Запросы к таблице applications — «источник правды» для sqlc.
-- Даты наружу отдаём строками (YYYY-MM-DD и RFC3339), как и в остальных
-- сервисах: домен не зависит от типов дат драйвера.

-- Вставка новой заявки. Уникальность «не более одной активной заявки
-- на клиента» обеспечивает частичный индекс из миграции — при попытке
-- второй активной Postgres вернёт unique_violation (23505), репозиторий
-- переведёт её в доменную ErrAlreadyExists.
-- Дату передаём готовым pgtype.Date (конвертация из строки — забота
-- адаптера, не SQL: касты внутри VALUES sqlc не выводит).
-- name: CreateApplication :exec
INSERT INTO applications (id, customer_id, applicant_full_name, applicant_dob, status)
VALUES ($1, $2, $3, $4, $5);

-- name: GetApplication :one
SELECT id, customer_id, applicant_full_name,
       coalesce(to_char(applicant_dob, 'YYYY-MM-DD'), '')::text AS applicant_dob,
       coalesce(reviewer_id::text, '')::text                    AS reviewer_id,
       status, reject_reason,
       to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::text AS created_at,
       to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::text AS updated_at
FROM applications
WHERE id = $1;

-- Активная заявка клиента (их не может быть больше одной — см. индекс).
-- :many, а не :one, чтобы «нет активной заявки» не было ошибкой чтения.
-- name: FindActiveByCustomer :many
SELECT id, status
FROM applications
WHERE customer_id = $1
  AND status IN ('SUBMITTED', 'AUTO_CHECKS', 'IN_REVIEW', 'DOCS_REQUESTED');

-- Очередь заявок. Два запроса (без фильтра / с фильтром) вместо одного
-- с условием — тот же приём, что в customer-service: Postgres не
-- разрешает именованные параметры в LIMIT, а смешивать их с
-- позиционными sqlc не умеет.
-- name: ListApplications :many
SELECT id, customer_id, applicant_full_name,
       coalesce(to_char(applicant_dob, 'YYYY-MM-DD'), '')::text AS applicant_dob,
       coalesce(reviewer_id::text, '')::text                    AS reviewer_id,
       status, reject_reason,
       to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::text AS created_at,
       to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::text AS updated_at
FROM applications
ORDER BY created_at
LIMIT $1 OFFSET $2;

-- name: ListApplicationsByStatus :many
SELECT id, customer_id, applicant_full_name,
       coalesce(to_char(applicant_dob, 'YYYY-MM-DD'), '')::text AS applicant_dob,
       coalesce(reviewer_id::text, '')::text                    AS reviewer_id,
       status, reject_reason,
       to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::text AS created_at,
       to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::text AS updated_at
FROM applications
WHERE status = $1
ORDER BY created_at
LIMIT $2 OFFSET $3;

-- ClaimApplication (ТЗ: «одну заявку одновременно ревьюит один OFFICER —
-- оптимистическая блокировка»). Условие reviewer_id IS NULL в WHERE и есть
-- блокировка: выигрывает тот UPDATE, который прошёл первым, второй получит
-- 0 обновлённых строк (:execrows) — это и есть «проигрыш гонки».
-- name: ClaimApplication :execrows
UPDATE applications
SET reviewer_id = @officer_id, updated_at = now()
WHERE id = @id AND status = 'IN_REVIEW' AND reviewer_id IS NULL;

-- Следующая заявка для авто-чеков: сначала свежеподанные (SUBMITTED),
-- потом застрявшие в прошлом запуске (AUTO_CHECKS). Гонку двух воркеров
-- гасит GuardedTransition (WHERE status = ожидаемый) — заявка реально
-- обработается один раз, второй воркер просто не найдёт строку.
-- name: FindNextSubmitted :many
SELECT id, status
FROM applications
WHERE status IN ('SUBMITTED', 'AUTO_CHECKS')
ORDER BY created_at
LIMIT 1;
