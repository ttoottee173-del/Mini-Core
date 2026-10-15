-- kyc_checks и kyc_events: чеки и аудит.

-- Идемпотентная вставка результата чека: UNIQUE(application_id, check_type)
-- + ON CONFLICT DO NOTHING — повторная обработка не задвоит чек.
-- name: InsertCheck :exec
INSERT INTO kyc_checks (id, application_id, check_type, passed, details)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (application_id, check_type) DO NOTHING;

-- Строка аудита. Вызывается репозиторием В ТОЙ ЖЕ транзакции, что и
-- UPDATE статуса, — см. GuardedTransition в internal/adapters/postgres.
-- name: AppendEvent :exec
INSERT INTO kyc_events (id, application_id, actor, from_status, to_status, comment)
VALUES ($1, $2, $3, $4, $5, $6);

-- GuardedTransition — смена статуса «с защитой»: строка обновится только
-- если заявка всё ещё в ожидаемом статусе from. :execrows вернёт 0, если
-- кто-то обогнал нас (гонка воркеров/офицеров) — репозиторий переведёт это
-- в доменную ErrInvalidTransition.
-- name: TransitionApplication :execrows
UPDATE applications
SET status        = @to_status,
    reject_reason = @reject_reason,
    updated_at    = now()
WHERE id = @id AND status = @from_status;

-- Повторная подача (DOCS_REQUESTED → SUBMITTED): заявка возвращается в
-- общую очередь, прежний офицер её больше «не держит». Вызывается
-- репозиторием в одной транзакции с TransitionApplication.
-- name: ResetReviewer :exec
UPDATE applications
SET reviewer_id = NULL, updated_at = now()
WHERE id = $1;
