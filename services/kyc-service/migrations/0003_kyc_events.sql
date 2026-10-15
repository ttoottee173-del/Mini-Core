-- +goose Up
-- kyc_events — полный аудит жизненного цикла заявки (ТЗ: «кто, когда,
-- какой переход, комментарий»). Строку сюда пишет тот же SQL-транзакций,
-- что меняет статус заявки, — аудит не может «отстать» от статуса.
CREATE TABLE kyc_events (
    id             UUID PRIMARY KEY,
    application_id UUID NOT NULL REFERENCES applications(id),
    actor          TEXT NOT NULL,           -- user_id клиента/офицера или 'auto-checks'
    from_status    TEXT NOT NULL DEFAULT '',
    to_status      TEXT NOT NULL,
    comment        TEXT NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE kyc_events;
