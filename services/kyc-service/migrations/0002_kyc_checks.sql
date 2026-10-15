-- +goose Up
-- kyc_checks — результаты каждого авто-чека (ТЗ: «результаты каждого
-- чека сохраняются в kyc_checks»). UNIQUE(application_id, check_type)
-- делает вставку идемпотентной: повторная обработка заявки
-- (at-least-once!) не задвоит чеки.
CREATE TABLE kyc_checks (
    id             UUID PRIMARY KEY,
    application_id UUID NOT NULL REFERENCES applications(id),
    check_type     TEXT NOT NULL,          -- SANCTIONS / AGE
    passed         BOOLEAN NOT NULL,
    details        TEXT NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (application_id, check_type)
);

-- +goose Down
DROP TABLE kyc_checks;
