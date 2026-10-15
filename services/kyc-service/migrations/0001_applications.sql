-- +goose Up
-- applications — заявки на верификацию (главная таблица kyc-service).
-- applicant_full_name / applicant_dob — «снимок» профиля на момент подачи:
-- авто-чеки (санкционный список, возраст) обязаны работать с теми данными,
-- с которыми заявка подавалась, а не с текущим состоянием профиля.
-- reviewer_id — офицер, забравший заявку в работу (NULL = никто).
CREATE TABLE applications (
    id                  UUID PRIMARY KEY,
    customer_id         UUID NOT NULL,
    applicant_full_name TEXT NOT NULL DEFAULT '',
    applicant_dob       DATE,
    reviewer_id         UUID,
    status              TEXT NOT NULL DEFAULT 'SUBMITTED',
    reject_reason       TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- «Не более одной активной заявки на клиента» (ТЗ 5.4).
-- Частичный уникальный индекс: уникальность проверяется только среди
-- активных статусов, закрытые заявки (APPROVED/REJECTED/AUTO_REJECTED)
-- не мешают клиенту подать новую.
CREATE UNIQUE INDEX one_active_per_customer
    ON applications (customer_id)
    WHERE status IN ('SUBMITTED', 'AUTO_CHECKS', 'IN_REVIEW', 'DOCS_REQUESTED');

-- +goose Down
DROP TABLE applications;
