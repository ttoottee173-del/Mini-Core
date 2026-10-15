-- +goose Up
-- sanctions_list — мок «санкционного списка» из ТЗ: ФИО + дата рождения.
-- Совпадение и имени, и даты → AUTO_REJECTED. В реальной системе это
-- внешняя проверка по API; здесь — своя таблица с парой тестовых строк.
CREATE TABLE sanctions_list (
    id            UUID PRIMARY KEY,
    full_name     TEXT NOT NULL,
    date_of_birth DATE NOT NULL
);

-- Тестовые данные: на них можно проверить срабатывание авто-чека.
INSERT INTO sanctions_list (id, full_name, date_of_birth) VALUES
    ('00000000-0000-7000-8000-000000000001', 'Ivan Ivanov',  '1990-01-01'),
    ('00000000-0000-7000-8000-000000000002', 'Maria Petrova', '1985-03-15');

-- +goose Down
DROP TABLE sanctions_list;
