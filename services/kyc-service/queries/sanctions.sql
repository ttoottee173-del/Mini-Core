-- sanctions_list: мок-проверка по «санкционному списку» (ТЗ 5.4).
-- Совпадение считается только при равенстве И имени, И даты рождения:
-- тёзки с разными датами не должны отклоняться.
-- Дату передаём готовым pgtype.Date (парсинг строки — в адаптере).
-- name: SanctionsMatch :one
SELECT EXISTS (
    SELECT 1
    FROM sanctions_list
    WHERE lower(full_name) = lower($1)
      AND date_of_birth = $2
) AS hit;
