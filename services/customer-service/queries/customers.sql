-- Запросы к таблице customers — «источник правды» для sqlc.
-- Дату рождения храним типом DATE, наружу отдаём строкой YYYY-MM-DD:
-- to_char при чтении, to_date при записи. Так домен не зависит от
-- типов дат библиотеки драйвера.

-- Вставка пустого профиля. ON CONFLICT DO NOTHING делает её
-- идемпотентной: повторное событие user_registered не уронит consumer.
-- name: CreateCustomer :exec
INSERT INTO customers (id, user_id, status)
VALUES ($1, $2, $3)
ON CONFLICT (user_id) DO NOTHING;

-- coalesce + to_char: если даты нет, вернётся пустая строка, а не NULL.
-- name: GetCustomerByUserID :one
SELECT id, user_id, first_name, last_name, phone,
       coalesce(to_char(date_of_birth, 'YYYY-MM-DD'), '')::text AS date_of_birth,
       address, citizenship, status
FROM customers
WHERE user_id = $1;

-- Именованные параметры (@имя) дают понятные поля в сгенерированной
-- структуре UpdateCustomerParams. nullif превращает пустую строку в NULL,
-- поэтому незаполненная дата рождения останется NULL и в базе.
-- name: UpdateCustomer :exec
UPDATE customers
SET first_name    = @first_name,
    last_name     = @last_name,
    phone         = @phone,
    date_of_birth = to_date(nullif(@date_of_birth::text, ''), 'YYYY-MM-DD'),
    address       = @address,
    citizenship   = @citizenship,
    status        = @status,
    updated_at    = now()
WHERE user_id = @user_id;

-- Список без фильтра. Фильтр по статусу вынесен в отдельный запрос ниже.
-- name: ListCustomers :many
SELECT id, user_id, first_name, last_name, phone,
       coalesce(to_char(date_of_birth, 'YYYY-MM-DD'), '')::text AS date_of_birth,
       address, citizenship, status
FROM customers
ORDER BY created_at
LIMIT $1 OFFSET $2;

-- Список с фильтром по статусу. Два запроса вместо одного с условием
-- «пустой фильтр — вернуть всех»: Postgres не разрешает именованные
-- параметры в LIMIT, а смешивать их с позиционными sqlc не умеет.
-- Репозиторий сам выбирает метод по наличию фильтра — это проще,
-- чем хитрые касты в SQL.
-- name: ListCustomersByStatus :many
SELECT id, user_id, first_name, last_name, phone,
       coalesce(to_char(date_of_birth, 'YYYY-MM-DD'), '')::text AS date_of_birth,
       address, citizenship, status
FROM customers
WHERE status = $1
ORDER BY created_at
LIMIT $2 OFFSET $3;
