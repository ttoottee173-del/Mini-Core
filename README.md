# Mini-Core — учебное банковское ядро на Go. Каждый файл целиком + построчный разбор

> **Как устроен этот README.** Начиная с раздела 4, каждый файл проекта приведён **целиком** (как он лежит в репозитории), а под ним — подробный разбор: зачем этот файл существует с инженерной точки зрения, как он работает построчно, и какие решения в нём спрятаны. Это не справка «на бегу» — это попытка положить тебе в голову картину целиком: от синтаксиса Go до того, как один HTTP-запрос превращается в INSERT в Postgres и событие в Kafka.
>
> **Пример вопроса, который закрывается таким форматом.** Вот фрагмент `verifier.go` из gateway:
>
> ```go
> func New(publicPEM []byte) (*Verifier, error) {
> 	pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
> 	if err != nil {
> 		return nil, fmt.Errorf("public key: %w", err)
> 	}
> 	return &Verifier{
> 		key:    func(*jwt.Token) (interface{}, error) { return pub, nil },
> 		parser: jwt.NewParser(jwt.WithValidMethods([]string{"RS256"})),
> 	}, nil
> }
> ```
>
> На первый взгляд: «проверяет подпись, если нет — ошибка, если всё норм — отправляет новую подпись?». Ни то, ни другое. Разбор в разделе 5.3 покажет, что здесь вообще **ничего не проверяется и не подписывается** — это конструктор, который один раз при старте **заряжает инструмент**, а сама проверка происходит в совсем другом методе (`Verify`) на каждом запросе. Когда ты увидишь оба метода рядом построчно, путаница исчезнет. Такой разбор ниже получает каждый файл.

**Содержание**

1. [Что это за проект](#1-что-это-за-проект)
2. [Быстрый старт: поднять и потрогать руками](#2-быстрый-старт-поднять-и-потрогать-руками)
3. [Go с нуля: минимум, которого хватает на этот проект](#3-go-с-нуля-минимум-которого-хватает-на-этот-проект)
4. [Корень репозитория: каждый файл целиком](#4-корень-репозитория-каждый-файл-целиком)
5. [api-gateway — каждый файл целиком](#5-api-gateway--каждый-файл-целиком)
6. [auth-service — каждый файл целиком](#6-auth-service--каждый-файл-целиком)
7. [customer-service — каждый файл целиком](#7-customer-service--каждый-файл-целиком)
8. [Сгенерированный код: protobuf и sqlc](#8-сгенерированный-код-protobuf-и-sqlc)
9. [Главный сценарий: от «создать перса» до заполненной анкеты](#9-главный-сценарий-от-создать-перса-до-заполненной-анкеты)
10. [Сквозной разбор ошибок](#10-сквозной-разбор-ошибок)
11. [Грабли и частые вопросы](#11-грабли-и-частые-вопросы)
12. [Что развивать дальше](#12-что-развивать-дальше)
13. [Шпаргалка команд](#13-шпаргалка-команд)

Порядок чтения, если ты здесь впервые: **раздел 1 → 2 (запусти проект!) → 3 → 5.3 (разбор Verifier'а как образец) → 6 (auth: домен и сценарии — вся суть архитектуры) → 9 (сценарий целиком)**. Остальное — по мере правок.

## 1. Что это за проект

Mini-Core — упрощённое ядро банка. Пользователь регистрируется (email + пароль), в базе автоматически создаётся пустой профиль клиента («перс»), пользователь заполняет анкету (ФИО, телефон, дата рождения, адрес, гражданство), после чего профиль можно смотреть. Это учебная модель первых трёх недель большого ТЗ: дальше были бы KYC, счета, карты и платежи — всё это выходит за рамки проекта.

Почему именно **микросервисы**, а не одна программа? Учебная цель — прочувствовать то, с чем ты столкнёшься на работе: сервисы живут в разных процессах, у каждого **своя база данных** (database per service), они не могут просто вызвать функцию соседа — только сеть (gRPC) или шину событий (Kafka). Как только это понять, становятся объяснимыми все странности архитектуры: токены проверяются локально через публичный ключ, а не «спрашивают auth-service»; профиль создаётся по событию из Kafka, а не прямым вызовом.

**Стек и зачем каждому месту его инструмент:**

| Технология | Где | Почему она, а не что-то другое |
|---|---|---|
| Go 1.22+ | все сервисы | требования ТЗ; статическая типизация и простота деплоя — стандарт индустрии для бэкенда |
| gRPC + protobuf | между сервисами | строгое описание контракта в .proto; из него генерируется и сервер, и клиент — не пишем руками ни HTTP-парсинг, ни сериализацию |
| REST (JSON) | снаружи gateway | браузер/curl/фронтенд говорят HTTP; gRPC наружу не отдаём |
| PostgreSQL | auth_db, customer_db | две отдельные базы — «database per service», сервисы не лезут в чужие таблицы |
| Kafka | топик `auth.events` | слабая связанность: auth не знает, кто и когда воспользуется фактом «зарегистрировался юзер» |
| Redis | rate limit в gateway | токен-бакет с атомарным Lua-скриптом — счётчики в памяти процесса не пережили бы рестарт и не делились бы между инстансами |
| sqlc | auth, customer | SQL пишем руками в .sql-файлах, Go-код генерируется: типобезопасно, без магии ORM |
| buf | proto | современный тулчейн вместо голого protoc: lint + генерация одной командой |

```mermaid
graph TD
    Client["curl / Postman / будущий фронтенд"] -->|"REST JSON :8080"| GW[api-gateway]
    GW -->|"gRPC :50051"| AUTH[auth-service]
    GW -->|"gRPC :50052"| CUST[customer-service]
    AUTH -->|SQL| PGA[(PostgreSQL<br/>auth_db)]
    CUST -->|SQL| PGC[(PostgreSQL<br/>customer_db)]
    AUTH -->|"событие user_registered"| K[(Kafka<br/>topic: auth.events)]
    K -->|"читает событие"| CUST
    AUTH -.->|"/jwks: публичный ключ"| GW
```

Пунктирная линия важна: gateway **не ходит** в auth-service за проверкой каждого токена. Один раз при старте он читает публичный ключ (PEM-файл; эндпоинт `/jwks` тоже есть) и дальше проверяет подписи сам. Это классика: подпись проверяется локально, по сети не гоняем.

**Три сервиса и их зоны ответственности:**

| Сервис | Адрес | Отвечает за | Не отвечает за |
|---|---|---|---|
| **api-gateway** | :8080 HTTP | REST наружу, проверка JWT, rate limit, перевод JSON↔gRPC, request-id | бизнес-логику (ТЗ прямо запрещает) |
| **auth-service** | :50051 gRPC, :8081 HTTP (JWKS) | учётные записи, пароли (argon2id), выпуск JWT, публикация событий в Kafka | профили клиентов |
| **customer-service** | :50052 gRPC | анкеты клиентов, статусная машина, реакция на `user_registered` | пароли, токены, регистрацию |

«Слой» внутри каждого сервиса повторяет гексагональную архитектуру (порты и адаптеры). Не пугайся слова — на практике это три папки:

- `internal/domain` — сущности и бизнес-правила. Не зависит ни от сети, ни от базы. Тестировать можно без Docker.
- `internal/app` — сценарии (use cases): «зарегистрировать», «залогинить», «обновить анкету». Зависит только от domain и **интерфейсов** — хранилище и Kafka тут просто интерфейсы.
- `internal/adapters` — реализации интерфейсов и входные точки: gRPC-сервер, репозиторий Postgres, продюсер/консьюмер Kafka, JWKS. Плюс `internal/config` (настройки из окружения) и `cmd/main.go` (сборка всего этого в работающую программу).

Направление зависимостей строгое: `adapters → app → domain`. Ни domain, ни app не знают про Postgres, gRPC и Kafka — они знают про *интерфейсы*. Вся «сборка» происходит в `main.go`.

---

## 2. Быстрый старт: поднять и потрогать руками

### 2.1 Что нужно установить

1. **Go** ≥ 1.22 (проект собирался на 1.27): [go.dev/dl](https://go.dev/dl). Проверка: `go version`.
2. **Docker + Docker Compose** — для Postgres, Kafka, Redis.
3. **buf** и **sqlc** — только если будешь менять proto или SQL. Для просто запуска не нужны: сгенерированный код лежит в репозитории (папки `gen/` и `internal/adapters/postgres/db/`). Установка: `go install github.com/bufbuild/buf/cmd/buf@latest` и `go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest`.

### 2.2 Ключи для JWT

auth-service подписывает токены приватным RSA-ключом, gateway проверяет публичным. Пара ключей лежит в `services/auth-service/keys/` (в git они игнорируются через `.gitignore`, в архиве проекта — есть, чтобы проект стартовал сразу). Сгенерировать свои:

```bash
cd services/auth-service/keys
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out private.pem
openssl rsa -in private.pem -pubout -out public.pem
```

### 2.3 Поднять инфраструктуру

```bash
docker compose up -d        # Postgres :5432, Kafka :9092, Redis :6379
docker compose ps           # все три должны быть Up
```

Postgres при первом старте сам выполнит `scripts/init-dbs.sql` и создаст вторую базу `customer_db` (первая, `auth_db`, задаётся переменной `POSTGRES_DB` в docker-compose.yml). Миграции таблиц — обычные SQL-файлы в `services/*/migrations/`; в учебном проекте их достаточно применить руками:

```bash
# auth_db: таблицы users и refresh_tokens
docker exec -i $(docker ps -qf name=postgres) psql -U dev -d auth_db < services/auth-service/migrations/0001_users.sql
docker exec -i $(docker ps -qf name=postgres) psql -U dev -d auth_db < services/auth-service/migrations/0002_refresh_tokens.sql
# customer_db: таблица customers
docker exec -i $(docker ps -qf name=postgres) psql -U dev -d customer_db < services/customer-service/migrations/0001_customers.sql
```

### 2.4 Запустить три сервиса (три терминала)

```bash
make run-gateway     # терминал 1: REST на :8080
make run-auth        # терминал 2: gRPC :50051 + JWKS :8081
make run-customer    # терминал 3: gRPC :50052 + консьюмер Kafka
```

`make run-*` — это просто `cd services/<сервис> && go run ./cmd`. Если хочешь бинарники: `make build` сложит их в `bin/`.

### 2.5 Прогонить весь сценарий

```bash
# 1. Регистрация (после этого customer-service молча создаст пустой профиль!)
curl -s localhost:8080/auth/register -d '{"email":"anna@example.com","password":"password123"}'
# → {"user_id":"0192...","email":"anna@example.com","role":"CLIENT"}

# 2. Логин: пара токенов
curl -s localhost:8080/auth/login -d '{"email":"anna@example.com","password":"password123"}'
# → {"access_token":"eyJ...","refresh_token":"kX3..."}

ACCESS=...   # вставь access_token из ответа

# 3. Заполняем анкету (профиль станет PROFILE_FILLED)
curl -s -X PUT localhost:8080/customers/me \
  -H "Authorization: Bearer $ACCESS" \
  -d '{"first_name":"Анна","last_name":"Иванова","phone":"+7 900 000-00-00","date_of_birth":"1990-05-14","address":"г. Москва, ул. Ленина, 1","citizenship":"RU"}'

# 4. Смотрим свой профиль
curl -s localhost:8080/customers/me -H "Authorization: Bearer $ACCESS"

# 5. Refresh: старый refresh гасится, выдаётся новая пара
curl -s localhost:8080/auth/refresh -d '{"refresh_token":"..."}'
```

И два места, где можно *увидеть* асинхронность: `make kafka-read` покажет событие `user_registered` в топике, а `make kafka-lag` — что группа `customer-service` всё прочитала. В логах customer-service в момент регистрации появится строка `событие: user_registered (id=...)`.

Если что-то не поднялось — раздел [11. Грабли](#11-грабли-и-частые-вопросы), там список типовых ошибок и что проверить.

---

## 3. Go с нуля: минимум, которого хватает на этот проект

Этот раздел — ускоренный курс языка ровно в том объёме, в котором код его использует. Каждый пункт — с примером из репозитория. Если уже пишешь на Go — пролистай диагонально: про модули и workspace всё равно стоит освежить.

### 3.1 Пакеты и модули

**Пакет** (`package`) — папка с .go-файлами, минимальная единица компиляции. Файлы одной папки обязаны иметь один и тот же `package` в шапке. Всё, что начинается с **заглавной буквы** (`Config`, `New`, `Load`) — экспортируется наружу пакета; с маленькой (`pool`, `envOr`) — видно только внутри пакета. Никаких ключевых слов `public/private` в Go нет — регистр первой буквы решает всё.

**Модуль** — проект с файлом `go.mod`, в котором указаны имя модуля, версия языка и зависимости. У нас четыре модуля: три сервиса + корневой `go.work`:

```go
// go.work — Go workspace: три модуля собираются как единое целое
go 1.27.1

use (
        ./services/api-gateway
        ./services/auth-service
        ./services/customer-service
)
```

Workspace нужен, чтобы gateway импортировал `mini-core/services/auth-service/gen/...` прямо из соседней папки, а не из опубликованного в сети пакета. Имя модуля (`module mini-core/services/auth-service` в go.mod) — это префикс всех импортов внутри него.

**Импорт** — путь к пакету относительно корня модуля:

```go
import (
        "context"                                   // стандартная библиотека
        "github.com/jackc/pgx/v5/pgxpool"           // внешняя зависимость (go.mod)
        "mini-core/services/auth-service/internal/domain" // свой пакет
)
```

Папка `internal/` — специальная: импортировать её пакеты может только код внутри того же модуля. Идеально, чтобы никто снаружи не depended на наших адаптерах. Именно поэтому всё кроме `cmd` живёт в `internal/`.

### 3.2 Структуры, методы, указатели

Структура — набор полей. Метод — функция с «получателем» перед именем:

```go
type UserRepo struct {
        q *db.Queries   // поле — указатель на сгенерированную sqlc структуру
}

// «получатель» (r *UserRepo) — аналог this/self
func (r *UserRepo) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
        row, err := r.q.GetUserByEmail(ctx, email)  // r.q — доступ к полю
        ...
}
```

`*` перед типом — **указатель**: не копия значения, а ссылка на оригинал. В Go всё передаётся по значению, поэтому большие структуры и всё, что надо менять «на месте», передают указателем. Правило проекта: `*domain.User`, `*slog.Logger` — везде указатели на структуры; строки, числа, контексты — значениями.

Нулевое значение указателя — `nil`. Отсюда типичный код `if err != nil` и типы вроде `*time.Time` в `domain.RefreshToken.RevokedAt`: `nil` означает «отзыва не было».

Создание структуры — литералом с именами полей (так и читается, и безопасно при добавлении новых полей):

```go
return &domain.Customer{
        ID:     id,
        UserID: userID,
        Status: domain.StatusNew,
}
```

`&` перед литералом = «создать и отдать указатель».

### 3.3 Интерфейсы — главный инструмент проекта

Интерфейс — список методов. Всё, что умеет эти методы, **автоматически** подходит под интерфейс — реализовывать ничего не нужно, ключевого слова `implements` в Go нет:

```go
// Объявлено в internal/app/login.go — там, где интерфейс ИСПОЛЬЗУЕТСЯ
type UserReader interface {
        FindByEmail(ctx context.Context, email string) (*domain.User, error)
}
```

Ни один репозиторий не пишет «я реализую UserReader». Но `postgres.UserRepo` умеет `FindByEmail` с ровно такой сигнатурой — значит, его можно подсунуть в `LoginService`. Это называется *неявная реализация*, и из-за неё интерфейсы в Go объявляют у потребителя, а не у исполнителя: сценарию нужен только `FindByEmail`? Значит, интерфейс состоит из одного метода.

Зачем это всё? **Тестируемость.** Смотри `internal/app/login_test.go`: сценарий Login проверяется с `fakeUsers` и `fakeTokens` — структурами-обёртками, которые возвращают заранее заготовленные ответы. Никакой базы, никакой сети, тест выполняется за миллисекунды:

```go
type fakeUsers struct {
        u   *domain.User
        err error
}

func (f *fakeUsers) FindByEmail(_ context.Context, _ string) (*domain.User, error) {
        if f.err != nil {
                return nil, f.err
        }
        return f.u, nil
}
```

Подчёркивание `_` вместо имени параметра — «этот аргумент мне не нужен». Это не спецсинтаксис тестов: `_` — везде в Go «выбросить значение».

### 3.4 Ошибки: значения, а не исключения

В Go нет try/catch. Ошибка — обычное значение, последний возвращаемый аргумент, и её принято проверять сразу:

```go
row, err := r.q.GetUserByEmail(ctx, email)
if err != nil {
        return nil, mapUserErr(err)
}
```

`error` — встроенный интерфейс из одного метода `Error() string`. Свои «известные» ошибки создаются один раз и сравниваются через `errors.Is`:

```go
// domain/user.go
var ErrNotFound = errors.New("not found")

// где-то в репозитории
if errors.Is(err, pgx.ErrNoRows) {
        return domain.ErrNotFound
}
```

`errors.Is` проходит по цепочке обёрнутых ошибок (обёртка — через `fmt.Errorf("...: %w", err)`), поэтому работает даже если ошибка прошла несколько слоёв. Три репозитория проекта выполняют одну и ту же работу: переводят ошибки драйвера (`pgx.ErrNoRows`, код 23505) в ошибки домена (`ErrNotFound`, `ErrEmailTaken`). Слои выше ничего не знают про Postgres — им не нужно.

Паника (`panic`) — это не про ожидаемые ошибки, а про программные баги. Перехватывается `recover()` — так устроен `recoverMW` в gateway: паника в одном хендлере превращается в HTTP 500, а не в падение процесса.

### 3.5 context.Context

`context` — первый параметр почти каждого метода в проекте. Это «рюкзак запроса»: дедлайны, сигналы отмены, метаданные. Три главных сценария в коде:

1. **Отмена по Ctrl+C.** `signal.NotifyContext` в каждом `main.go` создаёт контекст, который отменяется по SIGINT/SIGTERM. Всё, что работает «долго» (gRPC-сервер, консьюмер Kafka), следит за этим контекстом и аккуратно завершается.
2. **Таймауты.** `context.WithTimeout(...)` — контекст, который сам отменится через 10 секунд (graceful shutdown gateway).
3. **Прокидывание значений.** `context.WithValue` + парные функции-доступы — так claims из JWT и request-id путешествуют между middleware в gateway (см. `httpapi/helpers.go`):

```go
type ctxKey int                     // свой тип ключа: чтобы не пересечься с чужими

const (
        ctxKeyClaims ctxKey = iota // iota: 0, следующая константа 1, ...
        ctxKeyRequestID
)

func withClaims(ctx context.Context, c *Claims) context.Context {
        return context.WithValue(ctx, ctxKeyClaims, c)
}
func claimsFromCtx(ctx context.Context) *Claims {
        c, _ := ctx.Value(ctxKeyClaims).(*Claims)  // type assertion: «достань *Claims или дай nil»
        return c
}
```

Конструкция `v, ok := x.(Тип)` — *type assertion*: проверка, что в интерфейсе лежит значение типа `Тип`. Если нет — `ok == false`, паники не будет. Запятая после `c, _` — «false мне не интересен, дай значение».

### 3.6 Горутины, каналы и `go func() { ... }()`

Горутина — легковесный поток: `go перед вызовом` и функция выполняется параллельно. В проекте их ровно там, где нужно без них нельзя:

```go
// auth-service: HTTP-сервер JWKS живёт в фоне, main продолжает сборку gRPC
go func() {
        if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                log.Warn("jwks http", "err", err)
        }
}()
```

`func() { ... }()` — анонимная функция, вызываемая сразу; `go` — «запусти её отдельной горутиной». Каналы в проекте используются в простейшей форме: `<-ctx.Done()` — приём из канала, который закроется при отмене контекста:

```go
go func() {
        <-ctx.Done()                  // блокируемся до Ctrl+C
        log.Info("graceful shutdown...")
        srv.GracefulStop()            // аккуратно гасим сервер
}()
```

Порядок чтения полей, замыкания (анонимные функции видят переменные снаружи — `fatal` в main использует `log` из окружающей области) и `defer` (отложить вызов до выхода из функции — `defer pool.Close()`, `defer rows.Close()`) — вот и весь конкурентный арсенал проекта.

### 3.7 Мелочи, которые встретятся в каждой строке

- **Несколько возвращаемых значений**: `access, refresh, err := s.login.Login(...)`. Идиоматично возвращать `(результат, ошибка)`.
- **Короткое объявление `:=`** — создать и присвоить с выводом типа; `=` — присвоить в существующее. Внутри `if` тоже работает: `if v := os.Getenv(key); v != "" { ... }` — переменная живёт только внутри if.
- **switch вместо лестницы if/else** — в `grpcapi/server.go`: `switch { case errors.Is(...): ... }` — switch без выражения, условия по очереди.
- **struct tags** — аннотации у полей в бэктиках: `Email string \`json:"email"\``. Их читают `encoding/json` (как называется поле в JSON) и sqlc. Это данные о данных, сам Go их не интерпретирует.
- **iota** — счётчик внутри блока констант (см. 3.5).
- **variadic**: `func Chain(h http.Handler, mws ...Middleware)` — сколько угодно аргументов, внутри это срез.
- **срезы и append**: `var items []domain.Customer; items = append(items, *c)`. Срез — динамический массив; `append` возвращает новый срез, присваиваем обратно.
- **map**: `map[string]any{...}` — словарь; используется в JWKS-хендлере и payload-ах Kafka.
- **Методы-конструкторы `New*`** — не язык, а конвенция: функция `NewX(...)` возвращает готовый `*X`. Везде в проекте: `config.Load`, `db.New`, `verifier.New`.

### 3.8 Тесты

Файл с суффиксом `_test.go` компилируется только для тестов. Функция `func TestXxx(t *testing.T)` — один тест; `t.Fatal(err)` — «упал, дальше не проверяем». Тесты проекта (`login_test.go`, `register_test.go`, `domain/customer_test.go`) — юнит-тесты домена и сценариев с фейками вместо инфраструктуры. Запуск: `make test` (это `go test ./...` в каждом модуле). Тестировать сгенерированный код (gen/, db/) не нужно — его задача просто компилироваться и соответствовать контракту.
### 3.9 (бонус) Как читать ошибки компилятора

Go-компилятор многословен, но его сообщения честные. Три типовых для новичка:

- `undefined: log.Fatalf` — у типа `*slog.Logger` нет метода Fatalf (в отличие от старого пакета log). Ищем правильное имя: у slog это Error/Info/Warn. Именно эта ошибка возникла у нас при переходе main'ов на slog — и решилась маленьким помощником `fatal` в cmd/main.go.
- `cannot use x (variable of type string) as int32 value` — проверь типы на границе: sqlc генерирует int32 для LIMIT/OFFSET, приложение считает в int; конверсия `int32(limit)` — забота адаптера.
- `declared and not used: err` — Go запрещает неиспользуемые переменные. Если ошибка действительно не нужна — `_ = err` или обработай.

Ошибка всегда приходит с файлом и строкой: `services/auth-service/cmd/main.go:40:7`. Читай её первым делом — она почти всегда про суть, а не про мистику.

---

## 4. Корень репозитория: каждый файл целиком

Корень — это не код, а «правила игры»: как собраны модули (go.work), как воспроизводить генерацию и сборку (Makefile), на чём живёт инфраструктура (docker-compose.yml). Разберём каждый файл, потому что именно тут чаще всего непонятно, *зачем* он нужен, пока не увидишь, кто его читает.

```text
mini-core/
├── go.work                      # workspace: какие модули собираем вместе
├── Makefile                     # шпаргалка частых команд (up, gen, build, run-*, test)
├── docker-compose.yml           # инфраструктура: Postgres + Kafka + Redis
├── scripts/
│   └── init-dbs.sql             # создаст customer_db при первом старте Postgres
└── services/
    ├── api-gateway/             # REST наружу → gRPC внутрь
    ├── auth-service/            # учётки, JWT, события в Kafka
    └── customer-service/        # анкеты клиентов, консьюмер Kafka
```

Каждый сервис — **самостоятельный Go-модуль** со своим `go.mod`, и внутри устроен одинаково:

```text
services/auth-service/
├── cmd/
│   └── main.go                  # точка входа: собрать зависимости и запустить
├── internal/
│   ├── config/config.go         # настройки из переменных окружения
│   ├── domain/                  # сущности и правила (не зависит ни от чего)
│   ├── app/                     # сценарии + порты (интерфейсы)
│   └── adapters/                # реализации: grpcapi, postgres, kafka, ...
├── proto/                       # контракт gRPC + конфиги buf
│   ├── buf.yaml  buf.gen.yaml
│   └── bank/auth/v1/auth.proto
├── gen/                         # СГЕНЕРИРОВАНО из proto (не редактируем руками)
├── queries/                     # SQL-запросы — источник правды для sqlc
├── migrations/                  # SQL схемы (goose-совместимые аннотации)
├── keys/                        # PEM-ключи (в git не попадают)
├── sqlc.yaml                    # конфиг sqlc
└── go.mod
```

### 4.1 go.work

```
go 1.27.1

use (
        ./services/api-gateway
        ./services/auth-service
        ./services/customer-service
)
```

**Разбор.** В Go код раскладывается по **модулям** — единицам версионирования и разрешения импортов. У каждого модуля свой `go.mod` со своим именем. В этом проекте три сервиса = три модуля: `services/api-gateway/go.mod`, `services/auth-service/go.mod`, `services/customer-service/go.mod` (загляни в разделы 5–7, там каждый показан).

Зачем workspace-файл? Gateway импортирует пакеты из gen/ других модулей:

```go
authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
```

Имя `mini-core/services/auth-service` — это имя **чужого** модуля. Без go.work компилятор искал бы этот модуль... в интернете: он пошёл бы в прокси-репозиторий Go (proxy.golang.org) и попытался бы скачать версию `mini-core/services/auth-service`. Такого модуля там, естественно, нет — сборка упала бы.

`go work use ./services/...` говорит компилятору: «эти три модуля существуют **рядом на диске**, импорты их имён разрешай локально, ничего ниоткуда не скачивай». Файл сгенерирован один раз командой `go work init && go work use ./services/api-gateway ./services/auth-service ./services/customer-service` и больше руками не трогается.

Строка `go 1.27.1` — минимальная версия языка для всего workspace. Именно из неё workspace берёт правило «синтаксис не новее этой версии».

Куда смотрит `go build`? Он сначала проверяет go.work (если есть), потом go.mod текущего модуля. Практическое следствие: пока ты в корне репозитория, все три модуля собираются вместе; если скопировать один сервис в отдельную папку без workspace — он будет собираться только с заменой локальных импортов на публичные зависимости (например, через `go mod edit -replace`).

### 4.2 Makefile

```makefile
# Makefile — шпаргалка по частым командам.
# Запускать из корня репозитория: make <цель>

# --- Инфраструктура (Postgres + Kafka + Redis) ---

up:
	docker compose up -d

down:
	docker compose down

# --- Генерация кода ---
# proto-файлы теперь живут внутри каждого сервиса (services/*/proto),
# сгенерированный Go-код падает в services/*/gen.
# buf ищет свой конфиг (buf.yaml / buf.gen.yaml) в текущей папке,
# поэтому команды выполняются из services/*/proto.

proto-lint:
	cd services/auth-service/proto && buf lint
	cd services/customer-service/proto && buf lint

proto-gen:
	cd services/auth-service/proto && buf generate
	cd services/customer-service/proto && buf generate

# sqlc превращает SQL из services/*/queries в типизированный Go-код
# в services/*/internal/adapters/postgres/db.

sqlc-gen:
	cd services/auth-service && sqlc generate
	cd services/customer-service && sqlc generate

# Всё сразу: сгенерировать gRPC-код и код базы данных.
gen: proto-gen sqlc-gen

# --- Сборка и запуск ---

# go build ./... собирает все пакеты модуля; бинарник кладём в bin/
# (в git не попадает, см. .gitignore).

build:
	mkdir -p bin
	cd services/auth-service && go build -o ../../bin/auth-service ./cmd
	cd services/customer-service && go build -o ../../bin/customer-service ./cmd
	cd services/api-gateway && go build -o ../../bin/api-gateway ./cmd

test:
	cd services/auth-service && go test ./...
	cd services/customer-service && go test ./...
	cd services/api-gateway && go vet ./...

run-auth:
	cd services/auth-service && go run ./cmd

run-customer:
	cd services/customer-service && go run ./cmd

run-gateway:
	cd services/api-gateway && go run ./cmd

# --- Kafka: подсмотреть, что происходит в топике ---

kafka-topics:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-lag:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group customer-service

kafka-read:
	docker exec -it mini-core-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic auth.events --from-beginning
```

**Разбор.** Make — это «запоминатор команд»: каждая цель (`up`, `build`, `proto-gen`...) — ярлык для одной или нескольких shell-команд. Он нужен не потому, что команды сложные, а потому, что они **легко забываются или выполняются не из той папки**.

Разберём решения, которые здесь спрятаны:

- **`proto-lint` / `proto-gen` делают `cd services/auth-service/proto && buf ...`.** buf ищет свои конфиги (buf.yaml, buf.gen.yaml) в **текущей папке**. Если запустить `buf generate` из корня — он не найдёт конфиг и ничего не сделает или упадёт с загадочной ошибкой. Поэтому цель сначала переходит в папку с конфигом, потом запускается. Это же — главный источник ошибок «поменял proto, ничего не сгенерилось»: генерация не запускается сама, её надо позвать.
- **`sqlc-gen` делает `cd services/auth-service && sqlc generate`.** sqlc.yaml лежит в корне сервиса (не в proto/), потому что он относится к папкам migrations/ и queries/ относительно **сервиса**.
- **`gen: proto-gen sqlc-gen`** — цель может зависеть от других целей: `make gen` выполнит обе генерации по порядку. Одна команда на всё после правки контрактов.
- **`build` собирает в `bin/`, а не в текущую папку.** `mkdir -p bin` создаёт папку, если её нет (и не ругается, если есть). Бинарники — артефакты сборки, они не попадают в git (см. .gitignore ниже), но хранить их в одном месте удобно для запуска.
- **`test` — три строки, не цикл.** Make не умеет удобных циклов по списку; три явные строки проще и читаются с одного взгляда. Для gateway вместо `go test` стоит `go vet` — статический анализатор, потому что тестов в gateway нет (вся его логика — склейка, её проверяют интеграционным прогоном из раздела 2.5).
- **Блок kafka-\*** — три часто нужных команды отладки Kafka, спрятанные в docker exec. `kafka-read` особенно полезен: он показывает всё, что реально упало в топик `auth.events` — оттуда видно, дошло ли событие о регистрации.

Ключевая привычка: **правишь proto или SQL → запускаешь `make gen` → запускаешь `make build`**. Сгенерированный код не обновляется сам и не «подхватывается» при компиляции.

### 4.3 docker-compose.yml

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: dev
      POSTGRES_PASSWORD: dev
      POSTGRES_DB: auth_db
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./scripts/init-dbs.sql:/docker-entrypoint-initdb.d/init.sql

  kafka:
    image: apache/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
  redis:
    image: redis:7
    ports:
      - "6379:6379"

volumes:
  pgdata:
```

**Разбор.** docker-compose поднимает несколько docker-контейнеров одной командой и связывает их сетью. Здесь три контейнера — ровно та инфраструктура, которую сервисы ждут снаружи (адреса localhost:5432, localhost:9092, localhost:6379 берутся из конфигов разделов 5–7).

- **postgres.** Образ postgres:16. `environment` задаёт учётку `dev/dev` и базу `auth_db`, которую образ создаёт при первом старте. А вот вторая база (`customer_db`) создается специальным способом — см. 4.4. `ports: "5432:5432"` пробрасывает порт контейнера на хост: сервисы ходят на `localhost:5432` и попадают в контейнер. `volumes: pgdata:/var/lib/postgresql/data` — именованный docker-том: данные переживают перезапуск контейнера (`docker compose down` их не стирает, а вот `down -v` — стирает вместе с томом).
- **kafka.** Самый страшный на вид блок — минимум для Kafka **в режиме KRaft** (без ZooKeeper, современный вариант). По строчкам: `KAFKA_NODE_ID: 1` — id единственного узла; `KAFKA_PROCESS_ROLES: broker,controller` — этот узел и хранит данные (broker), и управляет кластером (controller) — так работает single-node KRaft; `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` — кто голосует за контроллера (сам с собой, порт 9093 — внутренний); `KAFKA_LISTENERS` — слушаем клиентский порт 9092 и контроллерный 9093; `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092` — **самая важная строка**: адрес, который Kafka называет клиентам («ходи ко мне сюда»). Клиенты — твои Go-сервисы на хосте, поэтому localhost. Если сюда написать имя контейнера — сервисы получат «ходи на kafka:9092», не сумеют разрешить имя и зависнут с загадочными таймаутами. `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` и соседние — запретить репликацию (нечем: узел один); без этого Kafka откажется создавать внутренние топики.
- **redis.** Один порт, без конфигов — Redis из коробки работает пустой конфигурацией. Используется только rate limit'ом gateway.

Почему всё это не в Go-коде? Потому что сервисы **не должны знать**, как поднимается инфраструктура — они знают только адреса (из env, см. config.go). Compose — это «окружение разработки», его можно заменить k8s-манифестами, не тронув ни одной строки сервиса.

### 4.4 scripts/init-dbs.sql

```sql
CREATE DATABASE customer_db OWNER dev;
```

**Разбор.** Одна строка, а вокруг неё главный грабль проекта. Образ postgres выполняет **все** скрипты из `/docker-entrypoint-initdb.d/` **один раз — при самом первом создании тома данных** (когда папка данных пуста). Синтаксис `./scripts/init-dbs.sql:/docker-entrypoint-initdb.d/init.sql` в compose монтирует наш скрипт именно туда.

Что происходит по шагам: образ создаёт `auth_db` (это задано в environment), потом выполняет наш скрипт — он создаёт `customer_db` с тем же владельцем `dev`. В итоге в одном инстансе Postgres живут две базы — по одной на сервис. Это реализация принципа **database per service**: у auth-сервиса нет прав/причин лезть в таблицы customer-сервиса и наоборот, интеграция — только через gRPC и события, никогда через «общую базу».

Грабль: скрипт выполняется **только при первом** создании тома. Поднял compose до добавления этого файла — customer_db не появится при рестарте. Лечение: `docker compose down -v` (сотрёт данные!) и `up` заново, либо руками `CREATE DATABASE customer_db OWNER dev;`. Эта и другие грабли собраны в разделе 11.

### 4.5 .gitignore

```text
services/auth-service/keys/
*.pem
bin/
```

**Разбор.** Три правила, каждое —security- или hygiene-решение:

- `services/auth-service/keys/` и `*.pem` — **ключи подписи никогда не попадают в git**. Приватный ключ = возможность выпускать токены от имени сервиса; утечка ключа в репозиторий компрометирует всю аутентификацию. В реальном проекте ключи выдаёт секрет-менеджер (Vault, KMS), а для разработки их генерирует скрипт (см. раздел 2.2). Проект работает без них? Нет: main упадёт на `os.ReadFile` с понятной ошибкой — и это правильно, сервис без ключей запускать нельзя.
- `bin/` — результаты `make build`. Бинарники не хранят в git: они собираются из кода, а хранить «код, скомпилированный в другом месте» — путь к рассинхрону.

Обрати внимание, чего **нет** в .gitignore: папок `gen/` и `internal/adapters/postgres/db/`. Сгенерированный код в этом проекте **коммитится** — это осознанное решение: любой, кто склонирует репозиторий, может собрать проект, не ставя buf и sqlc. Платой является необходимость перегенерации при правке контрактов (команда `make gen`).

## 5. api-gateway — каждый файл целиком

Gateway — единственная «дверь» наружу: принимает REST/JSON, проверяет токены и rate limit, и переводит запросы в gRPC-вызовы внутренних сервисов. Бизнес-логики здесь нет **по контракту ТЗ** — это правило стоит держать в голове на каждом файле: если в gateway появляется `if` про клиента — скорее всего, логика попала не туда.

Файлов всего семь, и они образуют конвейер:

```text
HTTP-запрос
  → middleware (recover → requestID → log → cors)      middleware.go
  → роутер (mux)                                        server.go: Routes()
  → [для защищённых роутов] authMW → rateLimitMW        middleware.go
  → хендлер: JSON ↔ proto, вызов gRPC                   server.go: register/login/...
  → ошибка/ответ: grpcError/writeJSON                   helpers.go
```

### 5.1 services/api-gateway/cmd/main.go

```go
// api-gateway — единственная «дверь» снаружи: REST (JSON) → gRPC внутрь.
// main здесь только собирает зависимости по конфигу и запускает сервер:
// конфиг → verifier → limiter → gRPC-клиенты → HTTP-сервер → graceful shutdown.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
	customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
	"mini-core/services/api-gateway/internal/config"
	"mini-core/services/api-gateway/internal/httpapi"
	"mini-core/services/api-gateway/internal/ratelimit"
	"mini-core/services/api-gateway/internal/verifier"
)

// fatal — логируем и выходим явно (os.Exit не выполняет defer'ы!).
func fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}

func main() {
	// Контекст, который отменяется по Ctrl+C / SIGTERM — через него
	// мы узнаём, что пора останавливаться.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config.Load() // ← все настройки в одной структуре

	// Публичный ключ auth-service: им gateway ПРОВЕРЯЕТ подписи токенов локально.
	pubPEM, err := os.ReadFile(cfg.JWTPublicKeyPath)
	if err != nil {
		fatal(log, "public key", err)
	}
	ver, err := verifier.New(pubPEM)
	if err != nil {
		fatal(log, "verifier", err)
	}

	// Rate limit: token bucket в Redis.
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	limiter := ratelimit.New(rdb, cfg.RateLimitCapacity, cfg.RateLimitRefillPerMin/60.0)

	// gRPC-клиенты к внутренним сервисам (insecure — только для локальной разработки).
	authConn, err := grpc.NewClient(cfg.AuthAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(log, "auth conn", err)
	}
	defer authConn.Close()
	custConn, err := grpc.NewClient(cfg.CustomerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(log, "customer conn", err)
	}
	defer custConn.Close()

	h := httpapi.New(log, ver, limiter,
		authv1.NewAuthServiceClient(authConn),
		customerv1.NewCustomerServiceClient(custConn),
	)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown: по сигналу перестаём принимать новые запросы
	// и даём текущим 10 секунд на завершение.
	go func() {
		<-ctx.Done()
		log.Info("graceful shutdown...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	log.Info("api-gateway: http " + cfg.HTTPAddr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal(log, "http", err)
	}
	log.Info("остановлен аккуратно")
}
```

**Разбор.** `main.go` — это **сборочная точка**: единственное место в сервисе, которое знает про всё остальное и соединяет зависимости. Логики тут ноль — только порядок сборки: `конфиг → ключ → verifier → limiter → gRPC-клиенты → HTTP-сервер → graceful shutdown`. Пройдём построчно по смысловым блокам.

**Пакет и импорты (1–26).** `package main` — обязательное имя для исполняемой программы; именно из пакета main с функцией main() собирается бинарник. Импорты разбиты на три группы (стандартная библиотека / внешние пакеты / свои пакеты) — не правило языка, а соглашение, за которым следуют линтеры. Заметь `mini-core/services/...` — свои пакеты резолвятся через go.work (раздел 4.1). Алиасы в импортах (`authv1`, `customerv1`) — сокращения: имя пакета в Go — это последний элемент пути (`v1`), а нам нужны различимые имена.

**`fatal` (29–32).** Функция «залогировать и выйти с кодом 1». Почему не `log.Fatal`? В старом пакете `log` она была, а в современном `log/slog` — **нет** (слог — структурированный логгер, он про форматирование, а не про управление процессом). Пишем сами. Важная деталь: `os.Exit` **не выполняет `defer`'ы** — поэтому fatal вызывают только до того, как появились ресурсы, требующие deferred-очистки, или осознанно жертвуют очисткой (при старте падать можно смело — пока ничего не создано).

**Контекст с сигналами (37–38).** `signal.NotifyContext(context.Background(), SIGINT, SIGTERM)` возвращает контекст, который **отменяется**, когда процессу приходят Ctrl+C (SIGINT) или запрос на завершение от docker/k8s (SIGTERM). Это «канал новостей о смерти процесса»: всё, что слушает этот контекст (сервисы, gRPC-серверы), узнаёт об остановке через `ctx.Err()`. `defer stop()` освобождает внутренние ресурсы подписки, когда main завершится.

**Логгер и конфиг (40–41).** `slog.NewTextHandler(os.Stdout, nil)` — текстовый формат логов (`time=... level=INFO msg=...`), без лишних зависимостей. `config.Load()` собирает **все** настройки в одну структуру один раз (раздел 5.2) — дальше main читает только `cfg.X`, и ни одна строка кода больше не трогает `os.Getenv`.

**Публичный ключ и verifier (43–51).** Gateway **читает файл** публичного ключа auth-service с диска и передаёт байты в `verifier.New`. Это та самая конструкция из твоего вопроса — подробно в 5.3. Здесь важна идея: gateway проверяет подписи **локально**, без похода по сети к auth-service на каждый запрос. Единственное знание gateway про auth — публичный ключ. Раскладка ролей: приватный ключ есть только у auth (подписывает), публичный — у всех (проверяет). Это асимметричная криптография в чистом виде.

**Redis + limiter (53–55).** `redis.NewClient` — **ленивый**: соединения открываются при первой команде, а не сейчас, поэтому проверять его в main не обязательно (упадёт — увидим в первом запросе в логах). `cfg.RateLimitRefillPerMin/60.0` — конфиг задаёт «токенов в минуту», лимитеру нужна скорость «в секунду»: деление здесь, на границе, чтобы limiter не знал о единицах конфига.

**gRPC-клиенты (57–67).** `grpc.NewClient(...)` создаёт **объект-канал**, а не соединение: gRPC устанавливает реальное TCP-соединение лениво, при первом вызове, и сам его переподнимает после разрывов. Поэтому здесь нет Ping. `grpc.WithTransportCredentials(insecure.NewCredentials())` — без TLS; для localhost-разработки норма, для прода — нет (раздел 11). `defer authConn.Close()` — освободить канал при выходе из main (после ListenAndServe вернётся управление).

**Сборка хендлера (69–72).** `httpapi.New(log, ver, limiter, authv1.NewAuthServiceClient(authConn), customerv1.NewCustomerServiceClient(custConn))` — момент, где всё соединяется. `NewAuthServiceClient(conn)` — **сгенерированная** функция (из gen/, раздел 8): она возвращает объект, реализующий интерфейс `AuthServiceClient` с методами Register/Login/Refresh. Gateway не знает, что внутри — канал к gRPC-серверу auth-service; при вызове метода произойдёт сериализация → сеть → вызов → ответ. Инъекция зависимостей руками: все зависимости идут аргументами конструктора, никаких глобальных переменных и синглтонов.

**HTTP-сервер (74–78).** `http.Server` с тремя полями: адрес, хендлер (`h.Routes()` — весь конвейер middleware, раздел 5.7) и `ReadHeaderTimeout: 5 * time.Second`. Последнее — защита от slowloris-атаки (клиент, который открывает соединение и никогда не дочитывает заголовки): без таймаута такие соединения копятся бесконечно и съедают файловые дескрипторы.

**Graceful shutdown (80–88).** Классический паттерн из трёх шагов. (1) Горутина `go func() { <-ctx.Done(); ... }()` ждёт отмены контекста (Ctrl+C); `<-ctx.Done()` — чтение из канала, блокируется, пока канал не закроется. (2) По сигналу — `httpSrv.Shutdown(ctx10s)`: сервер перестаёт принимать **новые** соединения и ждёт до 10 секунд, пока текущие запросы доделаются. (3) Тем временем **основная** горутина всё ещё висит в `ListenAndServe()` — и вот после Shutdown она возвращается с ошибкой `http.ErrServerClosed`, что **не ошибка**, поэтому условие `err != nil && !errors.Is(err, http.ErrServerClosed)`: настоящий сбой слушателя (порт занят) — fatal, штатная остановка — «остановлен аккуратно».

Почему shutdown в горутине, а не после ListenAndServe? Потому что ListenAndServe блокируется и сигнал сам по себе его не прерывает. Горутина + Shutdown — способ «ткнуть» заблокированный вызов снаружи.

### 5.2 services/api-gateway/internal/config/config.go

```go
// Package config — единственное место, где api-gateway «знает про окружение».
// Все настройки собираются в одну структуру Config один раз при старте
// (функция Load), а дальше весь код работает только с этой структурой.
// Источник настроек — переменные окружения; если переменная не задана,
// используется значение по умолчанию, чтобы проект поднимался без конфигов.
package config

import (
        "os"
        "strconv"
)

// Config — все настройки gateway в одном месте.
// Структура заполняется один раз при старте и дальше только читается.
type Config struct {
        // HTTPAddr — адрес, который слушает HTTP-сервер (REST наружу).
        HTTPAddr string
        // AuthAddr — адрес gRPC-сервера auth-service.
        AuthAddr string
        // CustomerAddr — адрес gRPC-сервера customer-service.
        CustomerAddr string
        // RedisAddr — адрес Redis (нужен rate limit'у).
        RedisAddr string
        // JWTPublicKeyPath — путь к публичному ключу auth-service (PEM-файл).
        JWTPublicKeyPath string
        // RateLimitCapacity — «размер ведра»: сколько запросов можно сделать подряд.
        RateLimitCapacity int
        // RateLimitRefillPerMin — сколько токенов возвращается в ведро за минуту.
        RateLimitRefillPerMin float64
}

// Load читает переменные окружения и возвращает готовый Config.
func Load() Config {
        return Config{
                HTTPAddr:              envOr("GATEWAY_ADDR", ":8080"),
                AuthAddr:              envOr("AUTH_ADDR", "localhost:50051"),
                CustomerAddr:          envOr("CUSTOMER_ADDR", "localhost:50052"),
                RedisAddr:             envOr("REDIS_ADDR", "localhost:6379"),
                JWTPublicKeyPath:      envOr("JWT_PUBLIC_KEY_PATH", "../auth-service/keys/public.pem"),
                RateLimitCapacity:     envInt("RL_CAPACITY", 10),
                RateLimitRefillPerMin: envFloat("RL_REFILL_PER_MIN", 5),
        }
}

// envOr возвращает значение переменной окружения или значение по умолчанию.
func envOr(key, def string) string {
        if v := os.Getenv(key); v != "" {
                return v
        }
        return def
}

// envInt — то же, но с разбором целого числа (настройки-лимиты).
func envInt(key string, def int) int {
        if v := os.Getenv(key); v != "" {
                if n, err := strconv.Atoi(v); err == nil {
                        return n
                }
        }
        return def
}

// envFloat — то же, но с разбором дробного числа (скорость пополнения).
func envFloat(key string, def float64) float64 {
        if v := os.Getenv(key); v != "" {
                if f, err := strconv.ParseFloat(v, 64); err == nil {
                        return f
                }
        }
        return def
}
```

**Разбор.** Задача файла — **изоляция окружения**: только здесь читаются переменные среды, весь остальной код получает готовый `Config`. Зачем так? Три причины. (1) Проверяемость: чтобы протестировать код, зависящий от настроек, не нужно настраивать окружение — достаточно передать структуру. (2) Видимость: все настройки проекта перечислены в одном месте — открыл файл, увидел, чем управляется сервис. (3) Безопасность дефолтов: сервис поднимается **без единой переменной окружения** — все значения имеют разумные локальные дефолты.

**Структура Config (15–30).** Обычная структура с экспортируемыми полями (большие буквы = доступны из других пакетов, строчные — приватные). Комментарии над полями — не украшение: это документация настроек, которую IDE показывает при наборе `cfg.`.

**Load (33–43).** Возвращает структуру, собранную из трёх хелперов. Обрати внимание на имена env-переменных: префикс `GATEWAY_`/`AUTH_`/... — способ не столкнуться с чужими переменными в общем окружении. `RL_CAPACITY`/`RL_REFILL_PER_MIN` — параметры rate limit: «ведро на 10 запросов, доливается 5 токенов в минуту» (механика — в 5.4).

**Хелперы envOr/envInt/envFloat (45–71).** `envOr` — «прочитай или дай дефолт»: `os.Getenv` возвращает пустую строку для отсутствующей переменной, поэтому проверка `v != ""`. `envInt`/`envFloat` — то же с разбором числа: `strconv.Atoi` (ASCII to integer) возвращает `(число, ошибка)` — если переменная есть, но не число (`RL_CAPACITY=abc`), мы молча берём дефолт. Спорное решение для прода (тихо съеденная опечатка в настройке), но для учебного проекта — минимизирует возню при старте. `err == nil` — идиома Go: функция возвращает ошибку значением, и мы её проверяем явно; никакого throw.

Что важно понимать инженерно: **Config иммутабелен после Load**. Ни один кусок кода не может «поменять настройку на лету» — конфиг читается один раз при старте, дальше живёт как обычная структура. Перезагрузка конфига без рестарта — отдельная фича, её тут сознательно нет.

### 5.3 services/api-gateway/internal/verifier/verifier.go — полный разбор

Это файл из твоего вопроса. Разбор подробнее остальных — как образец того, как читать любой Go-код.

```go
package verifier

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// Claims — что gateway достаёт из проверенного access-токена.
type Claims struct {
	UserID string
	Role   string
}

// Verifier проверяет подпись access-токенов ПУБЛИЧНЫМ ключом auth-service.
type Verifier struct {
	key    jwt.Keyfunc
	parser *jwt.Parser
}

func New(publicPEM []byte) (*Verifier, error) {
	pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
	if err != nil {
		return nil, fmt.Errorf("public key: %w", err)
	}
	return &Verifier{
		key:    func(*jwt.Token) (interface{}, error) { return pub, nil },
		parser: jwt.NewParser(jwt.WithValidMethods([]string{"RS256"})),
	}, nil
}

// Verify — подпись и срок живы? Тогда claims. Иначе ошибка.
func (v *Verifier) Verify(tokenStr string) (*Claims, error) {
	mc := jwt.MapClaims{}
	if _, err := v.parser.ParseWithClaims(tokenStr, mc, v.key); err != nil {
		return nil, err
	}
	sub, _ := mc["sub"].(string)
	role, _ := mc["role"].(string)
	if sub == "" {
		return nil, fmt.Errorf("нет sub в токене")
	}
	return &Claims{UserID: sub, Role: role}, nil
}
```

**Сначала — картину целиком: кто кто подписывает, кто проверяет.**

В системе ровно два места, работающих с подписью JWT:

| Место | Файл | Ключ | Операция | Когда вызывается |
|---|---|---|---|---|
| auth-service | `jwtauth/issuer.go` | **приватный** | **создаёт** подпись (`SignedString`) | при login/refresh |
| api-gateway | `verifier/verifier.go` | **публичный** | **проверяет** чужую подпись | на каждый запрос с токеном |

Проверка **никогда** не создаёт новой подписи — ни здесь, ни где-либо в gateway. Gateway ничего не «перевыдаёт»: он читает токен, убеждается математикой RSA, что подпись поставлена держателем приватного ключа (то есть auth-service) и что токен не просрочен, и достаёт из полезной нагрузки `user_id` и `role`. Всё. Никакой сети, никакой связи с auth-service в момент проверки.

**Теперь построчно.**

```go
package verifier
```
Имя пакета — последняя папка. Пакет — единица инкапсуляции: всё, что не экспортируется (с маленькой буквы), видно только внутри.

```go
// Claims — что gateway достаёт из проверенного access-токена.
type Claims struct {
        UserID string
        Role   string
}
```
Своё имя для claims: gateway не хочет зависеть от деталей формата JWT (`MapClaims` — просто map[string]interface{}). Во всём остальном коде gateway ходит тип `*verifier.Claims` с читаемыми полями `UserID`/`Role`. Это «перевод» внешнего формата в свой словарь — тот же приём, что и в остальных адаптерах проекта.

```go
type Verifier struct {
        key    jwt.Keyfunc
        parser *jwt.Parser
}
```
Два поля. `jwt.Keyfunc` — это **тип-функция** из библиотеки: `func(*jwt.Token) (interface{}, error)`. То есть в поле `key` лежит не ключ, а **функция, которая умеет отдать ключ**. `parser` — настроенный экземпляр парсера JWT.

```go
func New(publicPEM []byte) (*Verifier, error) {
```
Конструктор. Соглашение Go: функцию, создающую объект, называют `NewXxx` и возвращают `(*T, error)`. `publicPEM []byte` — **сырые байты** файла ключа (их читает main через `os.ReadFile` — раздел 5.1, строка 44). PEM — это текстовый формат-обёртка: `-----BEGIN PUBLIC KEY-----` + base64 + `-----END PUBLIC KEY-----`. Байты файла ≠ ключ; это как бумага с рецептом ≠ лекарство.

```go
        pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
        if err != nil {
                return nil, fmt.Errorf("public key: %w", err)
        }
```
**Первая настоящая работа `New`:** распарсить байты PEM в объект ключа `*rsa.PublicKey` — структуру с двумя большими числами (модуль N и экспонента E). После этой строки байты больше не нужны, в памяти живёт готовый математический объект. `fmt.Errorf("public key: %w", err)` — оборачивание ошибки: `%w` сохраняет исходную ошибку внутри новой (по ней потом работает `errors.Is/As`), а сообщение дополняет контекстом («ошибка была с публичным ключом»). При неудаче возвращаем `nil, err` — нулевое значение указателя и ошибку; вызователь (main) обязан это проверить.

```go
        return &Verifier{
                key:    func(*jwt.Token) (interface{}, error) { return pub, nil },
                parser: jwt.NewParser(jwt.WithValidMethods([]string{"RS256"})),
        }, nil
}
```
**Вторая и последняя работа `New` — настроить два поля.** Здесь ничего не проверяется и не подписывается — только собирается «инструмент».

Поле `key`: литерал функции (анонимная функция), которая **игнорирует** свой аргумент `*jwt.Token` (аргумент даже безымянный: `func(*jwt.Token)`) и всегда возвращает один и тот же `pub`. Зачем библиотеке вообще функция, а не просто ключ? Потому что в общем случае ключ для проверки зависит **от токена**: в заголовке токена может лежать `kid` (идентификатор ключа), и настоящая реализация ходит по `kid` в JWKS-хранилище и возвращает нужный ключ из многих. Библиотека спроектирована под эту гибкость. У нас ключ один, поэтому функция-константа. Обрати внимание на **замыкание** (closure): анонимная функция «захватила» переменную `pub` из внешней области — она будет помнить её всегда, даже после выхода из `New`. `interface{}` — то же, что `any`: «значение любого типа»; интерфейс `jwt.Keyfunc` требует `interface{}`, потому что бывают и RSA-, и ECDSA-, и HMAC-ключи.

Поле `parser`: `jwt.NewParser(...)` создаёт парсер с опциями. Опция `jwt.WithValidMethods([]string{"RS256"})` — **белый список алгоритмов**. Критично понять, зачем: заголовок JWT сам объявляет свой алгоритм (`"alg": "..."`). Если библиотеке верить на слово, злоумышленник шлёт токен с `"alg": "none"` (подписи нет вообще) или с HS256 иPublic ключом в роли секрета — знаменитые атаки «algorithm confusion», на которых падали реальные продакшены. С `WithValidMethods` парсер смотрит в заголовок **до** всякой проверки и отшивает всё, что не RS256. Это одна строка, но именно она отделяет «просто работающий JWT» от «безопасно работающий JWT».

Итого `New` — это **заряжание**: PEM → объект ключа, ключ → функция-поставщик, парсер → с белым списком. Вызывается **один раз** при старте gateway (main, строка 48). Ошибка здесь — повод не стартовать вообще (без ключа gateway бесполезен).

**Метод Verify — а вот тут происходит проверка.**

```go
func (v *Verifier) Verify(tokenStr string) (*Claims, error) {
```
`func (v *Verifier)` — метод: `v` — получатель (receiver), «сам объект». Вызывается из middleware `authMW` на **каждом** запросе к защищённым роутам (middleware.go, строка 97): `s.verifier.Verify(strings.TrimPrefix(h, prefix))`.

```go
        mc := jwt.MapClaims{}
        if _, err := v.parser.ParseWithClaims(tokenStr, mc, v.key); err != nil {
                return nil, err
        }
```
`MapClaims` — map[string]interface{}: «просто JSON-объект из тела токена». `ParseWithClaims` — рабочая лошадь; внутри неё последовательно:

1. строка токена `header.payload.signature` (base64url-куски через точку) разбивается на три части;
2. заголовок декодируется; проверяется `alg` — он должен быть в белом списке RS256 (наша опция из New);
3. вызывается наша `key`-функция → получаем `pub`;
4. **математика**: подпись в третьей части сверяется с RSA-подписью первых двух частей публичным ключом. Подделать подпись без приватного ключа нельзя — это не «сложно», это вычислительно невозможно;
5. проверяются стандартные временные claims: `exp` (истёк?) — библиотека jwt/v5 валидирует его автоматически; если токен просрочен — именно здесь выйдет ошибка;
6. декодированный payload кладётся в `mc`.

Первое возвращаемое значение (`*jwt.Token`) нам не нужно — отсюда `_`. Если хоть один шаг упал — возвращаем ошибку наверх; middleware превратит её в HTTP 401 (раздел 5.6). Никаких «новых подписей» тут не рождается — токен остаётся ровно тем, каким пришёл.

```go
        sub, _ := mc["sub"].(string)
        role, _ := mc["role"].(string)
```
**Type assertion с ok-формой.** `mc["sub"]` имеет тип `interface{}` (в map лежит «что-то»). Запись `x.(string)` говорит: «поверь мне, это строка». Но если это НЕ строка (или ключа нет вовсе), голая форма запаникует. Форма с двумя значениями `v, ok := x.(string)` не паникует: при неудаче `ok = false`, а `v` — нулевое значение типа (пустая строка). Мы `ok` игнорируем (`_`), потому что следующей строкой проверяем результат по-другому:

```go
        if sub == "" {
                return nil, fmt.Errorf("нет sub в токене")
        }
```
Если `sub` отсутствовал или был не строкой — он пуст. Пустой user_id нам не годится: весь downstream строится на том, что claims идентифицируют пользователя. Заметь: `role` так не проверяется — токен без роли валиден, роль просто будет пустой (RBAC появится позже; gateway роль ни на что не проверяет и в этой итерации не обязан).

```go
        return &Claims{UserID: sub, Role: role}, nil
```
Собрали свой тип — вернули. Дальше middleware кладёт claims в контекст запроса (`withClaims`), а хендлеры достают (`claimsFromCtx`) — о том, как значения путешествуют через контекст, см. 5.5.

**Резюме-мнемоника для этого файла:**
- `New` = зарядить пистолет (один раз, при старте): распарсить ключ, завернуть в функцию, настроить парсер с whitelist'ом.
- `Verify` = выстрелить (каждый запрос): разобрать токен, проверить алгоритм → подпись → срок, достать claims.
- Ни один путь в этом файле не создаёт подпись. Подпись в системе рождается ровно в одном месте — `Issuer.IssueAccess` (раздел 6.12), приватным ключом.

### 5.4 services/api-gateway/internal/ratelimit/limiter.go

```go
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// bucketScript — атомарный token bucket.
// KEYS[1] — ключ; ARGV: capacity, refill/сек, now_ms, cost.
var bucketScript = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
local ts     = tonumber(redis.call('HGET', KEYS[1], 'ts'))
if tokens == nil then
    tokens, ts = capacity, now
end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * refill)
local allowed = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 3600000)
return allowed
`)

type Limiter struct {
	rdb      *redis.Client
	capacity int
	refill   float64
}

func New(rdb *redis.Client, capacity int, refillPerSec float64) *Limiter {
	return &Limiter{rdb: rdb, capacity: capacity, refill: refillPerSec}
}

// Allow расходует один токен. false = лимит исчерпан.
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	res, err := bucketScript.Run(ctx, l.rdb, []string{"rl:" + key},
		l.capacity, l.refill, time.Now().UnixMilli(), 1).Int()
	if err != nil {
		return false, fmt.Errorf("redis: %w", err)
	}
	return res == 1, nil
}
```

**Разбор.** Rate limit — защита от перегрузки и перебора: «не больше N запросов от одного клиента». Здесь реализован **token bucket** («ведро с токенами»): у каждого клиента своё ведро; каждый запрос вынимает один токен; пустое ведро → 429; ведро само доливается с постоянной скоростью. Почему ведро, а не «N в минуту»? Ведро пропускает короткие всплески (человек открыл страницу с 8 параллельными запросами), но не даёт держать постоянную скорость выше разрешённой — это компромисс «мягко на пиках, строго в среднем».

**Lua-скрипт (13–33).** Вся логика ведра — один скрипт на языке Lua, выполняемый **внутри Redis**. Почему логика не в Go? Атомарность: Redis исполняет скрипт целиком, между его шагами никто не вклинится. Представь вариант без скрипта: Go читает `tokens=1`, решает «можно», вычитает, пишет. Между «читает» и «пишет» другой запрос успел прочитать те же `tokens=1` — и оба запроса прошли, лимит дырявый. Атомарный скрипт закрывает проблему начисто.

`redis.NewScript(...)` оборачивает текст скрипта: при первом запуске Redis получит текст (EVAL), при следующих — хеш скрипта (EVALSHA), короче трафик. По строчкам:

1. `tonumber(ARGV[...])` — аргументы приходят строками, приводим к числам: capacity (размер ведра), refill (скорость долива в секунду), now (текущее время в миллисекундах, передаёт Go), cost (сколько токенов списать за запрос).
2. `HGET KEYS[1] 'tokens'` — читаем два поля **хеша** (один Redis-ключ `rl:<user>`, в нём поля tokens и ts). `nil` → клиент новый, ведро полное: `tokens, ts = capacity, now`.
3. `elapsed = (now - ts) / 1000` — сколько секунд прошло с прошлого запроса; `tokens = min(capacity, tokens + elapsed * refill)` — **долив**: за простой накопилось, но не больше размера ведра. Вот почему «ведро»: простой копится в запас, всплеск его расходует.
4. `if tokens >= cost` — хватает? Вычесть и `allowed = 1`. Иначе остаёмся с `allowed = 0` и НЕ списываем (не было денег — не списали).
5. `HSET ... 'tokens', tokens, 'ts', now` — записать новое состояние; `PEXPIRE KEYS[1] 3600000` — ключ живёт час: неактивные клиенты не копятся в Redis вечно.
6. `return allowed` — 1 или 0.

Важно: время передаёт **Go** (`time.Now().UnixMilli()`), не берёт из Redis — так проще и предсказуемее для одного инстанса.

**Limiter (35–43).** Структура хранит клиент Redis и два параметра. `New` — стандартный конструктор. Ничего не делает при создании (кроме запоминания) — «ленивая» зависимость, как и `redis.NewClient` в main.

**Allow (45–53).** Метод, который зовёт middleware. `bucketScript.Run(ctx, l.rdb, []string{"rl:" + key}, ...)` — запускает скрипт: `ctx` (отмена/таймаут), куда (rdb), KEYS (срез из одного ключа; префикс `rl:` — namespace, чтобы не столкнуться с чужими ключами), ARGV (четыре аргумента). `.Int()` — приведение результата к Go-типу. Ошибка Redis → `fmt.Errorf("redis: %w", err)` с обёрткой — а решение, что делать при недоступном Redis, принимает **вызывающий** (middleware): это fail-open, раздел 5.6.

Инженерная точка: лимитёр не знает про HTTP, user_id и IP — он принимает произвольную строку-ключ. Это делает его переиспользуемым и тестируемым: ключ формирует middleware.

### 5.5 services/api-gateway/internal/httpapi/helpers.go

```go
package httpapi

import (
	"context"
	"encoding/json"
	"mini-core/services/api-gateway/internal/verifier"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ctxKey int

type Claims = verifier.Claims

const (
	ctxKeyClaims ctxKey = iota
	ctxKeyRequestID
)

func withClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, ctxKeyClaims, c)
}

func claimsFromCtx(ctx context.Context) *Claims {
	c, _ := ctx.Value(ctxKeyClaims).(*Claims)
	return c
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

func requestIDFromCtx(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyRequestID).(string)
	return id
}

func writeJSON(w http.ResponseWriter, httpStatus int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(v)
}

// errBody — единый формат ошибок наружу (требование ТЗ).
type errBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Details   string `json:"details,omitempty"`
	RequestID string `json:"request_id"`
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, httpStatus int, code, msg string) {
	writeJSON(w, httpStatus, errBody{
		Code:      code,
		Message:   msg,
		RequestID: requestIDFromCtx(r.Context()),
	})
}

// httpFromGRPC — перевод кодов gRPC в HTTP-статусы.
func httpFromGRPC(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return 400
	case codes.Unauthenticated:
		return 401
	case codes.PermissionDenied:
		return 403
	case codes.NotFound:
		return 404
	case codes.AlreadyExists:
		return 409
	case codes.FailedPrecondition:
		return 412
	case codes.ResourceExhausted:
		return 429
	case codes.Unavailable:
		return 503
	default:
		return 500
	}
}

// grpcError — ошибка от gRPC-вызова → единый JSON.
func (s *Server) grpcError(w http.ResponseWriter, r *http.Request, err error) {
	st, ok := status.FromError(err)
	if !ok {
		s.writeError(w, r, http.StatusBadGateway, "UPSTREAM", "сервис недоступен")
		return
	}
	s.writeError(w, r, httpFromGRPC(st.Code()), st.Code().String(), st.Message())
}
```

**Разбор.** Файл «мелочей», на которых держится пакет. Разберём по блокам.

**Контекстные ключи (13–20).** `context.Context` — способ пронести значения вдоль цепочки вызовов внутри одного запроса (claims, request-id, таймауты). Хранит он пары `ключ → значение`, где ключ — `interface{}`. Если использовать в качестве ключа строку `"claims"` — любой чужой пакет может использовать ту же строку, и значения столкнутся. Поэтому заводят **свой неповторимый тип** `type ctxKey int` и константы через `iota` (автоинкремент: 0, 1): два ключа гарантированно разные, потому что их тип уникален для пакета.

```go
type Claims = verifier.Claims
```
**Алиас типа** (знак `=`): не новый тип, а второе имя. httpapi пишет `*Claims`, компилятор видит `*verifier.Claims`. Сокращение импортного имени без создания своей сущности.

**withClaims/claimsFromCtx (22–29).** Пара «положить в контекст / достать из контекста». `context.WithValue(ctx, key, val)` возвращает **новый** контекст с добавленной парой (контекст иммутабелен). При доставании — та же ok-форма type assertion, что в 5.3: если claims в контексте нет (роут публичный, authMW не выполнялся) — вернётся nil, и это норма: `rateLimitMW` проверяет `if c := claimsFromCtx(...); c != nil`. Пара with/From — стандартный паттерн; прямой вызов `ctx.Value` с сырым ключом по всему коду — плохой стиль, потому что типы не проверяются.

**writeJSON (40–44).** Три строки, повторяющиеся в каждом хендлере, вынесены в функцию: Content-Type, WriteHeader (отправляет статус-строку), кодирование структуры в JSON прямо в writer. `_ = json.NewEncoder(w).Encode(v)` — ошибка кодирования **осознанно** игнорируется: если клиент уже отключился, писать нечем и некому, а ответить чем-то другим мы всё равно уже не можем (статус отправлен). Игнор ошибки — всегда решение, а не лень; здесь оно честное.

**errBody (46–52).** Единый формат ошибок наружу: `code` — машинный (gRPC-строка), `message` — человеческий, `request_id` — чтобы по логам найти именно этот запрос, `details` — опционально. Теги в обратных кавычках — **struct tags**: метаданные для json-пакета. `json:"details,omitempty"` — поле называется `details`, и если оно пустое — вообще не попадает в JSON. Тег — не комментарий, а читаемая рефлексией строка.

**writeError (54–60).** Метод-обёртка: собрать errBody с request_id из контекста и отправить. Почему метод, а не функция? Исторически — чтобы иметь доступ к s.log при расширении; сейчас s не используется. Мелочь.

**httpFromGRPC (62–84).** Таблица-переводчик: gRPC-код → HTTP-статус. `switch c { case codes.InvalidArgument: return 400 ... }` — обыкновенный switch, но смысл глубже: **два мира говорят на разных языках**, и нужна зафиксированная карта соответствий. Клиенту важно не «что сломалось внутри», а «что делать»: 429 — притормози и повтори позже, 401 — освежи токен, 409 — повтори с другими данными. Полная таблица с доменными ошибками — в разделе 10.

**grpcError (86–94).** Разбор ошибки от gRPC-вызова. `status.FromError(err)` возвращает `(status, ok)`: gRPC-ошибки — это `*status.Status` в обёртке; если ok=false — ошибка **не** от gRPC (сеть легла, сервис недоступен, dial failed). Тогда честный 502 BAD GATEWAY с кодом UPSTREAM: «виноват не ты, внутренний сервис». Если ok — берём код, переводим в HTTP и отдаём сообщение сервиса. Один канал ошибок на все хендлеры: каждый хендлер делает `if err != nil { s.grpcError(...) ; return }` — и формат ошибок везде одинаков.

### 5.6 services/api-gateway/internal/httpapi/middleware.go

```go
package httpapi

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Middleware — обёртка над хендлером.
type Middleware func(http.Handler) http.Handler

// Chain наворачивает в порядке перечисления: первый — самый внешний.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// recoverMW — паника в хендлере не роняет весь gateway (500 вместо смерти).
func (s *Server) recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic", "err", p, "path", r.URL.Path)
				s.writeError(w, r, http.StatusInternalServerError, "INTERNAL", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// requestIDMW — берём X-Request-ID или генерим; отдаём в заголовке ответа.
func (s *Server) requestIDMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", rid)
		next.ServeHTTP(w, r.WithContext(withRequestID(r.Context(), rid)))
	})
}

// statusRecorder — запоминает статус ответа для access-лога.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// logMW — access-лог: метод, путь, статус, длительность, request-id.
func (s *Server) logMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("access",
			"method", r.Method, "path", r.URL.Path,
			"status", rec.status, "dur", time.Since(start).Round(time.Microsecond),
			"request_id", requestIDFromCtx(r.Context()),
		)
	})
}

// corsMW — для разработки разрешаем всё; в проде так нельзя (долговая книжка).
func (s *Server) corsMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authMW — «Bearer <token>» → проверка подписи → claims в контекст.
func (s *Server) authMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, prefix) {
			s.writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "нужен Authorization: Bearer <token>")
			return
		}
		claims, err := s.verifier.Verify(strings.TrimPrefix(h, prefix))
		if err != nil {
			s.writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "токен невалиден или истёк")
			return
		}
		next.ServeHTTP(w, r.WithContext(withClaims(r.Context(), claims)))
	})
}

// rateLimitMW — лимит на user_id (если уже авторизован), иначе на IP.
func (s *Server) rateLimitMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, _, err := net.SplitHostPort(r.RemoteAddr) // "[::1]:54321" → "[::1]"
		if err != nil {
			key = r.RemoteAddr // на всякий случай
		}
		if c := claimsFromCtx(r.Context()); c != nil {
			key = c.UserID // авторизованных считаем по user_id, не по IP
		}
		ok, err := s.limiter.Allow(r.Context(), key)
		if err != nil {
			// fail-open: Redis упал — пропускаем, но орём в лог
			s.log.Error("rate limit недоступен, пропускаю (fail-open)", "err", err)
			next.ServeHTTP(w, r)
			return
		}
		if !ok {
			s.writeError(w, r, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "слишком много запросов, притормози")
			return
		}
		next.ServeHTTP(w, r)
	})
}
```

**Разбор.** Middleware — обёртка над хендлером: получает `http.Handler`, делает что-то **до** и/или **после**, и передаёт управление дальше через `next.ServeHTTP(w, r)`. Это луковица: запрос проходит слои снаружи внутрь, ответ — обратно.

```go
type Middleware func(http.Handler) http.Handler
```
Тип: функция «из хендлера в хендлер». Всё остальное — частные случаи.

```go
func Chain(h http.Handler, mws ...Middleware) http.Handler {
        for i := len(mws) - 1; i >= 0; i-- {
                h = mws[i](h)
        }
        return h
}
```
`Chain` наворачивает обёртки. `...mws` — вариативный параметр (в функции это срез). Цикл идёт **с конца**: последний элемент оборачивается первым и оказывается самым внутренним; первый в списке — самый внешний. `Chain(mux, recover, requestID, log, cors)` читается как порядок прохождения запроса: recover → requestID → log → cors → mux.

**recoverMW (24–34).** Паника в Go — не исключение, которое ловят повсеместно: это ошибка программиста (обращение к nil-указателю, выход за границы среза), и её не ловят обычно. **Но** в HTTP-сервере паника в одном хендлере не должна убивать процесс со всеми остальными запросами. `defer func() { if p := recover(); p != nil { ... } }()` — идиома: deferred-функция сработает при раскрутке паники, `recover()` вернёт значение паники и остановит её. Логируем и отвечаем 500. Процесс жив.

**requestIDMW (37–46).** Каждый запрос получает идентификатор: либо из заголовка `X-Request-ID` (принесли балансировщики/клиент — сквозная трассировка), либо `uuid.NewString()`. Отдаётся в ответ (клиент может цитировать его в багрепорте) и кладётся в контекст — дальше логи и ошибки ссылаются на него. Одна строка кода, а решает проблему «найди этот запрос в миллионе строк логов».

**statusRecorder + logMW (48–71).** Чтобы залогировать статус ответа, его нужно откуда-то взять — стандартный ResponseWriter код не возвращает. Решение: **встраивание** (embedding) —

```go
type statusRecorder struct {
        http.ResponseWriter
        status int
}
```
statusRecorder «наследует» все методы ResponseWriter (промоушен методов) и **переопределяет** только `WriteHeader`, запоминая код. Поэтому его можно подсунуть вместо оригинального writer'а — все остальные вызовы прозрачно пройдут дальше. В logMW: запомнить `start := time.Now()`, обернуть writer, вызвать `next`, после — посчитать `time.Since(start)` и записать строку access-лога (метод, путь, статус, длительность, request_id). Обрати внимание: лог пишется **после** next — так мы уже знаем статус и длительность.

**corsMW (74–86).** CORS — браузерная политика: JS с чужого домена не может читать ответы без разрешения. Для разработки разрешаем всем (`*`) и перечисляем заголовки. Отдельная ветка `if r.Method == http.MethodOptions` — **preflight**: браузер перед «опасным» запросом присылает OPTIONS, на который надо ответить 204 с заголовками и **не** передавать запрос дальше. В проде `*` так нельзя: заменяем на список доменов.

**authMW (89–104).** Сердце авторизации gateway. Пошагово: взять заголовок `Authorization`; `strings.HasPrefix(h, prefix)` — начинается ли с `Bearer `; нет — сразу 401 UNAUTHENTICATED. Да — `strings.TrimPrefix(h, prefix)` отрезает префикс и остаётся сам токен; `s.verifier.Verify(token)` — тот самый метод из 5.3. Ошибка → 401 «токен невалиден или истёк» (неважно, какая именно причина — наружу не утекаем). Успех → `r.WithContext(withClaims(...))`: **копия** запроса с обогащённым контекстом уходит в next. Важно: authMW кладёт в контекст **проверенные** claims — все последующие слои могут им доверять.

**rateLimitMW (107–129).** Ключ лимита: по умолчанию IP (`net.SplitHostPort` разбирает `"192.168.1.5:54321"` на хост и порт — RemoteAddr всегда с портом; ошибка → берём как есть). Но если в контексте есть claims (authMW отработал раньше!) — ключ `c.UserID`: считаем пользователя, а не его IP (за NAT сидят тысячи людей на одном IP). Отсюда важность **порядка**: `Chain(handler, s.authMW, s.rateLimitMW)` — сначала auth, потом лимит; наоборот — лимит всегда по IP.

`ok, err := s.limiter.Allow(...)`. При **ошибке** (Redis лёг) — fail-open: лог и пропуск запроса. Осознанный выбор: упавший Redis не валит весь API; анти-абьюз важнее доступности, но в учебном проекте выбор сделан в пользу доступности. При `!ok` — 429 RESOURCE_EXHAUSTED. Иначе — next.

### 5.7 services/api-gateway/internal/httpapi/server.go

```go
package httpapi

import (
        "context"
        "encoding/json"
        "log/slog"
        "net/http"

        "google.golang.org/grpc/metadata"

        authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
        customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
        "mini-core/services/api-gateway/internal/ratelimit"
        "mini-core/services/api-gateway/internal/verifier"
)

// Server — только перевод форматов. Бизнес-логики здесь нет (ТЗ запрещает).
type Server struct {
        log       *slog.Logger
        verifier  *verifier.Verifier
        limiter   *ratelimit.Limiter
        auth      authv1.AuthServiceClient
        customers customerv1.CustomerServiceClient
}

func New(log *slog.Logger, v *verifier.Verifier, l *ratelimit.Limiter,
        auth authv1.AuthServiceClient, customers customerv1.CustomerServiceClient) *Server {
        return &Server{log: log, verifier: v, limiter: l, auth: auth, customers: customers}
}

// Routes — вся маршрутизация. Go 1.22+ умеет паттерны «МЕТОД /путь» в штатном mux.
func (s *Server) Routes() http.Handler {
        mux := http.NewServeMux()

        // публичные: лимит по IP
        mux.Handle("POST /auth/register", Chain(http.HandlerFunc(s.register), s.rateLimitMW))
        mux.Handle("POST /auth/login", Chain(http.HandlerFunc(s.login), s.rateLimitMW))
        mux.Handle("POST /auth/refresh", Chain(http.HandlerFunc(s.refresh), s.rateLimitMW))

        // защищённые: сначала auth (claims в контекст), потом лимит по user_id
        mux.Handle("GET /customers/me", Chain(http.HandlerFunc(s.getMe), s.authMW, s.rateLimitMW))
        mux.Handle("PUT /customers/me", Chain(http.HandlerFunc(s.updateMe), s.authMW, s.rateLimitMW))

        // общий конвейер вокруг всего
        return Chain(mux, s.recoverMW, s.requestIDMW, s.logMW, s.corsMW)
}

// outgoing — контекст для gRPC: прокидываем request-id и claims в metadata.
func (s *Server) outgoing(r *http.Request) context.Context {
        pairs := []string{"x-request-id", requestIDFromCtx(r.Context())}
        if c := claimsFromCtx(r.Context()); c != nil {
                pairs = append(pairs, "x-user-id", c.UserID, "x-user-role", c.Role)
        }
        return metadata.NewOutgoingContext(r.Context(), metadata.Pairs(pairs...))
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
        var in struct {
                Email    string `json:"email"`
                Password string `json:"password"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        resp, err := s.auth.Register(s.outgoing(r), &authv1.RegisterRequest{Email: in.Email, Password: in.Password})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
        var in struct {
                Email    string `json:"email"`
                Password string `json:"password"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        resp, err := s.auth.Login(s.outgoing(r), &authv1.LoginRequest{Email: in.Email, Password: in.Password})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
        var in struct {
                RefreshToken string `json:"refresh_token"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        resp, err := s.auth.Refresh(s.outgoing(r), &authv1.RefreshRequest{RefreshToken: in.RefreshToken})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getMe(w http.ResponseWriter, r *http.Request) {
        c := claimsFromCtx(r.Context()) // user_id из ПРОВЕРЕННОГО токена, не из запроса!
        resp, err := s.customers.GetCustomer(s.outgoing(r), &customerv1.GetCustomerRequest{UserId: c.UserID})
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}

func (s *Server) updateMe(w http.ResponseWriter, r *http.Request) {
        var in struct {
                FirstName   string `json:"first_name"`
                LastName    string `json:"last_name"`
                Phone       string `json:"phone"`
                DateOfBirth string `json:"date_of_birth"`
                Address     string `json:"address"`
                Citizenship string `json:"citizenship"`
        }
        if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
                s.writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", "некорректный JSON")
                return
        }
        c := claimsFromCtx(r.Context())
        resp, err := s.customers.UpdateProfile(s.outgoing(r), &customerv1.UpdateProfileRequest{
                UserId:    c.UserID,
                FirstName: in.FirstName, LastName: in.LastName, Phone: in.Phone,
                DateOfBirth: in.DateOfBirth, Address: in.Address, Citizenship: in.Citizenship,
        })
        if err != nil {
                s.grpcError(w, r, err)
                return
        }
        writeJSON(w, http.StatusOK, resp)
}
```

**Разбор.** Это «тело» gateway: маршруты и пять хендлеров. И ни в одном хендлере нет бизнес-логики — только перевод форматов.

**Server (18–29).** Пять зависимостей: логгер, verifier, limiter и два **сгенерированных клиента** (`auth authv1.AuthServiceClient`, `customers customerv1.CustomerServiceClient`). Обрати внимание на типы: это **интерфейсы из gen/** (раздел 8.3). Gateway не знает ни про gRPC-каналы, ни про сеть — он зовёт методы интерфейса. `New` просто раскладывает аргументы по полям; поля с маленькой буквы — приватные, снаружи недоступны: всё общение с Server через методы.

**Routes (32–46).** `http.NewServeMux()` — штатный роутер. Go 1.22+ понимает паттерны `«МЕТОД /путь»`: `mux.Handle("POST /auth/register", ...)` — только POST на этот путь; на GET вернётся 405. Три группы:

- публичные (`register/login/refresh`): `Chain(http.HandlerFunc(s.register), s.rateLimitMW)` — конвейер из **двух** слоёв: хендлер + лимит (по IP, claims ещё нет).
- защищённые (`getMe/updateMe`): `Chain(handler, s.authMW, s.rateLimitMW)` — три слоя: сначала auth (положит claims в контекст), потом лимит (уже по user_id), потом хендлер. **Порядок в Chain = порядок исполнения.**
- вокруг всего: `Chain(mux, s.recoverMW, s.requestIDMW, s.logMW, s.corsMW)` — общий конвейер.

`http.HandlerFunc(s.register)` — адаптер: превращает функцию `func(w, r)` в `http.Handler` (у HandlerFunc есть метод ServeHTTP). Стандартная Go-магия, чтобы смешивать функции и хендлеры.

**outgoing (49–55).** Мост в gRPC-мир: контекст HTTP-запроса обогащаем **metadata** — gRPC-эквивалент HTTP-заголовков. `metadata.Pairs(pairs...)` собирает пары ключ-значение из вариативного списка (`pairs...` — разворот среза в аргументы). Прокидываем `x-request-id` всегда (сквозная трассировка до логов auth/customer), а `x-user-id`/`x-user-role` — только если есть claims. Кто-то внизstream может их прочитать (в этом проекте — пока никто, но контекст трассировки в проде читают все).

**Хендлеры (57–141).** Все пять сделаны по одному лекалу — разберём `register` и `updateMe`, остальное аналогично.

register: анонимная структура с JSON-тегами —

```go
var in struct {
        Email    string `json:"email"`
        Password string `json:"password"`
}
```
Структуру не нужно нигде больше объявлять — она существует только здесь (в Go структуры можно объявлять инлайн). `json.NewDecoder(r.Body).Decode(&in)` — читаем тело запроса прямо в структуру; `&in` — указатель, чтобы Decode мог заполнить поля. Ошибка декодирования (битый JSON) → 400 INVALID_ARGUMENT.

Дальше — вызов gRPC: `s.auth.Register(s.outgoing(r), &authv1.RegisterRequest{Email: in.Email, Password: in.Password})`. Смотрим внимательно: `RegisterRequest` — **сгенерированная** структура из gen/ (раздел 8); мы создаём её литералом с именованными полями (в Go литералы структур требуют именованных полей — позиционные запрещены линтером и опасны при изменении структуры). Ответ — `resp *authv1.RegisterResponse`. Ошибка → `s.grpcError` (5.5). Успех → `writeJSON(w, http.StatusCreated, resp)` — 201 Created, тело — та же proto-структура, закодированная json'ом (генератор добавляет json-теги к полям, поэтому `user_id`, а не `UserId`).

getMe — самая важная пара строк проекта с точки зрения безопасности:

```go
c := claimsFromCtx(r.Context()) // user_id из ПРОВЕРЕННОГО токена, не из запроса!
resp, err := s.customers.GetCustomer(s.outgoing(r), &customerv1.GetCustomerRequest{UserId: c.UserID})
```
Клиент **не указывает** user_id вообще — его берут из claims, а claims попали в контекст только после проверки подписи в authMW. Никто не может посмотреть чужой профиль, подставив чужой id: id берётся не из пользовательского ввода. Это правило «identity from token, not from request» — фундамент всех API такого рода.

updateMe: те же лекалы + шесть полей анкеты, передающихся в `UpdateProfileRequest`. Заметь: `DateOfBirth string` — строка `YYYY-MM-DD`; gateway ничего не валидирует в ней (валидация — забота customer-service).

**Итого по gateway.** Семь файлов: main собирает, config изолирует окружение, verifier проверяет подписи локально, limiter защищает от перегрузки, helpers унифицируют ответы, middleware образуют конвейер, server маршрутизирует и переводит JSON↔gRPC. Ни один из них не знает про банковские правила — за ними отправляемся в auth-service.

## 6. auth-service — каждый файл целиком

auth-service — владелец учётных записей и токенов. Внутри та же гексагональная раскладка, что и в gateway, но с двумя новыми слоями: **domain** (правила предметной области) и **app** (сценарии). Сначала посмотрим на слои, потом пойдём по файлам в порядке «контракт → база → домен → сценарии → адаптеры → main»:

```text
proto/*.proto  → контракт (что умеет сервис)
migrations/    → схема БД (как лежат данные)
queries/       → SQL-запросы (источник правды для sqlc)
internal/domain → сущности и правила (не знает про БД, gRPC, Kafka)
internal/app   → сценарии (Register/Login/Refresh) через порты-интерфейсы
internal/adapters → реализации портов: Postgres, JWT, Kafka, gRPC
cmd/main.go    → сборка всего вышеперечисленного
```

### 6.1 proto/bank/auth/v1/auth.proto

```protobuf
syntax = "proto3";

package bank.auth.v1;

service AuthService {
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc Login(LoginRequest) returns (LoginResponse);
  rpc Refresh(RefreshRequest) returns (LoginResponse);
}

message RegisterRequest {
  string email = 1;
  string password = 2;
}

message RegisterResponse {
  string user_id = 1;
  string email = 2;
  string role = 3;
}

message LoginRequest {
  string email = 1;
  string password = 2;
}

message LoginResponse {
  string access_token = 1;
  string refresh_token = 2;
}

message RefreshRequest {
  string refresh_token = 1;
}
```

**Разбор.** Proto-файл — **контракт**: описание того, какие методы у сервиса и какие структуры данных ходят по проводам. Из него генерируется Go-код (gen/, раздел 8), который используют обе стороны: сервер (реализует) и клиент (вызывает). Это «договор», зафиксированный в тексте, — поэтому два сервиса, написанные в разных репозиториях на разных языках, всё равно совместимы.

- `syntax = "proto3";` — версия языка описания (третья: обязательные поля отменены, дефолты нулевые).
- `package bank.auth.v1;` — пространство имён. Оно же определяет **папку** в gen/ (`bank/auth/v1`) и префикс gRPC-сервисов. `/v1/` — версионирование контракта: ломающие изменения → v2, старый v1 живёт параллельно.
- `service AuthService { rpc ... }` — список методов. Каждый rpc: имя(запрос) returns (ответ). Три метода: Register, Login, Refresh.
- `message` — структура. `string email = 1;` — тип, имя, **номер поля**. Номера — часть формата на проводе: сериализация шлёт не имена, а номера. Поэтому правило: номера не меняются и не переиспользуются, новые поля получают новые номера. Переименовать поле можно безопасно (по проводу имени нет), поменять номер — нельзя.
- `snake_case` в proto (`user_id`) → генератор превращает в `UserId`/`user_id` (Go-поле / json-тег). Ты пишешь snake_case, код получаешь CamelCase.
- `LoginResponse` используется **двумя** RPC: Login и Refresh возвращают одинаковую пару токенов. Отдельный message для Refresh — копипаста без пользы. Из-за этого в buf.yaml выключены два lint-правила (см. 6.2) — с комментарием-объяснением, чтобы решение не выглядело случайным.

Заметь, чего в proto нет: валидации (какой пароль «достаточно длинный»), сообщений об ошибках, форматов ошибок. Контракт описывает **форму**, а правила — забота сервиса.

### 6.2 proto/buf.yaml и proto/buf.gen.yaml

```yaml
version: v2
lint:
  use:
    - STANDARD
  except:
    # Login и Refresh возвращают одинаковый набор полей (access + refresh),
    # поэтому сознательно используем один message для двух RPC.
    - RPC_REQUEST_RESPONSE_UNIQUE
    - RPC_RESPONSE_STANDARD_NAME
```

```yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: mini-core/services/auth-service/gen
plugins:
  - local: protoc-gen-go
    out: ../gen
    opt: paths=source_relative
  - local: protoc-gen-go-grpc
    out: ../gen
    opt: paths=source_relative
```

**Разбор.** buf — современная замена «голому» protoc: линтер proto-файлов и единый инструмент генерации. Два файла — две роли.

**buf.yaml — правила качества.** `lint.use: [STANDARD]` — набор проверок: имена в PascalCase/snake_case, пакет с версией, service не пустой и т.д. Запускается `buf lint` (make proto-lint). Два правила **сознательно выключены** в `except`:

- `RPC_REQUEST_RESPONSE_UNIQUE` — «каждый RPC должен иметь свои уникальные request/response». Выключено, потому что Refresh сознательно переиспользует LoginResponse.
- `RPC_RESPONSE_STANDARD_NAME` — «ответ должен называться `ИмяResponse` по имени RPC». Выключено по той же причине.

Смысл не в самих выключениях, а в том, что решение **документировано в конфиге с объяснением** — через полгода никто не решит, что это случайность, и не «починит».

**buf.gen.yaml — как генерировать.** `managed.enabled: true` — управляемый режим: buf сам выставляет `go_package` для всех файлов (строка `override: go_package_prefix: mini-core/services/auth-service/gen`), руками в каждый .proto писать его не нужно. Блок `plugins` — два генератора:

- `protoc-gen-go` — сериализация: структуры сообщений + методы Marshal/Unmarshal → `auth.pb.go`;
- `protoc-gen-go-grpc` — транспорт: интерфейсы клиента/сервера и заглушки → `auth_grpc.pb.go`.

`out: ../gen` — куда класть (папка gen/ **внутри сервиса**, не общий пакет: у каждого сервиса свой контракт и своя копия кода). `opt: paths=source_relative` — сохранять структуру папок proto-пакета внутри out (получаем gen/bank/auth/v1/...). Команда `buf generate` читает оба файла; запускать её надо **из папки proto/** (make proto-gen делает cd за тебя).

### 6.3 migrations/0001_users.sql и 0002_refresh_tokens.sql

```sql
-- +goose Up
CREATE TABLE users (
    id            UUID PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'CLIENT',
    customer_id   UUID,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE users;
```

```sql
-- +goose Up
CREATE TABLE refresh_tokens (
    token_hash TEXT PRIMARY KEY,          -- хеш токена, не сам токен!
    user_id    UUID NOT NULL REFERENCES users(id),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ                 -- для будущего Logout
);

-- +goose Down
DROP TABLE refresh_tokens;
```

**Разбор.** Миграции — версионированные изменения схемы БД. Формат goose: `-- +goose Up` — как накатить, `-- +goose Down` — как откатить. Одна миграция = один шаг истории; применяются по порядку номеров. (Проект применяет их руками через psql — раздел 2.3; в роадмапе раздела 12 — автоматизировать через goose.)

**users (0001).** Каждый столбец — решение:

- `id UUID PRIMARY KEY` — первичный ключ из UUID v7 (генерирует приложение, см. 6.9). Почему не автоинкремент? UUID можно генерить в приложении до вставки (id известен **до** похода в БД — удобно для логов и событий), он глобально уникален (две базы не конфликтуют), v7 содержит метку времени → индекс остаётся сортированным, вставки не деградируют.
- `email TEXT NOT NULL UNIQUE` — уникальность на уровне БД. Именно она рождает ошибку 23505, которую репозиторий переводит в `ErrEmailTaken` (6.16). Уникальность в коде не проверяют — проверять может только БД (иначе гонки).
- `password_hash TEXT NOT NULL` — **хеш**, не пароль: argon2id-хеш вида `$argon2id$v=19$...`. Пароль в открытом виде не хранится нигде и никем не логируется.
- `role TEXT NOT NULL DEFAULT 'CLIENT'` — роль строкой с дефолтом. CHECK-ограничение на множество значений было бы правильнее (`CHECK (role IN ('CLIENT','OFFICER','ADMIN'))`) — упрощение учебного проекта.
- `customer_id UUID` — nullable-ссылка на профиль; в этой итерации не используется (связь живёт в customer_db), оставлена под будущее.
- `created_at/updated_at TIMESTAMPTZ NOT NULL DEFAULT now()` — БД сама проставит время. TIMESTAMPTZ (с таймзоной), а не TIMESTAMP — потому что «наивное» время без зоны — источник багов при переездах серверов.

**refresh_tokens (0002).** Ключевой инженерный факт: `token_hash TEXT PRIMARY KEY` — в базе **нет самих токенов, только SHA-256 хеши**. Утёкшая база не даёт злоумышленнику действующих refresh-токенов: хеш обратить нельзя. Сравнение при refresh: посчитать хеш пришедшего токена и найти его в таблице — тоже самое делает login (6.10).

- `user_id UUID NOT NULL REFERENCES users(id)` — внешний ключ: БД не даст вставить токен несуществующего юзера.
- `revoked_at TIMESTAMPTZ` — **nullable**: NULL = токен активен, непустое = отозван в это время. Nullable-колонка — удобный способ хранить «два состояния + дату перехода» в одном поле. Используется при refresh-ротации (6.11) и в будущем — для logout.

### 6.4 queries/users.sql

```sql
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
```

**Разбор.** Это **весь** SQL, который знает auth-service, про пользователей. Не «SQL размазан по Go-строкам», а отдельный файл — источник правды. sqlc (6.6) прочитает его и сгенерирует типизированные Go-методы.

Синтаксис аннотации: `-- name: ИмяМетода :тип`, где тип — `:exec` (ничего не возвращает), `:one` (одна строка), `:many` (срез строк). Три запроса:

- `CreateUser :exec` — INSERT четырёх полей. Параметры `$1..$4` — плейсхолдеры; sqlc выведет их типы из схемы (id: UUID → string по override; email: TEXT → string...) и сгенерирует структуру `CreateUserParams{ID, Email, PasswordHash, Role}`.
- `GetUserByEmail :one` — поиск по email; `$1` сравнивается с TEXT-колонкой → параметр `email string`. Результат: те же 4 колонки → `GetUserByEmailRow`.
- `GetUserByID :one` — то же по первичному ключу.

Почему это лучше строк SQL в коде: (1) типизация — sqlc сверит запрос со схемой **на этапе генерации**: опечатка в имени колонки или несоответствие типов — ошибка при `make sqlc-gen`, а не 500 в проде; (2) один дом SQL-запросов — легко ревьюить и искать; (3) код в Go остаётся без строковых литералов SQL — маппинг делает генератор.

### 6.5 queries/refresh_tokens.sql

```sql
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
```

**Разбор.** Три запроса под сценарий refresh-ротации:

- `CreateRefreshToken :exec` — INSERT хеша, user_id и срока годности.
- `GetRefreshToken :one` — поиск по `token_hash` (первичный ключ — быстрый lookup). Возвращаются все четыре колонки, включая `revoked_at` — по ней сценарий решает, жив ли токен.
- `RevokeRefreshToken :exec` — UPDATE: проставить `revoked_at = $1` по хешу. Обрати внимание: запрос не проверяет «а не отозван ли уже» — проверка «отозван → это кража» делается в сценарии (6.11), а не в SQL.

### 6.6 sqlc.yaml

```yaml
version: "2"
sql:
  - engine: "postgresql"
    schema: "migrations"        # схема берётся из goose-миграций сервиса
    queries: "queries"          # здесь лежит «источник правды» по запросам
    gen:
      go:
        package: "db"
        out: "internal/adapters/postgres/db"  # сгенерированный код живёт в internal
        sql_package: "pgx/v5"   # sqlc генерит код поверх драйвера pgx/v5
        overrides:
          # Учебное упрощение: UUID держим строками, чтобы домен не зависел
          # от pgtype. Постгрес сам проверит формат при вставке.
          # Примечание: в sqlc 1.31 тип указывается как «timestamptz»,
          # форма «pg_catalog.timestamptz» здесь не матчится.
          - db_type: "uuid"
            go_type: "string"
          - db_type: "uuid"
            nullable: true
            go_type:
              import: "github.com/google/uuid"
              type: "UUID"
              pointer: true
          - db_type: "timestamptz"
            go_type: "time.Time"
          - db_type: "timestamptz"
            nullable: true
            go_type:
              import: "time"
              type: "Time"
              pointer: true
```

**Разбор.** Конфиг генератора. Ключевые строки:

- `engine: "postgresql"`, `schema: "migrations"` — sqlc читает схему **из миграций**: он парсит CREATE TABLE и знает все колонки/типы. Отсюда типизация запросов. Следствие: поменял схему — сначала миграция, потом перегенерация.
- `queries: "queries"` — где лежат запросы.
- `gen.go.package: "db"`, `out: "internal/adapters/postgres/db"` — сгенерированный пакет живёт **внутри** адаптера Postgres. Это важно для архитектуры: сгенерированный код — деталь реализации адаптера, сценарии и домен о нём не знают.
- `sql_package: "pgx/v5"` — sqlc генерирует код поверх драйвера pgx (мы и так его используем для пула).
- `overrides` — ручная правка типов, где дефолт неудобен:

  - `uuid → string`: учебное упрощение — домен оперирует строками-UUID, а не типом `pgtype.UUID` библиотеки pgx (который втягивает pgx в домен). Постгрес сам проверит формат при вставке: кривая строка → ошибка вставки, а не мусор в базе.
  - `nullable uuid → *uuid.UUID` (импорт google/uuid) — nullable-вариант с указателем: nil = NULL.
  - `timestamptz → time.Time` и `nullable timestamptz → *time.Time` — из коробки sqlc дал бы `pgtype.Timestamptz`; override даёт обычное стандартное `time.Time`, с которым удобнее жить. Записанные в комментарии нюансы: в sqlc 1.31 тип в override называется ровно `timestamptz` — форма `pg_catalog.timestamptz` (как в системном каталоге) не матчится.

Правило работы с файлом: **генерацию не правят руками** — правят этот конфиг, миграции или queries, потом `make sqlc-gen`.

### 6.7 internal/domain/user.go

```go
// Package domain — сущности и правила предметной области auth-service.
// Здесь живёт «словарь» ошибок и инварианты (например, нормализация email).
// Пакет НЕ зависит ни от gRPC, ни от Postgres, ни от чего внешнего —
// это фундамент, на который смотрят все остальные слои.
package domain

import (
	"errors"
	"strings"
)

// Role — роль пользователя в системе.
// Базовый тип string, ограниченный константами.
type Role string

const (
	RoleClient  Role = "CLIENT"
	RoleOfficer Role = "OFFICER"
	RoleAdmin   Role = "ADMIN"
)

// Предопределённые ошибки, которые могут возвращать сценарии (use cases).
var (
	ErrInvalidInput       = errors.New("invalid input")
	ErrEmailTaken         = errors.New("email already taken")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrNotFound           = errors.New("not found")
)

// User — агрегат, представляющий пользователя.
// Все поля экспортируемые, так как они используются в других пакетах.
type User struct {
	ID           string
	Email        string
	PasswordHash string
	Role         Role
}

// NormalizeEmail приводит email к каноническому виду:
// удаляет пробелы по краям и приводит к нижнему регистру.
// Используется при создании пользователя и при поиске.
func NormalizeEmail(e string) string {
	return strings.ToLower(strings.TrimSpace(e))
}

// NewUser — фабрика для создания User.
// Выполняет валидацию email (должен содержать '@').
// При успехе возвращает указатель на User с ролью CLIENT по умолчанию.
// В случае ошибки возвращает nil и ErrInvalidInput.
func NewUser(email, passwordHash string) (*User, error) {
	email = NormalizeEmail(email)
	if !strings.Contains(email, "@") {
		return nil, ErrInvalidInput
	}
	return &User{Email: email, PasswordHash: passwordHash, Role: RoleClient}, nil
}
```

**Разбор.** Пакет domain — фундамент: сущности, правила, словарь ошибок. Его главный инженерный признак — **нули зависимостей**: здесь нет импортов pgx, gRPC, Kafka, gen/. Домен не знает, как его хранят и через какой транспорт зовут. Всё остальное зависит от домена, домен не зависит ни от кого — поэтому правила можно тестировать без БД (см. customer_test.go).

```go
type Role string
```
**Тип на базе string.** Это не просто алиас — новый тип: `Role` и `string` взаимно неприсваиваемы без конверсии. Смысл: функция `f(role Role)` не примет случайную строку по ошибке — нужна явная конверсия `Role(s)`. Константы ограничивают множество значений до трёх. (CHECK-констрейнт в БД дополнил бы картину — упрощение.)

```go
var (
        ErrInvalidInput       = errors.New("invalid input")
        ...
)
```
**Ошибки как значения.** `errors.New` создаёт уникальное значение-ошибку. Пакетные переменные с префиксом `Err` — стандарт Go. Весь механизм сверки — `errors.Is(err, domain.ErrEmailTaken)`: он сравнивает не текст, а идентичность значений (даже сквозь обёртки `fmt.Errorf("%w")`). Словарь ошибок в домене — это контракт «что может пойти не так» на языке бизнеса, без упоминания БД и транспорта.

```go
type User struct {
        ID           string
        Email        string
        PasswordHash string
        Role         Role
}
```
Агрегат пользователя. Поля экспортируемые (большая буква) — домен разрешает другим пакетам читать/писать напрямую; инварианты защищаются фабрикой и методами. Заметь: здесь **нет** CreatedAt и т.п. — домену нужны только бизнес-поля; время жизни записи — забота БД и репозитория.

```go
func NormalizeEmail(e string) string {
        return strings.ToLower(strings.TrimSpace(e))
}
```
Маленькая функция — большое правило: email канонизируется в **одном месте**. `Anna@Example.com ` и `anna@example.com` — один и тот же пользователь. Если нормализацию размазать (в хендлере обрезали, в запросе привели к нижнему регистру, а в одном месте забыли) — появятся дубли учёток. Инвариант живёт в домене и вызывается везде, где email входит в систему.

```go
func NewUser(email, passwordHash string) (*User, error) {
        email = NormalizeEmail(email)
        if !strings.Contains(email, "@") {
                return nil, ErrInvalidInput
        }
        return &User{Email: email, PasswordHash: passwordHash, Role: RoleClient}, nil
}
```
**Фабрика** — конструктор с валидацией. Валидация здесь минимальна (наличие `@` — формат email целиком проверять не будем; настоящую защиту даёт письмо-подтверждение, которого в учебном ТЗ нет). Правило, зашитое здесь: **новый пользователь всегда CLIENT** — роль не передаётся снаружи, её нельзя повысить при регистрации (только админ мог бы позже). Возврат `(*User, error)` — идиома Go «либо результат, либо ошибка»: нулевой указатель при ошибке.

### 6.8 internal/domain/refresh_token.go

```go
package domain

import "time"

// RefreshToken — refresh-токен в виде, как он лежит в БД (только хеш).
type RefreshToken struct {
	Hash      string
	UserID    string
	ExpiresAt time.Time
	RevokedAt *time.Time // nil = активен
}

// Active — доменное правило: токен жив, пока не отозван и не истёк.
// Проверяемо юнит-тестом без БД — в этом и смысл слоя.
func (t *RefreshToken) Active(now time.Time) bool {
	return t.RevokedAt == nil && now.Before(t.ExpiresAt)
}
```

**Разбор.** Вторая сущность и главное правило сессии.

```go
type RefreshToken struct {
        Hash      string
        UserID    string
        ExpiresAt time.Time
        RevokedAt *time.Time // nil = активен
}
```
`Hash`, не Token: в домене (как и в БД) сам токен не живёт — только его хеш. `RevokedAt *time.Time` — nullable: `nil` = активен. Указатель на время — Go-способ сказать «может отсутствовать»; json/sql умеют превращать nil ↔ NULL автоматически.

```go
func (t *RefreshToken) Active(now time.Time) bool {
        return t.RevokedAt == nil && now.Before(t.ExpiresAt)
}
```
**Бизнес-правило в методе**: токен жив, пока не отозван И не истёк. Почему метод, а не проверка на месте в сценарии? (1) Правило в одном месте — сценарий Refresh зовёт `rt.Active(now)`, не дублируя условия. (2) Проверяемость: юнит-тест правила не требует БД — создали структуру, позвали метод, сверили ответ. (3) Скорость изменения: завтра добавим лимит «не дольше 90 дней от создания» — правится одна строка здесь.

`now time.Time` передаётся параметром, а не берётся `time.Now()` внутри — **инъекция времени**: тест может подать любую дату (прошлую/будущую) и проверить поведение. Мелочь, которая делает правило детерминированным.

### 6.9 internal/app/register.go

```go
// Package app содержит бизнес-сценарии (use cases) для auth-сервиса.
// Реализованы сценарии:
//   - RegisterService — регистрация нового пользователя с хешированием пароля (argon2id).
//   - LoginService    — аутентификация, выпуск access и refresh токенов (JWT + SHA-256).
//
// Все зависимости (хранилища, генераторы токенов) внедряются через интерфейсы,
// определенные в этом же пакете (UserRepository, UserReader, TokenIssuer, RefreshWriter).
package app

import (
	"context"

	"github.com/alexedwards/argon2id" // хеширование паролей
	"github.com/google/uuid"          // UUID v7 — по требованию ТЗ

	"mini-core/services/auth-service/internal/domain"
)

// UserEvents — порт «рассказать миру о новых юзерах».
type UserEvents interface {
	UserRegistered(ctx context.Context, userID, email string)
}

// UserRepository — порт, который требуется сценарию RegisterService.
// Адаптер (например, postgres.UserRepo) реализует этот интерфейс.
// Определён здесь, чтобы сценарий не зависел от конкретной реализации.
type UserRepository interface {
	Create(ctx context.Context, u *domain.User) error
}

// RegisterService — структура, реализующая сценарий регистрации.
// Содержит зависимость от хранилища через интерфейс UserRepository.
type RegisterService struct {
	repo UserRepository
	pub  UserEvents
}

// NewRegister собирает сценарий регистрации.
// repo — порт хранилища: интерфейс объявлен здесь, реализацию
// (postgres.UserRepo) внедряет main при сборке.
func NewRegister(repo UserRepository, pub UserEvents) *RegisterService {
	return &RegisterService{repo: repo, pub: pub}
}

// Register — основной метод сценария.
// 1. Проверяет длину пароля (минимум 8 символов).
// 2. Хеширует пароль с помощью argon2id (медленный, но безопасный алгоритм).
// 3. Создаёт объект User через фабрику domain.NewUser (валидация email).
// 4. Генерирует UUID v7 для ID пользователя.
// 5. Сохраняет пользователя через репозиторий.
// Возвращает созданного пользователя или ошибку.
// Ошибки: domain.ErrInvalidInput (короткий пароль или невалидный email),
// domain.ErrEmailTaken (email уже существует), а также ошибки хеширования или генерации UUID.
func (s *RegisterService) Register(ctx context.Context, email, password string) (*domain.User, error) {
	if len(password) < 8 {
		return nil, domain.ErrInvalidInput
	}
	hash, err := argon2id.CreateHash(password, argon2id.DefaultParams)
	if err != nil {
		return nil, err
	}
	u, err := domain.NewUser(email, hash)
	if err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	u.ID = id.String()
	if err := s.repo.Create(ctx, u); err != nil {
		return nil, err
	}
	s.pub.UserRegistered(ctx, u.ID, u.Email) // факт после коммита в БД
	return u, nil
}
```

**Разбор.** Пакет app — **сценарии** (use cases): «что делает сервис», в терминах домена и портов. Порт = интерфейс, объявленный **там, где его используют** (в Go так принято; в Java интерфейсы обычно лежат рядом с реализацией). Сценарий объявляет, что ему нужно («умеющий создать юзера»), реализацию подставляет main.

```go
type UserEvents interface {
        UserRegistered(ctx context.Context, userID, email string)
}

type UserRepository interface {
        Create(ctx context.Context, u *domain.User) error
}
```
Два порта по одному методу. Маленькие интерфейсы — фишка Go (в отличие от «толстых» UserRepo на 30 методов): сценарий объявляет ровно то, что использует, а фейк для теста реализует ровно это. Оба интерфейса реализуются **уже существующими** типами: `postgres.UserRepo` (6.16) и `kafka.Publisher` (6.14) — им не нужно ничего менять, Go совпадение методов считает реализацией (structural typing).

```go
type RegisterService struct {
        repo UserRepository
        pub  UserEvents
}

func NewRegister(repo UserRepository, pub UserEvents) *RegisterService {
        return &RegisterService{repo: repo, pub: pub}
}
```
Сценарий хранит **порты**, а не реализации. Конструктор принимает их аргументами (dependency injection руками). Никаких глобальных переменных, никаких `New()` внутри структуры — всё видимым образом приходит снаружи.

```go
func (s *RegisterService) Register(ctx context.Context, email, password string) (*domain.User, error) {
        if len(password) < 8 {
                return nil, domain.ErrInvalidInput
        }
```
Шаг 1 — правило «пароль ≥ 8». При ошибке — доменная ошибка; адаптер позже переведёт её в gRPC-код InvalidArgument.

```go
        hash, err := argon2id.CreateHash(password, argon2id.DefaultParams)
        if err != nil {
                return nil, err
        }
```
Шаг 2 — **хеширование пароля**. Почему argon2id, а не sha256+соль? Пароли — низкоэнтропийный материал: sha256 считается миллиардами в секунду на GPU, и подбор тривиален. argon2id — «медленный по дизайну» алгоритм (память + время, настраиваемые параметры), он делает брутфорс дорогим. DefaultParams — нормальные дефолты библиотеки; результат хеша уже содержит соль и параметры (строка вида `$argon2id$v=19$m=65536,t=3,p=4$соль$хеш`), отдельно соль хранить не надо.

```go
        u, err := domain.NewUser(email, hash)
        if err != nil {
                return nil, err
        }
        id, err := uuid.NewV7()
        if err != nil {
                return nil, err
        }
        u.ID = id.String()
```
Шаги 3–4: создать доменную сущность (валидация email, роль CLIENT) и сгенерировать UUID **v7** — вариант UUID с меткой времени в старших битах: идентификаторы растут во времени → индексы БД не деградируют от случайных вставок (в отличие от v4). ID генерирует приложение, а не БД: он известен до INSERT, попадает в логи/события вместе с остальным контекстом.

```go
        if err := s.repo.Create(ctx, u); err != nil {
                return nil, err
        }
        s.pub.UserRegistered(ctx, u.ID, u.Email) // факт после коммита в БД
        return u, nil
}
```
Шаг 5 — сохранение через порт (внутри — sqlc-метод, 6.16). Шаг 6 — **событие после сохранения**: публикуем `user_registered`, только когда учётка уже в БД. Порядок принципиален: событие «до» означало бы «юзер зарегистрирован», которого нет. Честности ради: событие и INSERT **не в одной транзакции** (это outbox-паттерн, раздел 12) — если Kafka лежит, событие потеряется; регистрация при этом вернёт успех (producer логирует ошибку и не паникует, 6.14). Для учебной модели — осознанный компромисс.

Итог: сценарий — линейная последовательность доменных операций и портов; ни одного упоминания SQL, JWT-библиотеки или Kafka-топика. Поэтому его юнит-тест — три фейка и несколько assert'ов.

### 6.10 internal/app/login.go

```go
// Package app содержит бизнес-сценарии (use cases) для auth-сервиса.
// Реализованы сценарии:
//   - RegisterService — регистрация нового пользователя с хешированием пароля (argon2id).
//   - LoginService    — аутентификация, выпуск access и refresh токенов (JWT + SHA-256).
//
// Все зависимости (хранилища, генераторы токенов) внедряются через интерфейсы,
// определенные в этом же пакете (UserRepository, UserReader, TokenIssuer, RefreshWriter).
package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/alexedwards/argon2id"

	"mini-core/services/auth-service/internal/domain"
)

const (
	accessTTL  = 15 * time.Minute
	refreshTTL = 30 * 24 * time.Hour
)

// Три маленьких порта — по одному на каждую потребность.
// Интерфейс в том, кто ИСПОЛЬЗУЕТ, а не кто реализует (сейчас поймёшь зачем)
type UserReader interface {
	FindByEmail(ctx context.Context, email string) (*domain.User, error)
}

type TokenIssuer interface {
	IssueAccess(userID, role string, ttl time.Duration) (string, error)
}

type RefreshWriter interface {
	Create(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error
}

// LoginService — сценарий аутентификации пользователя.
// Содержит три зависимости: чтение пользователя, выпуск access-токена,
// запись refresh-токена. Не зависит от конкретных реализаций (Postgres, JWT-библиотека).
type LoginService struct {
	users   UserReader
	tokens  TokenIssuer
	refresh RefreshWriter
}

// NewLogin — конструктор LoginService.
// Принимает реализации портов и возвращает готовый сервис.
func NewLogin(users UserReader, tokens TokenIssuer, refresh RefreshWriter) *LoginService {
	return &LoginService{users: users, tokens: tokens, refresh: refresh}
}

// Login выполняет вход пользователя:
//  1. Нормализует email и ищет пользователя в БД.
//     Если пользователь не найден, возвращает ErrInvalidCredentials (без разглашения).
//  2. Сравнивает пароль с хешем через argon2id.
//  3. Выпускает access-токен через TokenIssuer (JWT с RS256).
//  4. Генерирует случайный refresh-токен (32 байта в base64url).
//  5. Сохраняет его SHA-256 хеш в хранилище через RefreshWriter.
//
// Возвращает:
//   - access — JWT-токен для доступа к API.
//   - refresh — случайный токен для обновления сессии.
//   - err    — ошибка (domain.ErrInvalidCredentials, domain.ErrNotFound и др.).
func (s *LoginService) Login(ctx context.Context, email, password string) (access, refresh string, err error) {
	u, err := s.users.FindByEmail(ctx, domain.NormalizeEmail(email))
	if errors.Is(err, domain.ErrNotFound) {
		return "", "", domain.ErrInvalidCredentials // ← ВНИМАНИЕ, вернёмся к этому в вопросах
	}
	if err != nil {
		return "", "", err
	}

	ok, err := argon2id.ComparePasswordAndHash(password, u.PasswordHash)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return "", "", domain.ErrInvalidCredentials
	}

	access, err = s.tokens.IssueAccess(u.ID, string(u.Role), accessTTL)
	if err != nil {
		return "", "", err
	}

	refresh, err = newRefreshToken()
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256([]byte(refresh))
	if err := s.refresh.Create(ctx, hex.EncodeToString(sum[:]), u.ID, time.Now().UTC().Add(refreshTTL)); err != nil {
		return "", "", err
	}
	return access, refresh, nil
}

// newRefreshToken генерирует криптостойкий случайный токен (32 байта)
// и кодирует его в base64url без дополнения (=).
// Используется внутри Login для создания refresh-токена.
func newRefreshToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
```

**Разбор.** Сценарий входа — здесь рождаются оба токена.

```go
const (
        accessTTL  = 15 * time.Minute
        refreshTTL = 30 * 24 * time.Hour
)
```
Сроки жизни. Access короткий: если украдут — проживёт не больше 15 минут. Refresh длинный, но хранится как хеш и работает один раз (ротация, 6.11).

```go
type UserReader interface {
        FindByEmail(ctx context.Context, email string) (*domain.User, error)
}

type TokenIssuer interface {
        IssueAccess(userID, role string, ttl time.Duration) (string, error)
}

type RefreshWriter interface {
        Create(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error
}
```
**Три крошечных порта.** Заметь, как они разложены по потребностям: логину нужен поиск юзера, выпуск access-токена и запись refresh-хеша — и всё. Он **не знает**, что токен будет JWT (может, завтра PASETO), что refresh ляжет в Postgres (может, в Redis), что юзер лежит в Postgres. Каждый порт реализует существующий адаптер: `UserRepo`, `Issuer`, `RefreshRepo`.

```go
func (s *LoginService) Login(ctx context.Context, email, password string) (access, refresh string, err error) {
        u, err := s.users.FindByEmail(ctx, domain.NormalizeEmail(email))
        if errors.Is(err, domain.ErrNotFound) {
                return "", "", domain.ErrInvalidCredentials // ← ВНИМАНИЕ, вернёмся к этому в вопросах
        }
```
Шаг 1 — поиск по email (нормализованному — иначе `Anna@Example.com` не найдёт учётку `anna@example.com`). Внимание на маскировку: «юзера нет» и «пароль неверный» для клиента **неотличимы** — оба превращаются в `ErrInvalidCredentials` и в итоге в 401 «неверный email или пароль». Зачем: если бы сервис отвечал «такого email нет», злоумышленник перебором собирал бы базу зарегистрированных почт. Стоимость решения — небольшая асимметрия в логах (реальную причину видно только в логах сервиса по request_id).

```go
        ok, err := argon2id.ComparePasswordAndHash(password, u.PasswordHash)
        if err != nil {
                return "", "", err
        }
        if !ok {
                return "", "", domain.ErrInvalidCredentials
        }
```
Шаг 2 — сверка пароля с хешем. `ComparePasswordAndHash` сама достанет соль и параметры из строки хеша и посчитает так же медленно. `ok=false` → та же маскированная ошибка.

```go
        access, err = s.tokens.IssueAccess(u.ID, string(u.Role), accessTTL)
```
Шаг 3 — выпуск access-токена через порт. Реализация (JWT RS256) — в 6.12; сценарию достаточно знать, что получится строка.

```go
        refresh, err = newRefreshToken()
        ...
        sum := sha256.Sum256([]byte(refresh))
        if err := s.refresh.Create(ctx, hex.EncodeToString(sum[:]), u.ID, time.Now().UTC().Add(refreshTTL)); err != nil {
                return "", "", err
        }
        return access, refresh, nil
}
```
Шаги 4–5 — refresh-токен. Генерация:

```go
func newRefreshToken() (string, error) {
        b := make([]byte, 32)
        if _, err := rand.Read(b); err != nil {
                return "", err
        }
        return base64.RawURLEncoding.EncodeToString(b), nil
}
```
`crypto/rand` — **криптографический** ГСЧ (не `math/rand`!). 32 случайных байта = 256 бит энтропии; `base64.RawURLEncoding` — без `=`-набивки и с URL-безопасным алфавитом: токен спокойно живёт в заголовках и JSON. Потом — `sha256.Sum256`: в БД уходит **хекс-строка хеша**, сам токен нигде не сохраняется. Срок годности: `time.Now().UTC().Add(refreshTTL)` — 30 дней.

Заметь сигнатуру: `(access, refresh string, err error)` — **именованные возвращаемые значения**; они объявлены в сигнатуре, и `return access, refresh, nil` их заполняет. Удобно, когда возвращаемых значений много и они одной природы.

### 6.11 internal/app/refresh.go

```go
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"mini-core/services/auth-service/internal/domain"
)

// RefreshStore — порт: поиск refresh-токена по хешу и его отзыв.
type RefreshStore interface {
	FindByHash(ctx context.Context, hash string) (*domain.RefreshToken, error)
	Revoke(ctx context.Context, hash string, at time.Time) error
}

// UserByIDReader — порт: найти пользователя по id.
type UserByIDReader interface {
	FindByID(ctx context.Context, id string) (*domain.User, error)
}

// RefreshService — сценарий «обновить пару токенов» с ротацией.
type RefreshService struct {
	store   RefreshStore
	users   UserByIDReader
	tokens  TokenIssuer
	refresh RefreshWriter
}

func NewRefresh(store RefreshStore, users UserByIDReader, tokens TokenIssuer, refresh RefreshWriter) *RefreshService {
	return &RefreshService{store: store, users: users, tokens: tokens, refresh: refresh}
}

// Refresh проверяет refresh-токен, отзывает его и выдаёт новую пару.
// Использование уже отозванного токена — признак кражи: отказ.
func (s *RefreshService) Refresh(ctx context.Context, token string) (access, newRefresh string, err error) {
	sum := sha256.Sum256([]byte(token))
	hash := hex.EncodeToString(sum[:])

	rt, err := s.store.FindByHash(ctx, hash)
	if errors.Is(err, domain.ErrNotFound) {
		return "", "", domain.ErrInvalidCredentials
	}
	if err != nil {
		return "", "", err
	}

	// Порядок важен: сначала проверка отзыва, потом истечение.
	if rt.RevokedAt != nil {
		// TODO(прод): отозвать все refresh этого юзера + тревога
		return "", "", domain.ErrInvalidCredentials
	}
	if !rt.Active(time.Now().UTC()) {
		return "", "", domain.ErrInvalidCredentials
	}

	u, err := s.users.FindByID(ctx, rt.UserID)
	if err != nil {
		return "", "", err
	}

	access, err = s.tokens.IssueAccess(u.ID, string(u.Role), accessTTL)
	if err != nil {
		return "", "", err
	}

	newRefresh, err = newRefreshToken()
	if err != nil {
		return "", "", err
	}
	sum = sha256.Sum256([]byte(newRefresh))
	if err := s.refresh.Create(ctx, hex.EncodeToString(sum[:]), u.ID, time.Now().UTC().Add(refreshTTL)); err != nil {
		return "", "", err
	}

	// Отзыв последним: если Create упал, старый токен ещё жив — клиент не без всего.
	if err := s.store.Revoke(ctx, hash, time.Now().UTC()); err != nil {
		return "", "", err
	}
	return access, newRefresh, nil
}
```

**Разбор.** Сценарий «обновить пару токенов» — здесь реализована **ротация**: каждый refresh-токен работает ровно один раз, после чего заменяется новым. Если один и тот же refresh используют дважды — второй раз откажет; это сигнал кражи (токен скопировали и успели первым).

```go
type RefreshStore interface {
        FindByHash(ctx context.Context, hash string) (*domain.RefreshToken, error)
        Revoke(ctx context.Context, hash string, at time.Time) error
}

type UserByIDReader interface {
        FindByID(ctx context.Context, id string) (*domain.User, error)
}
```
Ещё два порта. RefreshService собирает **четыре** зависимости:

```go
type RefreshService struct {
        store   RefreshStore
        users   UserByIDReader
        tokens  TokenIssuer
        refresh RefreshWriter
}
```
`store` и `refresh` в реальности один и тот же `RefreshRepo` (реализует оба интерфейса) — main передаст его в обе роли (6.19). Это не ошибка, а демонстрация силы маленьких интерфейсов: один тип, два взгляда.

```go
func (s *RefreshService) Refresh(ctx context.Context, token string) (access, newRefresh string, err error) {
        sum := sha256.Sum256([]byte(token))
        hash := hex.EncodeToString(sum[:])
```
Шаг 1 — тот же хеш, что считал login: поиск в БД идёт по хешу, значит и здесь считаем его от пришедшего токена.

```go
        rt, err := s.store.FindByHash(ctx, hash)
        if errors.Is(err, domain.ErrNotFound) {
                return "", "", domain.ErrInvalidCredentials
        }
```
Шаг 2 — найти. Не нашли → маскированная ошибка (см. 6.10 — наружу не различаем «нет такого» и «невалидный»).

```go
        if rt.RevokedAt != nil {
                // TODO(прод): отозвать все refresh этого юзера + тревога
                return "", "", domain.ErrInvalidCredentials
        }
        if !rt.Active(time.Now().UTC()) {
                return "", "", domain.ErrInvalidCredentials
        }
```
Шаг 3 — **порядок проверок важен**. Сначала «отозван?»: повторное использование отозванного токена — признак кражи (или гонки двух устройств); правильный ответ прод-системы — отозвать ВСЕ токены юзера и поднять тревогу (TODO оставлен в коде, в учебной версии — просто отказ). Потом «истёк?»: `rt.Active(now)` — то самое доменное правило из 6.8. Обе ветки дают одну маскированную ошибку.

```go
        u, err := s.users.FindByID(ctx, rt.UserID)
```
Шаг 4 — найти юзера по id из токена. Нужен, во-первых, для роли в новом access-токене, во-вторых, как проверка: юзер мог быть удалён за 30 дней жизни refresh — тогда входа нет.

```go
        access, err = s.tokens.IssueAccess(u.ID, string(u.Role), accessTTL)
        ...
        newRefresh, err = newRefreshToken()
        ...
        if err := s.refresh.Create(ctx, hex.EncodeToString(sum[:]), u.ID, time.Now().UTC().Add(refreshTTL)); err != nil {
                return "", "", err
        }
```
Шаги 5–6 — выпустить новую пару (тот же код, что в login).

```go
        if err := s.store.Revoke(ctx, hash, time.Now().UTC()); err != nil {
                return "", "", err
        }
        return access, newRefresh, nil
}
```
Шаг 7 — **отзыв старого токена последним**. Порядок «сначала создать новый, потом отозвать старый» выбран намеренно: если выпуск нового упадёт (БД моргнула), старый токен ещё жив — клиент не остался без сессии и может повторить. Наоборот («отозвали, потом упали на создании») — клиент без сессии, идёт логиниться заново. Минимальная цена варианта: между Create и Revoke оба токена действуют одновременно миллисекунды — риск принят. В проде оба шага заворачивают в одну транзакцию БД.

### 6.12 internal/adapters/jwtauth/issuer.go

```go
// Package jwtauth предоставляет реализацию порта TokenIssuer,
// используя JWT с подписью RS256 (RSA).
// Содержит структуру Issuer, которая умеет выпускать access-токены
// с claims: sub (user ID), role, iat, exp.
package jwtauth

import (
	"crypto/rsa"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Issuer — реализация TokenIssuer для выпуска JWT.
// Хранит приватный RSA-ключ для подписи токенов.
type Issuer struct {
	key *rsa.PrivateKey
}

// NewIssuer создаёт новый Issuer из PEM-закодированного приватного ключа RSA.
// Ожидается блок "RSA PRIVATE KEY". Возвращает ошибку, если ключ невалидный.
func NewIssuer(privateKeyPEM []byte) (*Issuer, error) {
	key, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	return &Issuer{key: key}, nil
}

// IssueAccess генерирует JWT-токен с подписью RS256.
// Устанавливает claims:
//   - sub  — userID
//   - role — роль пользователя (для RBAC)
//   - iat  — время выпуска (Unix)
//   - exp  — время истечения (Unix) = now + ttl.
//
// Возвращает строку токена или ошибку.
func (i *Issuer) IssueAccess(userID, role string, ttl time.Duration) (string, error) {
	now := time.Now().UTC()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub":  userID,
		"role": role,
		"iat":  now.Unix(),
		"exp":  now.Add(ttl).Unix(),
	})
	return tok.SignedString(i.key)
}
```

**Разбор.** Это **зеркало** verifier'а из 5.3 — и лучшая пара для понимания. Issuer хранит **приватный** ключ и создаёт подписи; Verifier хранит публичный и проверяет. Оба используют одну библиотеку `jwt/v5`, но для противоположных операций.

```go
type Issuer struct {
        key *rsa.PrivateKey
}
```
Одно поле — объект приватного ключа. Приватный ключ математически включает и публичную часть (публичный выводится из приватного), поэтому одного поля достаточно.

```go
func NewIssuer(privateKeyPEM []byte) (*Issuer, error) {
        key, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
        if err != nil {
                return nil, err
        }
        return &Issuer{key: key}, nil
}
```
Тот же паттерн «байты PEM → объект ключа», что в `verifier.New`. Разница в одном слове — Private против Public. Вызывается один раз при старте auth-service (6.19).

```go
func (i *Issuer) IssueAccess(userID, role string, ttl time.Duration) (string, error) {
        now := time.Now().UTC()
        tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
                "sub":  userID,
                "role": role,
                "iat":  now.Unix(),
                "exp":  now.Add(ttl).Unix(),
        })
        return tok.SignedString(i.key)
}
```
Вот **единственное место в системе, где рождается подпись**. По шагам:

- `now := time.Now().UTC()` — время в UTC: claims JWT — это Unix-числа, UTC не даёт скачков таймзоны испортить арифметику.
- `jwt.NewWithClaims(SigningMethodRS256, MapClaims{...})` — создать токен с заголовком `{"alg":"RS256","typ":"JWT"}` и payload'ом из claims. Значения:
  - `sub` (subject) — **кто**; gateway позже достанет его в `Claims.UserID`;
  - `role` — кастомный claim; для будущего RBAC;
  - `iat` (issued at) — когда выпущен;
  - `exp` (expires) — когда истечёт; **вот здесь** действует accessTTL=15 минут из 6.10. Сроки в Unix-секундах (`now.Unix()`).
- `tok.SignedString(i.key)` — сериализовать header.payload и **подписать** приватным ключом (RSA + SHA-256). Возвращает строку `header.payload.signature`. Кто угодно с публичным ключом может **прочитать** payload (base64 — не шифрование!) и **проверить** подпись, но **создать** подпись может только держатель приватного.

Инженерная рамка: Issuer реализует порт `app.TokenIssuer` — сценарий Login/Refresh зовёт `s.tokens.IssueAccess(...)`, не зная про JWT. Сменим технологию токенов — правится только этот файл.

### 6.13 internal/adapters/jwks/handler.go

```go
// Package jwks — HTTP-адаптер: отдаёт публичный ключ auth-service в формате JWKS
// (JSON Web Key Set — стандартный формат публикации ключей подписи).
// Gateway читает этот эндпоинт, чтобы проверять подпись access-токенов локально.
package jwks

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
)

// Handler собирает JWKS-документ из PEM-файла публичного ключа
// и возвращает http.HandlerFunc, который его отдаёт.
// Делаем это один раз при старте: ключ в процессе работы не меняется.
func Handler(publicPEM []byte) (http.HandlerFunc, error) {
	pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			// n и e — «сырьё» RSA-ключа (модуль и экспонента) в base64url.
			"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(bigIntBytes(pub.E)),
		}},
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}, nil
}

// bigIntBytes — экспонента (маленькое число) в big-endian байты.
// В JWKS экспонента передаётся именно как байты, поэтому конвертируем вручную.
func bigIntBytes(i int) []byte {
	b := make([]byte, 0)
	for i > 0 {
		b = append([]byte{byte(i & 0xff)}, b...)
		i >>= 8
	}
	return b
}
```

**Разбор.** JWKS (JSON Web Key Set) — стандартный способ **публиковать** публичные ключи подписи по HTTP: любой потребитель может прийти на `/.well-known/jwks.json` и узнать актуальные ключи. Зачем, если gateway читает ключ из файла? Для локальной разработки файла достаточно; в проде (и в роадмапе, раздел 12, п. 5) gateway будет сам ходить на этот эндпоинт — ключи смогут ротироваться без релиза gateway.

```go
func Handler(publicPEM []byte) (http.HandlerFunc, error) {
        pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
        if err != nil {
                return nil, err
        }
        doc := map[string]any{ ... }
        return func(w http.ResponseWriter, _ *http.Request) {
                w.Header().Set("Content-Type", "application/json")
                _ = json.NewEncoder(w).Encode(doc)
        }, nil
}
```
**Функция, возвращающая хендлер** — паттерн «фабрика middleware/handler». Почему не сразу хендлер? Потому что сборку JWKS-документа нужно сделать **один раз**: распарсить ключ, вычислить `n`/`e`, собрать map. Возвращённая анонимная функция (closure) **захватила** готовый `doc` и на каждый запрос просто отдаёт его — работа не повторяется. Второй аргумент `_` — запрос нам не нужен.

Структура JWKS-документа:

```go
"keys": []map[string]string{{
        "kty": "RSA",   // тип ключа
        "use": "sig",   // для чего: подпись (не шифрование)
        "alg": "RS256", // каким алгоритмом проверять
        "n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()), // модуль
        "e": base64.RawURLEncoding.EncodeToString(bigIntBytes(pub.E)), // экспонента
}}
```
RSA-публичный ключ — это всего два числа: модуль `n` (длинный) и экспонента `e` (обычно 65537). В JWKS их отдают **байтовыми строками** в base64url. `pub.N.Bytes()` — модуль как big.Int умеет отдавать свои байты напрямую. А вот `pub.E` в Go-структуре — **int**, поэтому нужен хелпер:

```go
func bigIntBytes(i int) []byte {
        b := make([]byte, 0)
        for i > 0 {
                b = append([]byte{byte(i & 0xff)}, b...)
                i >>= 8
        }
        return b
}
```
Ручная конвертация int → big-endian байты: `i & 0xff` — младший байт, `i >>= 8` — сдвиг вправо на байт; вставляем в начало среза. Для 65537 получится `[0x01, 0x00, 0x01]` — как и ожидает стандарт.

Заметь `_ = json.NewEncoder(w).Encode(doc)` — снова осознанное игнорирование ошибки кодирования (5.5). И — маленький HTTP-сервер с этим хендлером поднимает main auth-сервиса на `:8081` (6.19).

### 6.14 internal/adapters/kafka/producer.go

```go
package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// Envelope — единый формат событий (см. ТЗ).
type Envelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt string          `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

// Publisher — публикация событий в топик auth.events.
// TODO(прод): перевод на outbox-паттерн (требование ТЗ для статусных событий).
type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(broker string) *Publisher {
	return &Publisher{writer: &kafkago.Writer{
		Addr:                   kafkago.TCP(broker),
		Topic:                  "auth.events",
		AllowAutoTopicCreation: true,
	}}
}

func (p *Publisher) UserRegistered(ctx context.Context, userID, email string) {
	payload, _ := json.Marshal(map[string]string{"user_id": userID, "email": email})
	env := Envelope{
		EventID:    uuid.NewString(),
		EventType:  "user_registered",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Payload:    payload,
	}
	body, _ := json.Marshal(env)
	if err := p.writer.WriteMessages(ctx, kafkago.Message{Key: []byte(userID), Value: body}); err != nil {
		log.Printf("kafka publish: %v", err) // не роняем регистрацию из-за шины
	}
}

func (p *Publisher) Close() error { return p.writer.Close() }
```

**Разбор.** Адаптер публикации событий. Kafka — «журнал событий»: producer пишет сообщения в **топик** (лог), консьюмеры читают независимо. Здесь топик один — `auth.events`.

**Envelope (14–19).** Единый формат событий по ТЗ — «конверт» с общими полями:

```go
type Envelope struct {
        EventID    string          `json:"event_id"`
        EventType  string          `json:"event_type"`
        OccurredAt string          `json:"occurred_at"`
        Payload    json.RawMessage `json:"payload"`
}
```
`EventID` (UUID) — для дедупликации и трассировки события по системам. `EventType` — имя события (`user_registered`); консьюмер по нему решает, его ли это событие. `OccurredAt` — RFC3339-строка времени. `Payload json.RawMessage` — **сырой** вложенный JSON: RawMessage = «байты, которые уже JSON», при маршалинге подставляются как есть, без двойного кодирования. Такой конверт позволяет топику носить события разных типов без изменения формата.

**Publisher (23–33).** Обёртка над `kafkago.Writer`. `Addr: kafkago.TCP(broker)` — адрес брокера; `Topic: "auth.events"` — куда писать по умолчанию. `AllowAutoTopicCreation: true` — создать топик при первом сообщении: удобно в разработке (не нужен отдельный шаг), в проде отключают — топики создают с нужной конфигурацией.

**UserRegistered (35–47).**

```go
payload, _ := json.Marshal(map[string]string{"user_id": userID, "email": email})
```
Собрать payload события (ошибка маршалинга map из двух строк невозможна — `_`).

```go
env := Envelope{
        EventID:    uuid.NewString(),
        EventType:  "user_registered",
        OccurredAt: time.Now().UTC().Format(time.RFC3339),
        Payload:    payload,
}
body, _ := json.Marshal(env)
if err := p.writer.WriteMessages(ctx, kafkago.Message{Key: []byte(userID), Value: body}); err != nil {
        log.Printf("kafka publish: %v", err) // не роняем регистрацию из-за шины
}
```
Ключевая инженерная деталь — **Key сообщения**: `userID`. Kafka гарантирует порядок только **внутри партиции**, а партиция выбирается хешем ключа: все события одного юзера идут в одну партицию и сохраняют порядок относительно друг друга. Value — сам envelope.

И главное решение: **ошибка публикации не роняет регистрацию**. Регистрация уже успешна (учётка в БД, 6.9); потеря события в учебной модели — приемлемая цена (профиль можно создать руками/переиграть). Правильное прод-решение — outbox (12.2). Событие — «факт после коммита», его сбой не должен откатывать сделку.

### 6.15 internal/adapters/grpcapi/server.go

```go
// Package grpcapi реализует gRPC-сервер для банковского auth-сервиса.
// Это адаптер, который:
// - принимает gRPC-запросы,
// - преобразует их в вызовы сценариев (use cases) из пакета app,
// - маппит доменные ошибки в gRPC статусы (codes).
// Бизнес-логики здесь нет — только преобразование данных и обработка ошибок.
package grpcapi

import (
        "context"
        "errors"

        "google.golang.org/grpc/codes"
        "google.golang.org/grpc/status"

        authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
        "mini-core/services/auth-service/internal/app"
        "mini-core/services/auth-service/internal/domain"
)

// Server реализует gRPC-контракт bank.auth.v1.AuthService.
// Роль — переводчик: proto-запрос → вызов use case (пакет app),
// доменная ошибка → код gRPC (AlreadyExists, InvalidArgument, ...).
// Бизнес-логики здесь нет и быть не должно.
type Server struct {
        authv1.UnimplementedAuthServiceServer
        reg     *app.RegisterService // зависимость приходит снаружи, не создаётся внутри
        login   *app.LoginService
        refresh *app.RefreshService
}

// New — конструктор Server.
// Принимает сценарий регистрации и возвращает gRPC-обработчик.
func New(reg *app.RegisterService, login *app.LoginService, refresh *app.RefreshService) *Server {
        return &Server{reg: reg, login: login, refresh: refresh}
}

// Register — реализация RPC-метода из proto-контракта.
// 1. Извлекает email и password из запроса.
// 2. Вызывает сценарий RegisterService.Register.
// 3. Преобразует доменные ошибки в соответствующие gRPC-статусы:
//   - domain.ErrEmailTaken → codes.AlreadyExists
//   - domain.ErrInvalidInput → codes.InvalidArgument
//   - другие ошибки → codes.Internal (с общим сообщением, чтобы не раскрывать детали).
//
// 4. В случае успеха формирует gRPC-ответ с данными пользователя.
func (s *Server) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
        u, err := s.reg.Register(ctx, req.GetEmail(), req.GetPassword())
        switch {
        case errors.Is(err, domain.ErrEmailTaken):
                return nil, status.Error(codes.AlreadyExists, "email уже зарегистрирован")
        case errors.Is(err, domain.ErrInvalidInput):
                return nil, status.Error(codes.InvalidArgument, "некорректный email или пароль короче 8 символов")
        case err != nil:
                return nil, status.Error(codes.Internal, "internal error")
        }
        return &authv1.RegisterResponse{UserId: u.ID, Email: u.Email, Role: string(u.Role)}, nil
}

// Login — реализация RPC-метода из proto-контракта.
// 1. Извлекает email и password из запроса.
// 2. Вызывает сценарий LoginService.Login.
// 3. Преобразует доменные ошибки в gRPC-статусы:
//   - domain.ErrInvalidCredentials → codes.Unauthenticated
//   - любые другие ошибки → codes.Internal (с общим сообщением).
//
// 4. В случае успеха возвращает access и refresh токены.
func (s *Server) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
        access, refresh, err := s.login.Login(ctx, req.GetEmail(), req.GetPassword())
        switch {
        case errors.Is(err, domain.ErrInvalidCredentials):
                return nil, status.Error(codes.Unauthenticated, "неверный email или пароль")
        case err != nil:
                return nil, status.Error(codes.Internal, "internal error")
        }
        return &authv1.LoginResponse{AccessToken: access, RefreshToken: refresh}, nil
}

func (s *Server) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.LoginResponse, error) {
        access, refresh, err := s.refresh.Refresh(ctx, req.GetRefreshToken())
        if err != nil {
                return nil, mapAuthErr(err)
        }
        return &authv1.LoginResponse{AccessToken: access, RefreshToken: refresh}, nil
}

// mapAuthErr — единый перевод доменных ошибок в gRPC-коды.
func mapAuthErr(err error) error {
        switch {
        case errors.Is(err, domain.ErrInvalidCredentials):
                return status.Error(codes.Unauthenticated, "неверные учётные данные")
        case err != nil:
                return status.Error(codes.Internal, "internal error")
        }
        return nil
}
```

**Разбор.** gRPC-адаптер — «приёмник» сервиса: реализует контракт из proto (6.1) и переводит gRPC-мир в мир сценариев. Здесь нет ни одной строки бизнес-логики — только три действия: распаковать запрос, вызвать сценарий, перевести ошибки.

```go
type Server struct {
        authv1.UnimplementedAuthServiceServer
        reg     *app.RegisterService
        login   *app.LoginService
        refresh *app.RefreshService
}
```
**Встраивание `UnimplementedAuthServiceServer`** — обязательный ритуал gRPC-Go. Сгенерированный серверный интерфейс требует реализации всех методов RPC; заглушка Unimplemented даёт им дефолтную реализацию «not implemented». Смысл — **совместимость вперёд**: добавишь в proto новый RPC и перегенеришь — сервер всё ещё компилируется (новый метод просто отвечает Unimplemented), и только потом ты его реализуешь. Без встраивания каждая правка контракта ломала бы компиляцию всех, кто не сразу реализовал новый метод.

Поля — три **сценария**, не репозитории и не SQL: адаптер зависит от app (раздел 8.5 — таблица направлений зависимостей).

**Register (47–58):**

```go
u, err := s.reg.Register(ctx, req.GetEmail(), req.GetPassword())
switch {
case errors.Is(err, domain.ErrEmailTaken):
        return nil, status.Error(codes.AlreadyExists, "email уже зарегистрирован")
case errors.Is(err, domain.ErrInvalidInput):
        return nil, status.Error(codes.InvalidArgument, "некорректный email или пароль короче 8 символов")
case err != nil:
        return nil, status.Error(codes.Internal, "internal error")
}
return &authv1.RegisterResponse{UserId: u.ID, Email: u.Email, Role: string(u.Role)}, nil
```
Разбор по кусочкам:

- `req.GetEmail()` — сгенерированные геттеры вместо прямого доступа `req.Email`: геттер **nil-безопасен** (если req == nil, вернёт "", а не панику) — библиотека настойчиво рекомендует геттеры.
- `switch { case errors.Is(...) }` — switch **без выражения** (как цепочка if-else, только компактнее): сопоставляем доменные ошибки с gRPC-кодами. Это «таможня» между двумя мирами: `ErrEmailTaken` → `codes.AlreadyExists`, `ErrInvalidInput` → `codes.InvalidArgument`, всё остальное → `codes.Internal` с **обтекаемым** сообщением "internal error" — детали (запрос SQL, адреса) не утекают клиенту.
- `status.Error(codes.X, "msg")` — способ вернуть ошибку по gRPC: код + сообщение доедут до клиента как есть; gateway потом переведёт код в HTTP-статус (5.5).
- Успех — собрать `RegisterResponse` из доменного юзера. `string(u.Role)` — конверсия доменного типа Role обратно в строку: домен не утекает в контракт.

**Login (68–77)** — тот же лекал: `ErrInvalidCredentials` → `codes.Unauthenticated` (401 на выходе), прочее → Internal. Ответ — оба токена.

**Refresh (79–85) + mapAuthErr (87–96)** — лекал вынесен в хелпер `mapAuthErr`: один и тот же перевод ошибок, чтобы не копипастить switch. Из-за этого Refresh не различает «отозван/истёк/не найден» — все три дают Unauthenticated (маскировка из 6.11 доезжает до клиента как есть).

### 6.16 internal/adapters/postgres/user_repo.go

```go
// Package postgres реализует адаптеры для работы с PostgreSQL.
// Содержит репозитории для пользователей (UserRepo) и refresh-токенов (RefreshRepo).
// Начиная с рефакторинга на sqlc сами SQL-запросы живут в queries/*.sql,
// а здесь остаётся только «перевод» между миром БД и миром домена:
// строки сгенерированных структур → доменные модели,
// ошибки Postgres → доменные ошибки (domain.ErrNotFound, domain.ErrEmailTaken).
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"mini-core/services/auth-service/internal/adapters/postgres/db"
	"mini-core/services/auth-service/internal/domain"
)

// UserRepo — структура, реализующая интерфейс app.UserRepository.
// Внутри — не пул, а объект Queries, сгенерированный sqlc:
// на каждый запрос из queries/users.sql у него есть типизированный метод.
type UserRepo struct {
	q *db.Queries
}

// NewUserRepo — конструктор для UserRepo.
// db.New принимает любой объект, умеющий Exec/Query/QueryRow —
// пул соединений pgxpool как раз такой, поэтому подходим мы им.
func NewUserRepo(pool *pgxpool.Pool) *UserRepo {
	return &UserRepo{q: db.New(pool)}
}

// Create сохраняет пользователя в таблицу users.
// Если возникает ошибка нарушения уникальности (код 23505),
// преобразует её в domain.ErrEmailTaken — выше по стеку никто
// не должен знать про коды ошибок Postgres.
func (r *UserRepo) Create(ctx context.Context, u *domain.User) error {
	err := r.q.CreateUser(ctx, db.CreateUserParams{
		ID:           u.ID,
		Email:        u.Email,
		PasswordHash: u.PasswordHash,
		Role:         string(u.Role), // доменный Role → строка для БД
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // 23505 = unique violation в Postgres
		return domain.ErrEmailTaken // перевод: «язык БД» → «язык домена»
	}
	return err
}

// FindByEmail ищет пользователя по нормализованному email.
// pgx.ErrNoRows («в БД пусто») превращается в domain.ErrNotFound.
func (r *UserRepo) FindByEmail(ctx context.Context, email string) (*domain.User, error) {
	row, err := r.q.GetUserByEmail(ctx, email)
	if err != nil {
		return nil, mapUserErr(err)
	}
	return rowToUser(row.ID, row.Email, row.PasswordHash, row.Role), nil
}

// FindByID — то же самое, но по первичному ключу.
func (r *UserRepo) FindByID(ctx context.Context, id string) (*domain.User, error) {
	row, err := r.q.GetUserByID(ctx, id)
	if err != nil {
		return nil, mapUserErr(err)
	}
	return rowToUser(row.ID, row.Email, row.PasswordHash, row.Role), nil
}

// rowToUser — маппинг плоской строки из БД в доменную модель.
// Роль хранится строкой, здесь она превращается обратно в domain.Role.
func rowToUser(id, email, passwordHash, role string) *domain.User {
	return &domain.User{
		ID:           id,
		Email:        email,
		PasswordHash: passwordHash,
		Role:         domain.Role(role),
	}
}

// mapUserErr — единый перевод ошибок драйвера в ошибки домена.
func mapUserErr(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound // перевод: «пусто в БД» → язык домена
	}
	return err
}
```

**Разбор.** Репозиторий — реализация порта хранилища. После перехода на sqlc он **тонкий**: SQL теперь в queries/*.sql (6.4) и сгенерированных методах db/ (6.18), а здесь остаётся только перевод в обе стороны: строки БД → доменные модели, ошибки драйвера → доменные ошибки.

```go
type UserRepo struct {
        q *db.Queries
}

func NewUserRepo(pool *pgxpool.Pool) *UserRepo {
        return &UserRepo{q: db.New(pool)}
}
```
Хранит не пул, а `*db.Queries` — объект с методами на каждый SQL-запрос. `db.New(pool)` принимает **любой** объект, умеющий Exec/Query/QueryRow (интерфейс `DBTX`, см. 6.18) — пул как раз такой. Сменить пул на транзакцию — `db.New(tx)`, и все запросы пойдут через неё.

**Create (39–51):**

```go
err := r.q.CreateUser(ctx, db.CreateUserParams{
        ID:           u.ID,
        Email:        u.Email,
        PasswordHash: u.PasswordHash,
        Role:         string(u.Role),
})
var pgErr *pgconn.PgError
if errors.As(err, &pgErr) && pgErr.Code == "23505" {
        return domain.ErrEmailTaken
}
return err
```
- Вызов сгенерированного метода со структурой параметров. `string(u.Role)` — конверсия доменного Role в строку для БД.
- `errors.As(err, &pgErr)` — «разверни ошибку: внутри есть ли *pgconn.PgError?». Почему `As`, а не `Is`: `PgError` — **структура** (не сентинел-значение), и мы хотим её получить, чтобы прочитать поле `Code`. `23505` — официальный код Postgres `unique_violation`: сработал UNIQUE-констрейнт на email (6.3). Переводим в `domain.ErrEmailTaken` — **выше по стеку никто не знает про коды Postgres**: сценарий и gRPC-адаптер оперируют только доменными ошибками. Это и есть «адаптер» в чистом виде: перевод словаря БД на язык домена.

**FindByEmail / FindByID (55–70):**

```go
row, err := r.q.GetUserByEmail(ctx, email)
if err != nil {
        return nil, mapUserErr(err)
}
return rowToUser(row.ID, row.Email, row.PasswordHash, row.Role), nil
```
Вызов → перевод ошибки → перевод строки в домен. Хелперы внизу файла:

```go
func rowToUser(id, email, passwordHash, role string) *domain.User {
        return &domain.User{
                ...
                Role:         domain.Role(role),
        }
}

func mapUserErr(err error) error {
        if errors.Is(err, pgx.ErrNoRows) {
                return domain.ErrNotFound
        }
        return err
}
```
`pgx.ErrNoRows` — драйвер возвращает её, когда QueryRow не нашёл ни одной строки. Для домена это «юзера нет» → `domain.ErrNotFound`. Оба хелпера используются и вторым репозиторием (6.17) — они в одном пакете.

### 6.17 internal/adapters/postgres/refresh_repo.go

```go
package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mini-core/services/auth-service/internal/adapters/postgres/db"
	"mini-core/services/auth-service/internal/domain"
)

// RefreshRepo — репозиторий для работы с refresh-токенами в PostgreSQL.
// Хранит хеши токенов, идентификатор пользователя и время истечения.
// Как и UserRepo, это тонкая обёртка над sqlc-пакетом db.
type RefreshRepo struct {
	q *db.Queries
}

// NewRefreshRepo — конструктор RefreshRepo.
func NewRefreshRepo(pool *pgxpool.Pool) *RefreshRepo {
	return &RefreshRepo{q: db.New(pool)}
}

// Create сохраняет хеш refresh-токена в таблицу refresh_tokens.
// Параметры:
//   - tokenHash — SHA-256 хеш токена (в hex)
//   - userID    — идентификатор пользователя
//   - expiresAt — время истечения (UTC)
//
// Возвращает ошибку, если вставка не удалась (например, нарушение внешнего ключа).
func (r *RefreshRepo) Create(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error {
	return r.q.CreateRefreshToken(ctx, db.CreateRefreshTokenParams{
		TokenHash: tokenHash,
		UserID:    userID,
		ExpiresAt: expiresAt, // override в sqlc.yaml: timestamptz → time.Time
	})
}

// FindByHash возвращает токен по хешу. Обратите внимание: RevokedAt
// в БД допускает NULL, поэтому в сгенерированной структуре это *time.Time —
// ровно тот же тип, что и в доменной модели, маппинг получается тривиальным.
func (r *RefreshRepo) FindByHash(ctx context.Context, hash string) (*domain.RefreshToken, error) {
	row, err := r.q.GetRefreshToken(ctx, hash)
	if err != nil {
		return nil, mapUserErr(err) // тот же перевод ErrNoRows → ErrNotFound
	}
	return &domain.RefreshToken{
		Hash:      row.TokenHash,
		UserID:    row.UserID,
		ExpiresAt: row.ExpiresAt,
		RevokedAt: row.RevokedAt,
	}, nil
}

// Revoke помечает токен отозванным (записывает время отзыва).
func (r *RefreshRepo) Revoke(ctx context.Context, hash string, at time.Time) error {
	return r.q.RevokeRefreshToken(ctx, db.RevokeRefreshTokenParams{
		RevokedAt: &at, // nullable-колонка → *time.Time
		TokenHash: hash,
	})
}
```

**Разбор.** Второй репозиторий, такой же тонкий. Три метода — Create / FindByHash / Revoke — зеркалят три SQL-запроса (6.5).

Интересные места:

- **Create:** `ExpiresAt: expiresAt` — поле типа `time.Time` благодаря override'у timestamptz→time.Time из sqlc.yaml (6.6); без него здесь был бы pgtype-тип.
- **FindByHash:** обращает внимание на nullable-колонку —

```go
return &domain.RefreshToken{
        Hash:      row.TokenHash,
        UserID:    row.UserID,
        ExpiresAt: row.ExpiresAt,
        RevokedAt: row.RevokedAt,
}, nil
```
`row.RevokedAt` имеет тип `*time.Time` (NULL в БД), и доменная модель тоже `*time.Time` — маппинг **тривиален**, копирование поля. Это не случайно: модель домена спроектирована с оглядкой на nullable-семантику (nil = активен, 6.8), а sqlc-override даёт тот же тип. Границы сойдутся, если обе стороны договорились об одном представлении отсутствия значения.
- **Revoke:** `RevokedAt: &at` — берём **адрес** локальной переменной (параметр `at time.Time`), потому что сгенерированная структура ждёт `*time.Time` для nullable-колонки. `TokenHash: hash` — WHERE по первичному ключу.

Заметь: `mapUserErr` вызывается и здесь (`return nil, mapUserErr(err)`) — общий переводчик одного пакета postgres, никакой дубликации.

### 6.18 internal/adapters/postgres/db/ — что сгенерировал sqlc

Папку db/ приводим не целиком (это механический код), а три его опорных файла и главные фрагменты — полный текст можно открыть рядом, он короткий и однотипный.

**db.go — «шов» между sqlc-кодом и драйвером:**

```go
type DBTX interface {
        Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
        Query(context.Context, string, ...interface{}) (pgx.Rows, error)
        QueryRow(context.Context, string, ...interface{}) pgx.Row
}

func New(db DBTX) *Queries { return &Queries{db: db} }
```
`DBTX` — интерфейс из трёх методов pgx-драйвера. Пул (`*pgxpool.Pool`) и транзакция (`pgx.Tx`) оба его реализуют, поэтому `db.New(...)` работает с чем угодно из них. `Queries` хранит DBTX и получает по методу на каждый запрос из queries/*.sql.

**models.go — структуры «строка таблицы»:**

```go
type User struct {
        ID           string
        Email        string
        PasswordHash string
        Role         string
        CustomerID   *uuid.UUID
        CreatedAt    time.Time
       UpdatedAt    time.Time
}
type RefreshToken struct {
        TokenHash string
        UserID    string
        ExpiresAt time.Time
        CreatedAt time.Time
        RevokedAt *time.Time
}
```
Все колонки таблицы, типы — с учётом override'ов: uuid→string, nullable uuid→`*uuid.UUID`, timestamptz→time.Time, nullable →`*time.Time`. Заметь: sqlc-модель шире доменной (в domain.User нет CreatedAt/CustomerID) — репозиторий копирует только нужные поля.

**users.sql.go — запросы как константы + методы:**

```go
const getUserByEmail = `-- name: GetUserByEmail :one
SELECT id, email, password_hash, role
FROM users
WHERE email = $1
`

func (q *Queries) GetUserByEmail(ctx context.Context, email string) (GetUserByEmailRow, error) {
        row := q.db.QueryRow(ctx, getUserByEmail, email)
        var i GetUserByEmailRow
        err := row.Scan(&i.ID, &i.Email, &i.PasswordHash, &i.Role)
        return i, err
}
```
Вот и весь «магический» sqlc: SQL остался строкой, но **типизированно обвязан** — параметры описаны сигнатурой, результат сканируется в структуру. Ошибка в имени колонки или типе выявится на `sqlc generate`, не в рантайме. В шапке каждого файла — `DO NOT EDIT`: правки руками будут затёрты следующей генерацией.

### 6.19 cmd/main.go

```go
// auth-service — учётные записи, JWT, события в Kafka.
// main только собирает зависимости по конфигу и запускает серверы:
// конфиг → Postgres → Kafka → ключи → JWKS (HTTP) → gRPC → graceful shutdown.
package main

import (
        "context"
        "log/slog"
        "net"
        "net/http"
        "os"
        "os/signal"
        "syscall"

        "github.com/jackc/pgx/v5/pgxpool"
        "google.golang.org/grpc"
        "google.golang.org/grpc/reflection"

        authv1 "mini-core/services/auth-service/gen/bank/auth/v1"
        "mini-core/services/auth-service/internal/adapters/grpcapi"
        "mini-core/services/auth-service/internal/adapters/jwks"
        "mini-core/services/auth-service/internal/adapters/jwtauth"
        kafkaauth "mini-core/services/auth-service/internal/adapters/kafka"
        "mini-core/services/auth-service/internal/adapters/postgres"
        "mini-core/services/auth-service/internal/app"
        "mini-core/services/auth-service/internal/config"
)

func main() {
        // Контекст отменяется по Ctrl+C / SIGTERM — сигнал остановки для всего.
        ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
        defer stop()

        log := slog.New(slog.NewTextHandler(os.Stdout, nil))
        cfg := config.Load() // ← все настройки в одной структуре

        // fatal — логирует ошибку и завершает процесс с кодом 1.
        // (В slog нет Fatalf, как в старом log, — оборачиваем сами.)
        fatal := func(msg string, err error) {
                log.Error(msg, "err", err)
                os.Exit(1)
        }

        // --- Postgres: пул соединений со СВОЕЙ базой ---
        pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
        if err != nil {
                fatal("postgres", err)
        }
        defer pool.Close()
        if err := pool.Ping(ctx); err != nil {
                fatal("postgres ping", err)
        }

        // --- Kafka: публикуем факт user_registered ---
        pub := kafkaauth.NewPublisher(cfg.KafkaBrokers)
        defer pub.Close()

        // --- Ключи: приватным подписываем токены, публичный отдаём через JWKS ---
        privPEM, err := os.ReadFile(cfg.PrivateKeyPath)
        if err != nil {
                fatal("jwt private key", err)
        }
        pubPEM, err := os.ReadFile(cfg.PublicKeyPath)
        if err != nil {
                fatal("jwt public key", err)
        }
        issuer, err := jwtauth.NewIssuer(privPEM)
        if err != nil {
                fatal("jwt issuer", err)
        }

        // --- JWKS: маленький HTTP-сервер, отдающий публичный ключ в формате JWKS ---
        jwksH, err := jwks.Handler(pubPEM)
        if err != nil {
                fatal("jwks", err)
        }
        httpSrv := &http.Server{Addr: cfg.JWKSAddr, Handler: http.HandlerFunc(jwksH)}
        go func() {
                if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                        log.Warn("jwks http", "err", err)
                }
        }()

        // --- gRPC: сборка всех слоёв (adapters ← app ← main внедряет зависимости) ---
        lis, err := net.Listen("tcp", cfg.GRPCAddr)
        if err != nil {
                fatal("listen", err)
        }
        srv := grpc.NewServer()

        userRepo := postgres.NewUserRepo(pool)
        refreshRepo := postgres.NewRefreshRepo(pool)
        authv1.RegisterAuthServiceServer(srv, grpcapi.New(
                app.NewRegister(userRepo, pub),
                app.NewLogin(userRepo, issuer, refreshRepo),
                app.NewRefresh(refreshRepo, userRepo, issuer, refreshRepo), // refreshRepo в двух ролях
        ))
        reflection.Register(srv)

        go func() {
                <-ctx.Done()
                log.Info("graceful shutdown...")
                srv.GracefulStop()
                _ = httpSrv.Shutdown(context.Background())
        }()

        log.Info("auth-service: gRPC " + cfg.GRPCAddr + ", JWKS " + cfg.JWKSAddr + "/jwks")
        if err := srv.Serve(lis); err != nil {
                fatal("serve", err)
        }
        log.Info("остановлен аккуратно")
}
```

**Разбор.** Сборочная точка auth-сервиса — самая «плотная» в проекте: именно здесь гексагональная диаграмма превращается в вызовы конструкторов. Порядок: `конфиг → Postgres → Kafka → ключи → JWKS (HTTP) → gRPC → graceful shutdown`.

**fatal-замыкание (39–42):**

```go
fatal := func(msg string, err error) {
        log.Error(msg, "err", err)
        os.Exit(1)
}
```
Не функция пакета, а **переменная-замыкание**: захватила `log` из окружения main. В gateway fatal был отдельной функцией с логгером-аргументом — оба варианта норм, здесь короче вызовы. (Про os.Exit и defer — см. 5.1.)

**Postgres (44–52):** `pgxpool.New(ctx, cfg.DatabaseURL)` — пул соединений; `pool.Ping(ctx)` — **единственная** проверка живости при старте: если БД недоступна, сервис честно падает сразу, а не после первого запроса. Причина падения будет в логе с request_id=0 — проще диагностика.

**Kafka (54–56):** `kafkaauth.NewPublisher(cfg.KafkaBrokers)` — тоже ленивый: реальное соединение при первом сообщении. Алиас импорта `kafkaauth` — чтобы не путать с gRPC-пакетами.

**Ключи (58–70):** читаем **оба** PEM-файла. Приватный — в `jwtauth.NewIssuer` (подписывать). Публичный пойдёт в JWKS (отдавать миру). Если файлов нет — сервис не стартует: правильно, без ключей он не может делать свою главную работу.

**JWKS (72–82):**

```go
jwksH, err := jwks.Handler(pubPEM)
httpSrv := &http.Server{Addr: cfg.JWKSAddr, Handler: http.HandlerFunc(jwksH)}
go func() {
        if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                log.Warn("jwks http", "err", err)
        }
}()
```
Мини-HTTP-сервер (один эндпоинт) запускается в **горутине** — main не ждёт его, он живёт параллельно. Ошибка слушателя — Warn, а не fatal: упавший JWKS не валит основной сервис (gateway в dev читает ключ из файла).

**gRPC и сборка зависимостей (84–98):**

```go
userRepo := postgres.NewUserRepo(pool)
refreshRepo := postgres.NewRefreshRepo(pool)
authv1.RegisterAuthServiceServer(srv, grpcapi.New(
        app.NewRegister(userRepo, pub),
        app.NewLogin(userRepo, issuer, refreshRepo),
        app.NewRefresh(refreshRepo, userRepo, issuer, refreshRepo),
))
```
Вот он — **граф зависимостей в четырёх строках**: репозитории поверх пула; сценарии поверх репозиториев и issuer'а; gRPC-сервер поверх сценариев; `RegisterAuthServiceServer(srv, ...)` — связка сгенерированного контракта с нашей реализацией. Смотри, как один `refreshRepo` занял **две роли** в `NewRefresh` (RefreshStore и RefreshWriter — 6.11): это тот самый эффект маленьких интерфейсов. `reflection.Register(srv)` — включает gRPC-reflection: `grpcurl -plaintext localhost:50051 list` покажет список RPC — бесценно для отладки.

**Graceful shutdown (100–105):** как в gateway — горутина ждёт ctx.Done, затем `srv.GracefulStop()` (перестать принимать новые RPC, дождаться текущих) и `httpSrv.Shutdown` для JWKS. После `srv.Serve(lis)` возвращается управление — main завершается, defer'ы (`pool.Close`, `pub.Close`) выполняются.

Заметь иерархию знаний: main знает всё; grpcapi знает app; app знает domain и порты; адаптеры знают db/драйверы и domain. **Никто не знает main.** Это и позволяет менять сборку, не трогая слои.

## 7. customer-service — каждый файл целиком

customer-service — владелец профилей и статусной машины (NEW → PROFILE_FILLED → ON_KYC → ACTIVE/REJECTED). У него **два входа**: gRPC (для gateway) и Kafka-консьюмер (для событий от auth). Это делает его хорошим примером «сервиса-интегратора».

### 7.1 proto/bank/customer/v1/customer.proto

```protobuf
syntax = "proto3";

package bank.customer.v1;

service CustomerService {
  rpc GetCustomer(GetCustomerRequest) returns (Customer);
  rpc UpdateProfile(UpdateProfileRequest) returns (Customer);
  rpc GetCustomerStatus(GetCustomerStatusRequest) returns (GetCustomerStatusResponse);
  rpc ListCustomers(ListCustomersRequest) returns (ListCustomersResponse);
}

message Customer {
  string customer_id = 1;
  string user_id = 2;
  string first_name = 3;
  string last_name = 4;
  string phone = 5;
  string date_of_birth = 6; // YYYY-MM-DD
  string address = 7;
  string citizenship = 8;
  string status = 9;
}

message GetCustomerRequest {
  string user_id = 1;
}

message UpdateProfileRequest {
  string user_id = 1;
  string first_name = 2;
  string last_name = 3;
  string phone = 4;
  string date_of_birth = 5;
  string address = 6;
  string citizenship = 7;
}

message GetCustomerStatusRequest {
  string user_id = 1;
}

message GetCustomerStatusResponse {
  string user_id = 1;
  string status = 2;
}

message ListCustomersRequest {
  int32 page_size = 1;
  string page_token = 2;   // это offset (просто число строкой)
  string status_filter = 3; // "", "NEW", "PROFILE_FILLED", ...
}

message ListCustomersResponse {
  repeated Customer items = 1;
  string next_page_token = 2;
}
```

**Разбор.** Четыре RPC — и по ним видно API-мышление:

- `GetCustomer(GetCustomerRequest) returns (Customer)` — **сам Customer — message-ответ**: геттер возвращает сущность целиком. `GetCustomerRequest` — крошечный (один user_id): запрос и ответ редко бывают симметричны, поэтому отдельные message на каждый.
- `UpdateProfile(UpdateProfileRequest) returns (Customer)` — обновили, вернули свежую сущность: клиенту не нужен второй запрос «покажи, что сохранилось».
- `GetCustomerStatus` — узкий ответ `{user_id, status}`: когда нужен только статус, гонять всю анкету расточительно.
- `ListCustomers(ListCustomersRequest) returns (ListCustomersResponse)` — пагинация по канонам AIP-151: `page_size`, `page_token` (строка — на будущее курсор), `status_filter`. Ответ: `repeated Customer items` (repeated = список) + `next_page_token` (пустая строка = страниц больше нет).

`Customer` message повторяет анкету: все поля string, `date_of_birth` — строка YYYY-MM-DD (про дату — 7.3). Композит `bank.customer.v1` — как и в auth: пакет = путь в gen/.

### 7.2 migrations/0001_customers.sql

```sql
-- +goose Up
CREATE TABLE customers (
    id            UUID PRIMARY KEY,
    user_id       UUID NOT NULL UNIQUE,
    first_name    TEXT NOT NULL DEFAULT '',
    last_name     TEXT NOT NULL DEFAULT '',
    phone         TEXT NOT NULL DEFAULT '',
    date_of_birth DATE,
    address       TEXT NOT NULL DEFAULT '',
    citizenship   TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'NEW',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE customers;
```

**Разбор.** Таблица профилей — деталь за деталью:

- `id UUID PRIMARY KEY` — свой UUID v7 (генерит customer-service, не auth!): каждый сервис владеет своими идентификаторами.
- `user_id UUID NOT NULL UNIQUE` — ссылка на пользователя **без** FOREIGN KEY: базы у сервисов разные, пользователи живут в auth_db. UNIQUE — гарантия «один профиль на юзера» на уровне БД (страховка идемпотентности, 7.10). Отсутствие FK — осознанное следствие database-per-service: FK через базы невозможен, целостность поддерживают процессы (событие приходит только после создания юзера).
- `first_name TEXT NOT NULL DEFAULT ''` и прочие анкетные поля — `NOT NULL DEFAULT ''`, а не nullable: пустое поле — пустая строка. Зачем: nullable-строки в Go превращаются в `*string`, и весь код обрастает проверками на nil. Пустая строка = «не заполнено» — проще и достаточно.
- `date_of_birth DATE` — **nullable и без default**: дата либо настоящая, либо NULL. Тип DATE (день без времени) — дню рождения время не нужно. Как она превращается в строку для домена — 7.3.
- `status TEXT NOT NULL DEFAULT 'NEW'` — статус строкой, стартовое значение NEW. (CHECK IN ('NEW',...) опять же был бы уместен — упрощение.)
- `created_at/updated_at` — те же TIMESTAMPTZ DEFAULT now().

### 7.3 queries/customers.sql

```sql
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
```

**Разбор.** Самый содержательный SQL-файл проекта: здесь решены три проблемы, которые встречаются в каждом реальном проекте.

**Проблема 1 — идемпотентная вставка:**

```sql
-- name: CreateCustomer :exec
INSERT INTO customers (id, user_id, status)
VALUES ($1, $2, $3)
ON CONFLICT (user_id) DO NOTHING;
```
`ON CONFLICT (user_id) DO NOTHING` — если профиль для этого юзера уже есть, вставка **молча не сделается** (и это не ошибка). Зачем: события Kafka доставляются «at-least-once» — дубликаты норма. Повторное `user_registered` не уронит консьюмер: вставка просто ничего не сделает. Идемпотентность переложена с кода на SQL — правильное место.

**Проблема 2 — дата: в БД DATE, в домене строка:**

```sql
coalesce(to_char(date_of_birth, 'YYYY-MM-DD'), '')::text AS date_of_birth
```
Чтение: `to_char` форматтурует DATE в строку, `coalesce(..., '')` превращает NULL в пустую строку (чтобы в домен не утекал указатель), `::text` — явный каст для sqlc (иначе выведет неочевидный тип). Итог: домен получает `DateOfBirth string`, пусто = «не заполнено». Запись — зеркально:

```sql
date_of_birth = to_date(nullif(@date_of_birth::text, ''), 'YYYY-MM-DD'),
```
`nullif(x, '')` — если пришла пустая строка → NULL; иначе `to_date` парсит по шаблону. Кривая строка (не дата) — ошибка INSERT, а не мусор в БД. Оба преобразования живут **в SQL**, домен вообще не знает про DATE.

**Проблема 3 — опциональный фильтр списка:**

```sql
-- name: ListCustomers :many
... LIMIT $1 OFFSET $2;

-- name: ListCustomersByStatus :many
... WHERE status = $1 ORDER BY created_at LIMIT $2 OFFSET $3;
```
**Два запроса вместо одного с условием** «если фильтр пуст — вернуть всех». Причины (подробно в комментарии файла): PostgreSQL не разрешает именованные параметры в LIMIT/OFFSET; смешивать именованные и позиционные sqlc не умеет; трюк `(@f = '' OR status = @f)` ещё и убивает индекс по status (планировщик не может предвычислить условие). Выбор в Go, какой метод звать, — три строки в репозитории (7.10). Урок: иногда два простых запроса лучше одного хитрого.

Именованные параметры (`@first_name`, `@user_id`) в UpdateCustomer — sqlc соберёт из них структуру `UpdateCustomerParams` с читаемыми полями вместо `Field3`.

### 7.4 internal/config/config.go

```go
// Package config — единственное место, где customer-service «знает про окружение».
// Все настройки собираются в структуру Config один раз при старте (функция Load),
// дальше main и остальной код читают только её. Источник — переменные окружения;
// для каждой есть значение по умолчанию для локальной разработки.
package config

import "os"

// Config — все настройки customer-service в одном месте.
type Config struct {
	// GRPCAddr — адрес gRPC-сервера (основной API сервиса).
	GRPCAddr string
	// DatabaseURL — строка подключения к своей БД (database per service).
	DatabaseURL string
	// KafkaBrokers — адреса брокеров Kafka через запятую.
	KafkaBrokers string
	// ConsumerGroup — имя consumer-группы Kafka (по нему хранятся офсеты).
	ConsumerGroup string
}

// Load читает переменные окружения и возвращает готовый Config.
func Load() Config {
	return Config{
		GRPCAddr:      envOr("CUSTOMER_GRPC_ADDR", ":50052"),
		DatabaseURL:   envOr("DATABASE_URL", "postgres://dev:dev@localhost:5432/customer_db?sslmode=disable"),
		KafkaBrokers:  envOr("KAFKA_BROKERS", "localhost:9092"),
		ConsumerGroup: envOr("KAFKA_GROUP", "customer-service"),
	}
}

// envOr возвращает значение переменной окружения или значение по умолчанию.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
```

**Разбор.** Тот же лекал, что и у остальных (5.2): единственное место про окружение, структура один раз при старте, дефолты для локального запуска. Специфика сервиса — два поля:

- `DatabaseURL` — своя база `customer_db` (database per service; обе базы в одном инстансе — см. 4.4).
- `ConsumerGroup: envOr("KAFKA_GROUP", "customer-service")` — **consumer group** — идентификатор группы консьюмеров. Kafka хранит офсеты (позицию чтения) по имени группы: перезапустился сервис — продолжит с того же места. Одинаковая группа у N экземпляров = нагрузка делится (каждая партиция уходит одному экземпляру); другая группа = другая «копия» чтения. Для этого сервиса группа одна — «customer-service».

### 7.5 internal/domain/customer.go

```go
package domain

import (
	"errors"
	"time"
)

type CustomerStatus string

const (
	StatusNew           CustomerStatus = "NEW"
	StatusProfileFilled CustomerStatus = "PROFILE_FILLED"
	StatusOnKyc         CustomerStatus = "ON_KYC"
	StatusActive        CustomerStatus = "ACTIVE"
	StatusRejected      CustomerStatus = "REJECTED"
	StatusBlocked       CustomerStatus = "BLOCKED"
)

var ErrNotFound = errors.New("customer not found")
var ErrProfileLocked = errors.New("profile locked: KYC started")

// allowedTransitions — таблица переходов статусной машины.
var allowedTransitions = map[CustomerStatus][]CustomerStatus{
	StatusNew:           {StatusProfileFilled, StatusBlocked},
	StatusProfileFilled: {StatusOnKyc, StatusBlocked},
	StatusOnKyc:         {StatusActive, StatusRejected, StatusBlocked},
	StatusActive:        {StatusBlocked},
	StatusRejected:      {},
	StatusBlocked:       {},
}

// CanTransition — можно ли перейти из одного статуса в другой.
func CanTransition(from, to CustomerStatus) bool {
	for _, s := range allowedTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Customer — профиль клиента.
type Customer struct {
	ID          string
	UserID      string
	FirstName   string
	LastName    string
	Phone       string
	DateOfBirth string // YYYY-MM-DD
	Address     string
	Citizenship string
	Status      CustomerStatus
}

// UpdateProfile — доменное правило: заполнять можно только до ON_KYC.
func (c *Customer) UpdateProfile(first, last, phone, dob, addr, citizenship string) error {
	if c.Status != StatusNew && c.Status != StatusProfileFilled {
		return ErrProfileLocked
	}
	c.FirstName, c.LastName, c.Phone = first, last, phone
	c.DateOfBirth, c.Address, c.Citizenship = dob, addr, citizenship
	if c.Status == StatusNew {
		if !CanTransition(c.Status, StatusProfileFilled) {
			return errors.New("bad transition") // защита на будущее
		}
		c.Status = StatusProfileFilled
	}
	return nil
}

// BirthDate — распарсенная дата рождения (для будущих проверок KYC).
func (c *Customer) BirthDate() (time.Time, error) {
	return time.Parse("2006-01-02", c.DateOfBirth)
}
```

**Разбор.** Самый интересный домен проекта: **статусная машина**.

```go
type CustomerStatus string

const (
        StatusNew           CustomerStatus = "NEW"
        StatusProfileFilled CustomerStatus = "PROFILE_FILLED"
        StatusOnKyc         CustomerStatus = "ON_KYC"
        StatusActive        CustomerStatus = "ACTIVE"
        StatusRejected      CustomerStatus = "REJECTED"
        StatusBlocked       CustomerStatus = "BLOCKED"
)
```
Статусы — тот же приём, что `Role` в 6.7: строковый тип + константы. Шесть состояний жизни клиента банка.

```go
var allowedTransitions = map[CustomerStatus][]CustomerStatus{
        StatusNew:           {StatusProfileFilled, StatusBlocked},
        StatusProfileFilled: {StatusOnKyc, StatusBlocked},
        StatusOnKyc:         {StatusActive, StatusRejected, StatusBlocked},
        StatusActive:        {StatusBlocked},
        StatusRejected:      {},
        StatusBlocked:       {},
}
```
**Таблица переходов как данные**, а не if-и: из NEW можно в PROFILE_FILLED или BLOCKED; из ACTIVE — только BLOCKED; REJECTED и BLOCKED — терминальные (пустой список). Сила решения: добавить/запретить переход = правка одной строки map; новая проверка «а можно ли из X в Y» = уже написанная функция. If-лес бы разросся с каждым статусом.

```go
func CanTransition(from, to CustomerStatus) bool {
        for _, s := range allowedTransitions[from] {
                if s == to {
                        return true
                }
        }
        return false
}
```
Проверка по таблице: цикл по разрешённым целям. `allowedTransitions[from]` для отсутствующего ключа вернёт nil-срез — цикл просто не выполнится, паники не будет (нулевые значения map в Go безопасны для чтения).

```go
func (c *Customer) UpdateProfile(first, last, phone, dob, addr, citizenship string) error {
        if c.Status != StatusNew && c.Status != StatusProfileFilled {
                return ErrProfileLocked
        }
        c.FirstName, c.LastName, c.Phone = first, last, phone
        c.DateOfBirth, c.Address, c.Citizenship = dob, addr, citizenship
        if c.Status == StatusNew {
                if !CanTransition(c.Status, StatusProfileFilled) {
                        return errors.New("bad transition") // защита на будущее
                }
                c.Status = StatusProfileFilled
        }
        return nil
}
```
**Бизнес-правило анкеты.** Метод-мутатор: принимает шесть полей, меняет объект (получатель — указатель `*Customer`, иначе изменения пропали бы в копии). Строки по смыслу: (1) писать анкету можно только пока NEW или PROFILE_FILLED — после подачи на KYC профиль заморожен (`ErrProfileLocked`); (2) присвоение полей — кортежное, шесть значений за два присваивания; (3) первый раз заполнил — статус NEW → PROFILE_FILLED, но **только через CanTransition**: belt-and-suspenders — даже если таблицу переходов кто-то испортит, метод не пропустит запрещённый переход. `errors.New("bad transition")` на месте — защитная ветка «на будущее», в живых путях недостижима.

```go
func (c *Customer) BirthDate() (time.Time, error) {
        return time.Parse("2006-01-02", c.DateOfBirth)
}
```
Разбор строки в дату для будущих KYC-проверок (возраст ≥ 18 и т.п.). Литерал `2006-01-02` — не случайные цифры: в Go шаблон даты — это **референсное время** 3 января 2006 (01:02:03, МСК-07): `2006-год, 01-месяц, 02-день`. Запоминается как «1 2 3 4 5 6 → месяц день час минута секунда год».

### 7.6 internal/domain/customer_test.go

```go
package domain

import (
	"errors"
	"testing"
)

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to CustomerStatus
		want     bool
	}{
		{StatusNew, StatusProfileFilled, true},
		{StatusProfileFilled, StatusOnKyc, true},
		{StatusOnKyc, StatusActive, true},
		{StatusOnKyc, StatusRejected, true},
		{StatusNew, StatusActive, false}, // перескок запрещён
		{StatusActive, StatusProfileFilled, false},
		{StatusRejected, StatusNew, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("%s → %s: got %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestUpdateProfile_LockedAfterKyc(t *testing.T) {
	c := &Customer{Status: StatusOnKyc}
	err := c.UpdateProfile("a", "b", "c", "2000-01-01", "d", "e")
	if !errors.Is(err, ErrProfileLocked) {
		t.Fatalf("ожидали ErrProfileLocked, получили: %v", err)
	}
}

func TestUpdateProfile_FillsFromNew(t *testing.T) {
	c := &Customer{Status: StatusNew}
	if err := c.UpdateProfile("Ivan", "Ivanov", "+995...", "1990-05-01", "Tbilisi", "GE"); err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if c.Status != StatusProfileFilled {
		t.Errorf("статус = %s, ожидали PROFILE_FILLED", c.Status)
	}
}
```

**Разбор.** Юнит-тесты домена — доказательство, что правило статусной машины живёт именно здесь и не требует инфраструктуры.

**Тест 1 — table-driven (канонический паттерн Go):**

```go
cases := []struct {
        from, to CustomerStatus
        want     bool
}{
        {StatusNew, StatusProfileFilled, true},
        ...
        {StatusNew, StatusActive, false}, // перескок запрещён
}
for _, c := range cases {
        if got := CanTransition(c.from, c.to); got != c.want {
                t.Errorf("%s → %s: got %v, want %v", c.from, c.to, got, c.want)
        }
}
```
Анонимная структура-описание кейса + срез кейсов + цикл. Добавить кейс = добавить строку, тест не «растёт». `t.Errorf` (не Fatalf) — пометить кейс проваленным и проверить остальные; `t.Fatalf` в других тестах — «дальше не имеет смысла».

**Тест 2 — замок:**

```go
c := &Customer{Status: StatusOnKyc}
err := c.UpdateProfile("a", "b", "c", "2000-01-01", "d", "e")
if !errors.Is(err, ErrProfileLocked) {
        t.Fatalf("ожидали ErrProfileLocked, получили: %v", err)
}
```
`&Customer{...}` — литерал структуры по указателю: состояние собрали руками, БД не нужна. `errors.Is` — сверка ошибки по значению (6.7). `t.Fatalf` — провал теста сразу (дальнейшие проверки бессмысленны).

**Тест 3 — happy path:** заполнение из NEW меняет статус на PROFILE_FILLED — прямая проверка третьей ветки `UpdateProfile`. Три теста, ноль моков — вот награда за чистый домен.

### 7.7 internal/app/customer.go

```go
package app

import (
	"context"
	"strconv"

	"github.com/google/uuid"

	"mini-core/services/customer-service/internal/domain"
)

// CustomerRepo — порт хранилища профилей.
type CustomerRepo interface {
	Create(ctx context.Context, c *domain.Customer) error // идемпотентен: повтор — не ошибка
	FindByUserID(ctx context.Context, userID string) (*domain.Customer, error)
	Update(ctx context.Context, c *domain.Customer) error
	List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Customer, error)
}

// CustomerService — сценарии профиля.
type CustomerService struct {
	repo CustomerRepo
}

func NewCustomer(repo CustomerRepo) *CustomerService {
	return &CustomerService{repo: repo}
}

// EnsureProfile — создаёт пустой профиль по событию user_registered.
// Идемпотентно: дубликат события не ошибка (at-least-once!).
func (s *CustomerService) EnsureProfile(ctx context.Context, userID string) error {
	if _, err := s.repo.FindByUserID(ctx, userID); err == nil {
		return nil // уже есть — нормально
	}
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	return s.repo.Create(ctx, &domain.Customer{
		ID: id.String(), UserID: userID, Status: domain.StatusNew,
	})
}

func (s *CustomerService) Get(ctx context.Context, userID string) (*domain.Customer, error) {
	return s.repo.FindByUserID(ctx, userID)
}

func (s *CustomerService) UpdateProfile(ctx context.Context, userID, first, last, phone, dob, addr, citizenship string) (*domain.Customer, error) {
	c, err := s.repo.FindByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := c.UpdateProfile(first, last, phone, dob, addr, citizenship); err != nil {
		return nil, err // ErrProfileLocked и т.п. — наверх
	}
	if err := s.repo.Update(ctx, c); err != nil {
		return nil, err
	}
	return c, nil
}

func (s *CustomerService) List(ctx context.Context, pageSize int, pageToken, statusFilter string) ([]domain.Customer, int, error) {
	offset := 0
	if pageToken != "" {
		// в реале — курсор; для учёбы offset, но с защитой
		n, err := strconv.Atoi(pageToken)
		if err != nil || n < 0 {
			return nil, 0, domain.ErrNotFound
		}
		offset = n
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	items, err := s.repo.List(ctx, pageSize, offset, statusFilter)
	return items, offset + len(items), err
}
```

**Разбор.** Сценарии профиля. Обрати внимание на порт:

```go
type CustomerRepo interface {
        Create(ctx context.Context, c *domain.Customer) error // идемпотентен: повтор — не ошибка
        FindByUserID(ctx context.Context, userID string) (*domain.Customer, error)
        Update(ctx context.Context, c *domain.Customer) error
        List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Customer, error)
}
```
Четыре метода — и комментарий прямо в интерфейсе: **Create идемпотентен**. Контракт с консьюмером зафиксирован в типе: тот может звать Create сколько угодно раз, дубликата не будет.

**EnsureProfile (31–42) — «создать перса»:**

```go
func (s *CustomerService) EnsureProfile(ctx context.Context, userID string) error {
        if _, err := s.repo.FindByUserID(ctx, userID); err == nil {
                return nil // уже есть — нормально
        }
        id, err := uuid.NewV7()
        if err != nil {
                return err
        }
        return s.repo.Create(ctx, &domain.Customer{
                ID: id.String(), UserID: userID, Status: domain.StatusNew,
        })
}
```
Проверка «есть ли уже» → если есть, тихо выходим (дубликат события — не ошибка). Если нет — создаём **пустой** профиль: только id, user_id и статус NEW, все анкетные поля — дефолты. Гонка возможна (два экземпляра одновременно «не нашли» и создают), но SQL `ON CONFLICT DO NOTHING` (7.3) замыкает защиту: одна строка в таблице. Три уровня защиты: проверка в сценарии, UNIQUE в БД, идемпотентный INSERT — каждый покрывает свою дырку.

**UpdateProfile (48–60) — паттерн «найти → применить правило → сохранить»:**

```go
c, err := s.repo.FindByUserID(ctx, userID)
if err != nil {
        return nil, err
}
if err := c.UpdateProfile(first, last, phone, dob, addr, citizenship); err != nil {
        return nil, err // ErrProfileLocked и т.п. — наверх
}
if err := s.repo.Update(ctx, c); err != nil {
        return nil, err
}
return c, nil
```
Сценарий **не содержит** правила «можно ли менять» — он грузит сущность и зовёт её доменный метод (7.5). Так правило тестируется в домене, а сценарий остаётся оркестратором. `ErrProfileLocked` не перехватывается — он наверх: адаптер переведёт его в FailedPrecondition (7.9). Нет профиля → ErrNotFound из репозитория → NotFound.

**List (62–77) — пагинация и её защита:**

```go
offset := 0
if pageToken != "" {
        n, err := strconv.Atoi(pageToken)
        if err != nil || n < 0 {
                return nil, 0, domain.ErrNotFound
        }
        offset = n
}
if pageSize <= 0 || pageSize > 100 {
        pageSize = 20
}
items, err := s.repo.List(ctx, pageSize, offset, statusFilter)
return items, offset + len(items), err
```
pageToken — просто offset строкой (учебное упрощение; в проде — курсор). `strconv.Atoi` — строка в int; не число или отрицательное → отказ. pageSize зажимается в [1..100], дефолт 20 — защита от « pageSize=1000000» и от нуля. Возвращаемое `offset + len(items)` — следующий токен: сколько строк уже показано. Если строк вернулось меньше pageSize — next-токен укажет за пределы выборки, клиент по нему получит пустой список и остановится.

### 7.8 internal/adapters/kafka/consumer.go

```go
package kafka

import (
        "context"
        "encoding/json"
        "log"
        "time"

        kafkago "github.com/segmentio/kafka-go"

        "mini-core/services/customer-service/internal/app"
)

// Envelope — формат событий из ТЗ.
type Envelope struct {
        EventID    string          `json:"event_id"`
        EventType  string          `json:"event_type"`
        OccurredAt string          `json:"occurred_at"`
        Payload    json.RawMessage `json:"payload"`
}

type userRegisteredPayload struct {
        UserID string `json:"user_id"`
}

type Consumer struct {
        reader    *kafkago.Reader
        customers *app.CustomerService
}

func NewConsumer(brokers, group string, svc *app.CustomerService) *Consumer {
        return &Consumer{
                reader: kafkago.NewReader(kafkago.ReaderConfig{
                        Brokers: []string{brokers},
                        GroupID: group, // группа: офсеты хранятся на брокере
                        Topic:   "auth.events",
                }),
                customers: svc,
        }
}

// Run блокируется до отмены ctx. Обязателен в отдельной горутине.
func (c *Consumer) Run(ctx context.Context) {
        for {
                m, err := c.reader.FetchMessage(ctx) // читаем БЕЗ коммита
                if err != nil {
                        if ctx.Err() != nil {
                                return // штатная остановка
                        }
                        log.Printf("kafka fetch: %v", err)
                        time.Sleep(time.Second)
                        continue
                }

                if err := c.handle(ctx, m.Value); err != nil {
                        // В учебном проекте: логируем и коммитим, чтобы не зависнуть.
                        // TODO(прод): retry с backoff + DLQ-топик (требование ТЗ к консьюмерам)
                        log.Printf("handle event: %v", err)
                }
                if err := c.reader.CommitMessages(ctx, m); err != nil {
                        log.Printf("kafka commit: %v", err)
                }
        }
}

func (c *Consumer) handle(ctx context.Context, data []byte) error {
        var env Envelope
        if err := json.Unmarshal(data, &env); err != nil {
                return err // ядовитое сообщение
        }
        log.Printf("событие: %s (id=%s)", env.EventType, env.EventID)
        if env.EventType != "user_registered" {
                return nil // не наше — пропускаем
        }
        var p userRegisteredPayload
        if err := json.Unmarshal(env.Payload, &p); err != nil {
                return err
        }
        return c.customers.EnsureProfile(ctx, p.UserID)
}

func (c *Consumer) Close() error { return c.reader.Close() }
```

**Разбор.** Второй вход сервиса: события из топика `auth.events`. Консьюмер — вечный цикл «читать → обработать → подтвердить».

**Reader (31–40):**

```go
reader: kafkago.NewReader(kafkago.ReaderConfig{
        Brokers: []string{brokers},
        GroupID: group, // группа: офсеты хранятся на брокере
        Topic:   "auth.events",
}),
```
GroupID — консьюмер-группа (7.4): офсеты читаются/пишутся на брокере, рестарт продолжает с последнего подтверждённого места. Reader — «ленивый», реальное соединение при первом FetchMessage.

**Run (43–64) — главный цикл:**

```go
m, err := c.reader.FetchMessage(ctx) // читаем БЕЗ коммита
if err != nil {
        if ctx.Err() != nil {
                return // штатная остановка
        }
        log.Printf("kafka fetch: %v", err)
        time.Sleep(time.Second)
        continue
}
```
`FetchMessage(ctx)` блокируется, пока не придёт сообщение (или не отменится ctx). Проверка `ctx.Err() != nil` — как отличить «брокер умер» от «мы останавливаемся»: при graceful shutdown fetch вернёт ошибку из-за отменённого контекста — и это нормальный выход из вечного цикла, а не сбой. Реальная ошибка — лог и пауза в секунду (не долбить брокер), продолжаем.

```go
if err := c.handle(ctx, m.Value); err != nil {
        log.Printf("handle event: %v", err)
}
if err := c.reader.CommitMessages(ctx, m); err != nil {
        log.Printf("kafka commit: %v", err)
}
```
**Порядок: обработать → потом закоммитить.** Коммит после обработки — это семантика **at-least-once**: если процесс умрёт между handle и commit, сообщение не подтверждено и после рестарта придёт **снова**. Поэтому обработчик обязан быть идемпотентным (EnsureProfile именно такой, 7.7). Обратный порядок (коммит → обработка) дал бы at-most-once: умрём после коммита — событие потеряно навсегда; для создания профиля это хуже дубликата.

Ошибка обработки (7.9, «ядовитое» сообщение) — логируем и **всё равно коммитим**: учебный консьюмер не должен зависнуть на битом сообщении навсегда (в проде — retry с backoff + DLQ, раздел 12, п. 3). TODO прямо в коде.

**handle (66–80):**

```go
var env Envelope
if err := json.Unmarshal(data, &env); err != nil {
        return err // ядовитое сообщение
}
log.Printf("событие: %s (id=%s)", env.EventType, env.EventID)
if env.EventType != "user_registered" {
        return nil // не наше — пропускаем
}
var p userRegisteredPayload
if err := json.Unmarshal(env.Payload, &p); err != nil {
        return err
}
return c.customers.EnsureProfile(ctx, p.UserID)
```
Двойной Unmarshal: сначала конверт, потом payload (Envelope.Payload — json.RawMessage, «сырой» вложенный JSON, 6.14). Фильтр по EventType — топик общий, события других типов пропускаем без ошибки. В конце — вызов сценария: консьюмер не пишет SQL и не знает про статусы, он просто «переводит событие в вызов сценария» — тот же принцип адаптера, что и у gRPC.

### 7.9 internal/adapters/grpcapi/server.go

```go
package grpcapi

import (
        "context"
        "errors"
        "strconv"

        "google.golang.org/grpc/codes"
        "google.golang.org/grpc/status"

        customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
        "mini-core/services/customer-service/internal/app"
        "mini-core/services/customer-service/internal/domain"
)

// Server реализует контракт CustomerService. Переводчик proto ↔ use case.
type Server struct {
        customerv1.UnimplementedCustomerServiceServer
        svc *app.CustomerService
}

func New(svc *app.CustomerService) *Server {
        return &Server{svc: svc}
}

func mapErr(err error) error {
        switch {
        case errors.Is(err, domain.ErrNotFound):
                return status.Error(codes.NotFound, "customer not found")
        case errors.Is(err, domain.ErrProfileLocked):
                return status.Error(codes.FailedPrecondition, "нельзя менять профиль после подачи на KYC")
        case err != nil:
                return status.Error(codes.Internal, "internal error")
        }
        return nil
}

func toProto(c *domain.Customer) *customerv1.Customer {
        return &customerv1.Customer{
                CustomerId:  c.ID,
                UserId:      c.UserID,
                FirstName:   c.FirstName,
                LastName:    c.LastName,
                Phone:       c.Phone,
                DateOfBirth: c.DateOfBirth,
                Address:     c.Address,
                Citizenship: c.Citizenship,
                Status:      string(c.Status),
        }
}

func (s *Server) GetCustomer(ctx context.Context, req *customerv1.GetCustomerRequest) (*customerv1.Customer, error) {
        c, err := s.svc.Get(ctx, req.GetUserId())
        if err != nil {
                return nil, mapErr(err)
        }
        return toProto(c), nil
}

func (s *Server) UpdateProfile(ctx context.Context, req *customerv1.UpdateProfileRequest) (*customerv1.Customer, error) {
        c, err := s.svc.UpdateProfile(ctx, req.GetUserId(),
                req.GetFirstName(), req.GetLastName(), req.GetPhone(),
                req.GetDateOfBirth(), req.GetAddress(), req.GetCitizenship())
        if err != nil {
                return nil, mapErr(err)
        }
        return toProto(c), nil
}

func (s *Server) GetCustomerStatus(ctx context.Context, req *customerv1.GetCustomerStatusRequest) (*customerv1.GetCustomerStatusResponse, error) {
        c, err := s.svc.Get(ctx, req.GetUserId())
        if err != nil {
                return nil, mapErr(err)
        }
        return &customerv1.GetCustomerStatusResponse{UserId: c.UserID, Status: string(c.Status)}, nil
}

func (s *Server) ListCustomers(ctx context.Context, req *customerv1.ListCustomersRequest) (*customerv1.ListCustomersResponse, error) {
        items, nextOffset, err := s.svc.List(ctx, int(req.GetPageSize()), req.GetPageToken(), req.GetStatusFilter())
        if err != nil {
                return nil, mapErr(err)
        }
        resp := &customerv1.ListCustomersResponse{NextPageToken: strconv.Itoa(nextOffset)}
        for i := range items {
                resp.Items = append(resp.Items, toProto(&items[i]))
        }
        return resp, nil
}
```

**Разбор.** gRPC-адаптер, лекал как в auth (6.15): Unimplemented-встраивание, сценарий в поле, перевод ошибок и структур.

**mapErr (26–36):**

```go
case errors.Is(err, domain.ErrNotFound):
        return status.Error(codes.NotFound, "customer not found")
case errors.Is(err, domain.ErrProfileLocked):
        return status.Error(codes.FailedPrecondition, "нельзя менять профиль после подачи на KYC")
case err != nil:
        return status.Error(codes.Internal, "internal error")
```
Новые коды против auth: `FailedPrecondition` (HTTP 412) — «запрос валиден, но текущее состояние не позволяет» — точное название для замороженного профиля. Не гугли «какой код взять» — есть смысловая карта: InvalidArgument = «плохой ввод», FailedPrecondition = «плохое состояние», AlreadyExists = «конфликт уникальности», NotFound = «нет объекта».

**toProto (38–50)** — маппер домен → контракт: девять полей, `Status: string(c.Status)` — конверсия типа на границе. Зеркальный аналог rowToUser (6.16): домен не утекает ни в SQL, ни в wire-формат.

**GetCustomer (52–58) / UpdateProfile (60–68)** — «распаковать req.Get*() → вызвать сценарий → toProto или mapErr». GetCustomer возвращает `*customerv1.Customer` прямо — message-ответ (7.1).

**GetCustomerStatus (70–76)** — узкий ответ из двух полей: сценарий тот же (`svc.Get`), но наружу отдаём только статус — адаптер решает, что показывать.

**ListCustomers (78–88):**

```go
items, nextOffset, err := s.svc.List(ctx, int(req.GetPageSize()), req.GetPageToken(), req.GetStatusFilter())
...
resp := &customerv1.ListCustomersResponse{NextPageToken: strconv.Itoa(nextOffset)}
for i := range items {
        resp.Items = append(resp.Items, toProto(&items[i]))
}
return resp, nil
```
`int(req.GetPageSize())` — proto int32 → Go int на границе (зеркально конверсии в репозитории, 7.10). `strconv.Itoa(nextOffset)` — обратное превращение next-токена в строку. Цикл `for i := range items` с `&items[i]`: range **копирует** элемент в переменную — `toProto(&items[i])` берёт адрес оригинала (можно и `toProto` со значением — но у нас маппер принимает указатель). `append` собирает repeated-поле.

### 7.10 internal/adapters/postgres/customer_repo.go

```go
// Package postgres реализует адаптер для работы с PostgreSQL.
// CustomerRepo — репозиторий профилей клиентов. Сам SQL живёт в
// queries/customers.sql и превращён sqlc в типизированные методы пакета db;
// здесь остаётся маппинг строк БД в доменные модели и ошибок — в доменные.
package postgres

import (
        "context"
        "errors"

        "github.com/jackc/pgx/v5"
        "github.com/jackc/pgx/v5/pgxpool"

        "mini-core/services/customer-service/internal/adapters/postgres/db"
        "mini-core/services/customer-service/internal/domain"
)

type CustomerRepo struct {
        q *db.Queries // сгенерированные sqlc-методы вместо «сырых» строк SQL
}

func NewCustomerRepo(pool *pgxpool.Pool) *CustomerRepo {
        return &CustomerRepo{q: db.New(pool)} // пул подходит: умеет Exec/Query/QueryRow
}

// toCustomer — маппинг плоской строки из БД в доменную модель.
// Все три Row-структуры sqlc (Get/Update/List) содержат одинаковые
// поля, поэтому одна функция-помощник подходит всем методам.
// Статус хранится строкой, здесь возвращаем ему доменный тип.
func toCustomer(id, userID, firstName, lastName, phone, dob, address, citizenship, status string) *domain.Customer {
        return &domain.Customer{
                ID:          id,
                UserID:      userID,
                FirstName:   firstName,
                LastName:    lastName,
                Phone:       phone,
                DateOfBirth: dob, // уже строка YYYY-MM-DD: to_char в SQL
                Address:     address,
                Citizenship: citizenship,
                Status:      domain.CustomerStatus(status),
        }
}

// Create вставляет пустой профиль (только id, user_id, статус NEW).
// Идемпотентность обеспечивает сам SQL: ON CONFLICT (user_id) DO NOTHING,
// поэтому повторное событие из Kafka не будет ошибкой.
func (r *CustomerRepo) Create(ctx context.Context, c *domain.Customer) error {
        return r.q.CreateCustomer(ctx, db.CreateCustomerParams{
                ID:     c.ID,
                UserID: c.UserID,
                Status: string(c.Status),
        })
}

func (r *CustomerRepo) FindByUserID(ctx context.Context, userID string) (*domain.Customer, error) {
        row, err := r.q.GetCustomerByUserID(ctx, userID)
        if err != nil {
                return nil, mapCustomerErr(err)
        }
        return toCustomer(row.ID, row.UserID, row.FirstName, row.LastName, row.Phone,
                row.DateOfBirth, row.Address, row.Citizenship, row.Status), nil
}

// Update перезаписывает анкету. Пустая дата рождения — не проблема:
// в SQL nullif превратит её в NULL ещё до to_date.
func (r *CustomerRepo) Update(ctx context.Context, c *domain.Customer) error {
        return r.q.UpdateCustomer(ctx, db.UpdateCustomerParams{
                UserID:      c.UserID,
                FirstName:   c.FirstName,
                LastName:    c.LastName,
                Phone:       c.Phone,
                DateOfBirth: c.DateOfBirth,
                Address:     c.Address,
                Citizenship: c.Citizenship,
                Status:      string(c.Status),
        })
}

// List возвращает страницу профилей. Если statusFilter пуст — берём
// запрос без фильтра, иначе с фильтром (два метода sqlc вместо одного
// с хитрым условием; см. комментарии в queries/customers.sql).
// Приложение считает limit/offset в int, sqlc сгенерировал int32 —
// конвертация происходит на границе, как и положено адаптеру.
func (r *CustomerRepo) List(ctx context.Context, limit, offset int, statusFilter string) ([]domain.Customer, error) {
        lim, off := int32(limit), int32(offset)
        var (
                rows []db.ListCustomersRow
                err  error
        )
        if statusFilter == "" {
                rows, err = r.q.ListCustomers(ctx, db.ListCustomersParams{Limit: lim, Offset: off})
        } else {
                var byStatus []db.ListCustomersByStatusRow
                if byStatus, err = r.q.ListCustomersByStatus(ctx, db.ListCustomersByStatusParams{
                        Status: statusFilter, Limit: lim, Offset: off,
                }); err != nil {
                        return nil, mapCustomerErr(err)
                }
                out := make([]domain.Customer, 0, len(byStatus))
                for _, row := range byStatus {
                        c := toCustomer(row.ID, row.UserID, row.FirstName, row.LastName, row.Phone,
                                row.DateOfBirth, row.Address, row.Citizenship, row.Status)
                        out = append(out, *c)
                }
                return out, nil
        }
        if err != nil {
                return nil, mapCustomerErr(err)
        }
        out := make([]domain.Customer, 0, len(rows))
        for _, row := range rows {
                c := toCustomer(row.ID, row.UserID, row.FirstName, row.LastName, row.Phone,
                        row.DateOfBirth, row.Address, row.Citizenship, row.Status)
                out = append(out, *c)
        }
        return out, nil
}

// mapCustomerErr — перевод ошибок драйвера в ошибки домена.
// pgx.ErrNoRows означает «запрос не нашёл ни одной строки» —
// для домена это просто «клиент не найден».
func mapCustomerErr(err error) error {
        if errors.Is(err, pgx.ErrNoRows) {
                return domain.ErrNotFound
        }
        return err
}
```

**Разбор.** Репозиторий, как и в auth — тонкая обёртка над db.Queries. Но здесь есть две специфичные истории.

**toCustomer с девятью аргументами (30–42):**

```go
func toCustomer(id, userID, firstName, lastName, phone, dob, address, citizenship, status string) *domain.Customer {
```
Почему так, а не принять Row-структуру? sqlc генерирует **отдельную** Row-структуру на каждый SELECT (GetCustomerByUserIDRow, ListCustomersRow, ListCustomersByStatusRow) — у них одинаковые поля, но это разные типы, общий параметр не примешь. Три варианта: дублировать маппинг трижды; генерить view-запрос с одинаковым типом; или хелпер с набором полей. Выбран третий — одна функция, вызывается из трёх мест, поля передаются позиционно. Громоздко, но честно и без дублирования логики.

**List с двумя ветками (84–117):**

```go
lim, off := int32(limit), int32(offset)
if statusFilter == "" {
        rows, err = r.q.ListCustomers(ctx, db.ListCustomersParams{Limit: lim, Offset: off})
} else {
        var byStatus []db.ListCustomersByStatusRow
        if byStatus, err = r.q.ListCustomersByStatus(...); err != nil {
                return nil, mapCustomerErr(err)
        }
        ... цикл, маппинг, return
}
if err != nil {
        return nil, mapCustomerErr(err)
}
... цикл, маппинг, return
```
Ровно то, о чём договаривались в 7.3: пустой фильтр → метод без WHERE; непустой → метод с WHERE. `int32(limit)` — конверсия на границе: приложение считает в int, sqlc вывел int32 (LIMIT в Postgres). Две почти одинаковые ветки с циклами — плата за «два запроса вместо хитрого одного»; сознательный компромисс, в проде его бы украсили генериком или переиспользованием маппера по полям.

**Create (47–53)** — без всяких проверок: идемпотентность внутри SQL (ON CONFLICT, 7.3). Репозиторий просто вызывает — контракт порта (7.7) выполнен.

**mapCustomerErr (122–127)** — тот же перевод `pgx.ErrNoRows → domain.ErrNotFound`, что и в auth. Не «вынеси в общий пакет» — у каждого сервиса свои ошибки и свои адаптеры; совпадение строчек кода при независимых контрактах — нормально.

### 7.11 internal/adapters/postgres/db/ — сгенерировано sqlc

Состав как в auth (6.18): `db.go` (DBTX + Queries), `models.go` (строка таблицы customers), `customers.sql.go` (четыре запроса). Фрагмент для чувства типов:

```go
func (q *Queries) GetCustomerByUserID(ctx context.Context, userID string) (GetCustomerByUserIDRow, error)
```
`date_of_birth` в Row — **string** (to_char в SQL, 7.3): sqlc доверяет выходному типу колонки после каста. Именно поэтому домен получает строку без единого преобразования в Go.

`UpdateCustomerParams` собран из именованных параметров `@first_name...@user_id` — поля структуры читаемы, порядок не важен (структурный литерал с именами).

### 7.12 cmd/main.go

```go
// customer-service — профили клиентов, статусная машина, консьюмер Kafka.
// main только собирает зависимости по конфигу и запускает серверы:
// конфиг → Postgres → use cases → Kafka-консьюмер (фон) → gRPC → graceful shutdown.
package main

import (
        "context"
        "log/slog"
        "net"
        "os"
        "os/signal"
        "syscall"

        "github.com/jackc/pgx/v5/pgxpool"
        "google.golang.org/grpc"
        "google.golang.org/grpc/reflection"

        customerv1 "mini-core/services/customer-service/gen/bank/customer/v1"
        "mini-core/services/customer-service/internal/adapters/grpcapi"
        appkafka "mini-core/services/customer-service/internal/adapters/kafka"
        "mini-core/services/customer-service/internal/adapters/postgres"
        "mini-core/services/customer-service/internal/app"
        "mini-core/services/customer-service/internal/config"
)

func main() {
        ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
        defer stop()

        log := slog.New(slog.NewTextHandler(os.Stdout, nil))
        cfg := config.Load() // ← все настройки в одной структуре

        // fatal — логирует ошибку и завершает процесс с кодом 1.
        // (В slog нет Fatalf, как в старом log, — оборачиваем сами.)
        fatal := func(msg string, err error) {
                log.Error(msg, "err", err)
                os.Exit(1)
        }

        // --- Postgres: пул соединений со СВОЕЙ базой ---
        pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
        if err != nil {
                fatal("postgres", err)
        }
        defer pool.Close()
        if err := pool.Ping(ctx); err != nil {
                fatal("postgres ping", err)
        }

        // --- Сценарии (use cases) поверх репозитория ---
        svc := app.NewCustomer(postgres.NewCustomerRepo(pool))

        // --- Консьюмер Kafka: слушает user_registered и создаёт пустые профили ---
        consumer := appkafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, svc)
        go consumer.Run(ctx)

        // --- gRPC ---
        lis, err := net.Listen("tcp", cfg.GRPCAddr)
        if err != nil {
                fatal("listen", err)
        }
        srv := grpc.NewServer()
        customerv1.RegisterCustomerServiceServer(srv, grpcapi.New(svc))
        reflection.Register(srv)

        go func() {
                <-ctx.Done()
                log.Info("graceful shutdown...")
                srv.GracefulStop()
                _ = consumer.Close()
        }()

        log.Info("customer-service: gRPC " + cfg.GRPCAddr)
        if err := srv.Serve(lis); err != nil {
                fatal("serve", err)
        }
}
```

**Разбор.** Сборка сервиса с двумя входами. Отличия от auth-main (6.19) — в двух местах.

**Сборка зависимостей (50–55):**

```go
svc := app.NewCustomer(postgres.NewCustomerRepo(pool))

consumer := appkafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, svc)
go consumer.Run(ctx)
```
Одна строка графа: репозиторий → сценарий. Затем consumer получает **сценарий** (не репозиторий!) — консьюмер адаптера kafka зависит от app, как и gRPC-адаптер: оба входа входят в одни и те же сценарии, бизнес-правило одно, какого входа нет — неважно. `go consumer.Run(ctx)` — **обязательно в горутине**: Run вечный цикл, он заблокирует main, и gRPC-сервер никогда не поднимется. ctx прокинут внутрь: по сигналу цикл выйдет (7.8).

**Shutdown (66–71):**

```go
go func() {
        <-ctx.Done()
        log.Info("graceful shutdown...")
        srv.GracefulStop()
        _ = consumer.Close()
}()
```
Две остановки: gRPC (GracefulStop — дождаться текущих RPC) и консьюмер (`reader.Close` — закроет соединение; сам Run уже выйдет по ctx). Два входа — два корректных прощания. После `srv.Serve(lis)` вернётся управление, main завершится.

**Итого по customer-service:** домен со статусной машиной (тестируется без БД), сценарии-оркестраторы, два адаптера-входа (gRPC + Kafka) и один выход (Postgres через sqlc), main собирает граф. Каждый слой зависит только «вниз» — ровно та гексагональная картинка, которую рисуют в архитектурных докладах, только в коде.

## 8. Сгенерированный код: protobuf и sqlc

Это самый важный раздел для понимания проекта. Вопрос, который он закрывает: **«из папок `gen/` и `internal/adapters/postgres/db/` вывалилось пять тысяч строк — что из этого читать, а что просто использовать?»** Короткий ответ: читать не надо ничего, использовать — четыре вещи. Раскроем.

### 8.1 Зачем вообще кодогенерация

Контракт между gateway и auth-service нужно описать один раз и на нейтральном языке — это `auth.proto`. Из него **генерируется** код для обеих сторон: структуры запросов/ответов (с сериализацией в бинарный protobuf-формат), клиент, который сам упакует вызов в сеть, и серверный каркас, который сам распакует. Без генерации пришлось бы руками писать: JSON-кодирование в байты, разбор, маршрутизацию по методам, обработку частичных сбоев сети — в обоих сервисах, синхронно, без ошибок. Кодогенерация — это не «магия», это экономия рутины.

Тот же принцип в базе: SQL-запрос описан в `queries/*.sql` — генерируется Go-метод с правильными типами. Мы ни разу не пишем `rows.Scan(&i.ID, &i.Email, ...)` руками.

Правило одно для обеих технологий: **сгенерированные файлы не редактируются никогда**. Хочешь изменить поведение — меняешь исходник (.proto или .sql) и запускаешь генератор. Это гарантирует, что код всегда соответствует контракту.

### 8.2 Что лежит в services/auth-service/gen/bank/auth/v1/

Два файла, оба созданы `buf generate` из `proto/bank/auth/v1/auth.proto`:

- **auth.pb.go** — из `auth.proto` целиком: структуры всех message + акцессоры + сериализация.
- **auth_grpc.pb.go** — из секции `service AuthService{...}`: клиент, серверный интерфейс, регистрация.

Генерацию настраивает `proto/buf.gen.yaml`: два плагина `protoc-gen-go` и `protoc-gen-go-grpc`, выход в `../gen`, `paths=source_relative` (структура папок повторяет proto-путь), managed mode подставляет `go_package` = `mini-core/services/auth-service/gen/...` (полное имя пакета — импорт, который видишь в файлах). `buf lint` (buf.yaml) проверяет стиль контракта; два правила выключены с комментарием-оправданием: Login и Refresh возвращают одинаковый набор полей, поэтому сознательно один message на два RPC.

### 8.3 Четыре вещи, которые ты используешь из gen/

**1) Структуры запросов и ответов** (auth.pb.go). Каждому `message` — struct. CamelCase в proto → CamelCase в Go, snake_case-поля → тоже CamelCase:

| В .proto | В Go (gen/bank/auth/v1) |
|---|---|
| `message RegisterRequest { string email = 1; ... }` | `type RegisterRequest struct { Email string; Password string }` |
| `message LoginResponse { string access_token = 1; ... }` | `type LoginResponse struct { AccessToken, RefreshToken string }` |

Создаёшь литералом, читаешь поля напрямую или акцессорами `GetEmail()` (акцессор безопасен: не паникует на nil-указателе, возвращает нулевое значение). Используют эти структуры обе стороны: gateway собирает `&authv1.LoginRequest{...}`, grpcapi/auth разбирает `req.GetEmail()`.

**2) Серверный интерфейс** (auth_grpc.pb.go):

```go
type AuthServiceServer interface {
        Register(context.Context, *RegisterRequest) (*RegisterResponse, error)
        Login(context.Context, *LoginRequest) (*LoginResponse, error)
        Refresh(context.Context, *RefreshRequest) (*LoginResponse, error)
        mustEmbedUnimplementedAuthServiceServer()
}
```

Именно его реализует `grpcapi.Server` в auth-service — с встраиванием `authv1.UnimplementedAuthServiceServer` (структура с методами-заглушками «codes.Unimplemented»): если завтра в proto добавят новый RPC, наш сервер скомпилируется и до реализации вернёт внятную ошибку, а не упадёт.

**3) Регистрация сервера**: `authv1.RegisterAuthServiceServer(srv, grpcapi.New(...))` в cmd/main.go — связывает реализацию с gRPC-сервером, маршрутизация по методам готова.

**4) Фабрика клиента**: `authv1.NewAuthServiceClient(authConn)` в api-gateway/cmd/main.go. Возвращает `AuthServiceClient` с теми же тремя методами, но «в другую сторону»: `s.auth.Login(ctx, req)` сериализует запрос, шлёт по сети, ждёт, распаковывает ответ. Клиенту не нужны ни адрес конкретного метода, ни формат — только conn и вызов.

Итого поток вызова, который раньше выглядел загадкой:

```text
gateway:  req := &authv1.LoginRequest{Email: e, Password: p}
          resp, err := s.auth.Login(ctx, req)          // клиент из gen
              ↓ сеть (protobuf-байты) ↓
auth:     func (s *Server) Login(ctx, req *authv1.LoginRequest) (*authv1.LoginResponse, error)
          // сервер из gen; s.authv1-каркас сам разобрал байты в req
          return &authv1.LoginResponse{AccessToken: a, RefreshToken: r}, nil
              ↓ сеть ↓
gateway:  resp.AccessToken  // готовая структура
```

Для customer-service всё то же самое с типами `customerv1.*` и методами `GetCustomer/UpdateProfile/GetCustomerStatus/ListCustomers`. Остальные 90% строк в gen/ — это детали сериализации (Marshal/Unmarshal, размерные проверки, дескрипторы), которые ни разу не понадобятся руками.

### 8.4 migrations + queries → sqlc → db/

Три входа и выход:

```text
services/auth-service/
  migrations/*.sql        ← СХЕМА (таблицы)      ┐
  queries/*.sql           ← ЗАПРОСЫ (-- name: …) ┤→ sqlc generate → internal/adapters/postgres/db/
  sqlc.yaml               ← КОНФИГ               ┘
```

Как sqlc понимает запросы — на примере:

```sql
-- name: GetUserByEmail :one
SELECT id, email, password_hash, role
FROM users
WHERE email = $1;
```

sqlc разбирает схему из migrations (знает: `users.id — uuid NOT NULL` и т.д.), типизирует каждый запрос: `$1` по контексту сравнения с колонкой `email` → `string`; результат — строка с четырьмя полями. Сравнивает с override'ами из sqlc.yaml (uuid → string, timestamptz → time.Time). Результат в `users.sql.go`:

```go
func (q *Queries) GetUserByEmail(ctx context.Context, email string) (GetUserByEmailRow, error)
```

Аннотации:

| Аннотация | Что генерирует | Пример из проекта |
|---|---|---|
| `:one` | метод, возвращающий одну строку (или `pgx.ErrNoRows`) | `GetUserByEmail` |
| `:exec` | метод без результата, только ошибка | `CreateUser`, `RevokeRefreshToken` |
| `:many` | метод со срезом строк | `ListCustomers` в customer |

Если запрос возвращает несколько параметров — генерируется структура `XxxParams` (в `CreateUserParams{ID, Email, PasswordHash, Role}`); если колонки результата отличаются от колонок таблицы — структура `XxxRow` (`GetCustomerByUserIDRow` — там `date_of_birth` превращён `to_char`'ом в строку). Имена полей — из имён колонок; именованные параметры в SQL (`@first_name`) дают именам полей читабельность.

### 8.5 Кто что импортирует — сводная таблица

| Пакет | Импортируют | Не импортирует никогда |
|---|---|---|
| `gen/bank/auth/v1` (authv1) | grpcapi (auth), cmd (оба сервиса), httpapi (gateway) | domain, app, postgres |
| `gen/bank/customer/v1` (customerv1) | grpcapi (customer), cmd (оба сервиса), httpapi (gateway) | — |
| `internal/adapters/postgres/db` | только свой `internal/adapters/postgres` (репозитории) | app, domain, grpcapi, gateway |
| `internal/domain` | app, adapters, тесты | gen, db, pgx, grpc — ничего внешнего |
| `internal/app` | grpcapi, cmd | pgx/grpc/kafka/gen — только порты |

Из таблицы видно главное правило зависимостей: **сгенерированные пакеты не протекают в домен и сценарии**. На границе — репозитории и grpcapi, которые конвертируют типы (row → domain, proto → domain, domain → proto). Благодаря этому сменить sqlc на «что-то ещё» или gRPC-схему на другую версию можно, не тронув бизнес-правила.

---
### 8.6 Чеклист: добавить новый RPC с нуля

На примере гипотетического `Logout(refresh_token)` в auth-service:

1. `proto/bank/auth/v1/auth.proto`: message `LogoutRequest{ string refresh_token = 1; }` + строка `rpc Logout(LogoutRequest) returns (google.protobuf.Empty);` в service (или верни LoginResponse без refresh — как удобнее).
2. `make proto-lint` → `make proto-gen` — gen обновился. Компиляция сломается? Нет: сервер всё ещё компилируется — новый RPC отдаёт Unimplemented из встроенных заглушек. Это и есть смысл встраивания.
3. Сценарий: метод в `internal/app` (порты уже есть: RefreshStore.Revoke). Юнит-тест с фейками.
4. Адаптер: `func (s *Server) Logout(...)` в grpcapi — переводы ошибок в codes.
5. main пересобирает — обычно ничего менять не нужно.
6. Gateway: новый роут + хендлер по образцу `refresh`.

Шаги 3–4 независимы — можно в паре: один пишет сценарий, второй адаптер. Контракт уже зафиксирован proto — это главное преимущество кодогенерации, ты только что им воспользовался.

### 8.7 Чеклист: добавить новый SQL-запрос

1. SQL с аннотацией в `queries/*.sql`: `-- name: CountByStatus :one SELECT status, count(*) ...` (не забудь `GROUP BY`).
2. `make sqlc-gen` → в db/ появились метод и структуры.
3. Метод репозитория-обёртки (маппинг строк → домен), тест с фейком на уровне сценария.
4. Если поменял схему — сначала миграция в migrations/, потом запрос; sqlc увидит новые колонки автоматически.

Оба чеклиста — в чистом виде ответ на вопрос «что делать со сгенерированным кодом»: ничего не править в gen/ и db/, править исходники и перегенерировать.

## 9. Главный сценарий: от «создать перса» до заполненной анкеты

Пройдём весь путь по шагам, с файлами и строками кода на каждом этапе. Это тот случай, когда карта местности важнее отдельных улиц.

### 9.1 Карта сценария

```mermaid
sequenceDiagram
    participant C as curl (клиент)
    participant GW as api-gateway :8080
    participant A as auth-service :50051
    participant P1 as auth_db
    participant K as Kafka auth.events
    participant CU as customer-service :50052
    participant P2 as customer_db

    C->>GW: POST /auth/register {email, password}
    GW->>A: gRPC Register(RegisterRequest)
    A->>A: argon2id-хеш, UUID v7
    A->>P1: INSERT INTO users
    A-->>K: событие user_registered (Envelope)
    A-->>GW: RegisterResponse{user_id, role}
    GW-->>C: 201 {user_id, email, role}

    note over K,CU: асинхронно, могли пройти секунды или часы
    K->>CU: FetchMessage: user_registered
    CU->>P2: INSERT customers (status=NEW, ON CONFLICT DO NOTHING)
    CU->>K: CommitMessages

    C->>GW: POST /auth/login
    GW->>A: gRPC Login
    A-->>GW: LoginResponse{access_token, refresh_token}

    C->>GW: PUT /customers/me (Bearer access)
    GW->>GW: authMW: verify подпись → claims
    GW->>CU: gRPC UpdateProfile(user_id из claims, ...)
    CU->>P2: UPDATE customers
    CU-->>GW: Customer{status=PROFILE_FILLED}
    GW-->>C: 200
```

### 9.2 Шаг 1. Регистрация

`POST /auth/register` → `httpapi/server.go: s.register` → JSON в структуру → `s.auth.Register(ctx, &authv1.RegisterRequest{...})` (сгенерированный клиент) → `grpcapi/server.go: Server.Register` → `s.reg.Register(ctx, req.GetEmail(), req.GetPassword())`.

Внутри сценария (`app/register.go`): пароль ≥ 8 символов → `argon2id.CreateHash` → `domain.NewUser` (валидация email, роль CLIENT) → `uuid.NewV7()` → `userRepo.Create` → sqlc-метод `CreateUser` → `INSERT`. Повторный email: БД отвечает 23505 → репозиторий переводит в `domain.ErrEmailTaken` → grpcapi в `codes.AlreadyExists` → gateway в HTTP 409.

После сохранения: `s.pub.UserRegistered(...)` → `kafka/producer.go` собирает Envelope с `event_type: "user_registered"` и пишет в `auth.events` (ключ = user_id). Регистрация возвращает ответ **не дожидаясь** обработки события — в этом смысл асинхронности.

### 9.3 Шаг 2. Создание перса (асинхронно)

`customer-service/cmd/main.go` стартовал `go consumer.Run(ctx)` — горутина постоянно `FetchMessage` из `auth.events`. Приходит envelope → `handle` проверяет `event_type` → `app.EnsureProfile(userID)`:

```go
if _, err := s.repo.FindByUserID(ctx, userID); err == nil {
        return nil // уже есть — нормально
}
id, _ := uuid.NewV7()
return s.repo.Create(ctx, &domain.Customer{ID: id.String(), UserID: userID, Status: domain.StatusNew})
```

Репозиторий вызывает `q.CreateCustomer` → SQL `INSERT ... ON CONFLICT (user_id) DO NOTHING` — даже если два экземпляра сервиса обработают одно событие одновременно, в таблице останется одна строка. Затем `CommitMessages` — Kafka запомнит, что событие обработано. Три уровня защиты от дублей: идемпотентная проверка в сценарии, идемпотентный SQL, коммит после обработки. При умрании между handle и commit событие вернётся — и ничего не сломается: **at-least-once в кармане**.

В `customer_db` теперь лежит строка: `user_id = 0192...`, `status = 'NEW'`, остальные поля пустые. Это и есть «созданный перс».

### 9.4 Шаг 3. Логин

`POST /auth/login` → `s.login` → gRPC `Login` → `app.Login`: `FindByEmail` (не нашли? → `ErrInvalidCredentials` — намеренно неотличимо от «пароль неверный») → `argon2id.ComparePasswordAndHash` → `tokens.IssueAccess` (JWT RS256, sub/role/iat/exp, TTL 15 минут) → `newRefreshToken` (32 случайных байта) → sha256 → `refreshRepo.Create` (в БД только хеш, TTL 30 дней) → `LoginResponse{access_token, refresh_token}`.

### 9.5 Шаг 4. Заполнение анкеты

`PUT /customers/me` с заголовком `Authorization: Bearer <access>`. На gateway первым в цепочке стоит `authMW`: строка после "Bearer " → `verifier.Verify` → подпись и срок проверены локально публичным ключом → `claims{UserID, Role}` в контекст запроса. Дальше `rateLimitMW` (лимит уже по user_id) → `s.updateMe`.

Важная строка:

```go
c := claimsFromCtx(r.Context()) // user_id из ПРОВЕРЕННОГО токена, не из запроса!
resp, err := s.customers.UpdateProfile(s.outgoing(r), &customerv1.UpdateProfileRequest{UserId: c.UserID, ...})
```

Клиент не указывает user_id вообще — его берут из токена. Подделать нельзя: подпись RS256 без приватного ключа не воспроизвести.

В customer-service: `Server.UpdateProfile` → `app.UpdateProfile`: найти профиль (`GetCustomerByUserID`), доменный метод `c.UpdateProfile(...)` (статус NEW → разрешено; статус станет PROFILE_FILLED; статус ON_KYC и позже → `ErrProfileLocked`), сохранить (`UpdateCustomer`), вернуть свежий Customer → `toProto` → gateway → JSON 200.

Что видит клиент в ответе: все поля анкеты + `status: "PROFILE_FILLED"`. Статусная машина сделала свой ход: `NEW → PROFILE_FILLED` разрешён `allowedTransitions`.

### 9.6 Шаг 5. Дальше по кругу

- `GET /customers/me` — тот же путь, но чтение: `GetCustomer` → `q.GetCustomerByUserID` → домен → proto → JSON.
- `POST /auth/refresh` — `app.Refresh`: хеш от пришедшего токена → `FindByHash` → отозван/истёк? → отказ → выпустить новую пару → отозвать старую. Ротация: каждый refresh работает один раз.
- Ручки «офицер смотрит списком» (`ListCustomers`) в proto есть; gateway в этой итерации их не выставляет наружу — для списков нужен RBAC, который появится с KYC-неделями. Адаптер и сценарий уже готовы.

### 9.7 Где что могло сломаться — карта отказов

| Отказ | Что происходит | Почему это терпимо |
|---|---|---|
| Kafka недоступна при регистрации | лог ошибки, регистрация успешна | событие потеряно (не критично в учебной модели; в проде — outbox) |
| customer-service умер до Commit | событие обработается снова | EnsureProfile + ON CONFLICT идемпотентны |
| Redis недоступен | rate limit fail-open, запросы идут | доступность важнее лимита |
| БД customer недоступна | `UpdateProfile` вернёт Internal → 500 | клиент может повторить; данные не потеряны |
| Просрочен access | gateway вернёт 401 | клиент зовёт /auth/refresh |

---

## 10. Сквозной разбор ошибок

Ошибка рождается в самом низу (БД, драйвер) и должна подняться наверх, не потеряв смысла. В проекте для этого есть три «языка» и dва пограничных переводчика.

**Три языка:**

1. **Доменный** (`internal/domain`): `ErrInvalidInput`, `ErrEmailTaken`, `ErrInvalidCredentials`, `ErrNotFound`, `ErrProfileLocked`. Смысл: «что пошло не так с точки зрения бизнеса».
2. **gRPC** (`google.golang.org/grpc/codes`): `InvalidArgument`, `AlreadyExists`, `Unauthenticated`, `NotFound`, `FailedPrecondition`, `Internal`. Смысл: «какой это тип сбоя по каналу связи».
3. **HTTP** (коды статусов): 400, 409, 401, 404, 412, 500... Смысл: «что клиенту делать дальше».

**Переводчики** — две функции-таблицы:

```go
// auth-service, grpcapi/server.go
case errors.Is(err, domain.ErrEmailTaken):
        return nil, status.Error(codes.AlreadyExists, "email уже зарегистрирован")

// api-gateway, httpapi/helpers.go
case codes.AlreadyExists:
        return 409
```

Полная таблица соответствий:

| Доменная ошибка | gRPC-код | HTTP | Где рождается |
|---|---|---|---|
| `ErrInvalidInput` | InvalidArgument | 400 | register: email/пароль |
| `ErrEmailTaken` | AlreadyExists | 409 | register: дубль email (23505) |
| `ErrInvalidCredentials` | Unauthenticated | 401 | login/refresh |
| `ErrNotFound` (customer) | NotFound | 404 | getMe: профиля нет |
| `ErrProfileLocked` | FailedPrecondition | 412 | updateMe: KYC уже идёт |
| — | ResourceExhausted | 429 | rate limit |
| — | Unavailable / не-gRPC | 503 / 502 | сервис лежит |

Обрати внимание на **два разных «not found»**: login превращает «юзера нет» в `ErrInvalidCredentials` (не раскрывать существование email), а getMe — честный 404. Один и тот же факт БД может означать разные вещи на разных эндпоинтах — поэтому решение о «маскировке» принимается в сценарии, а не в репозитории.

Также заметь: внутренние ошибки никогда не показывают свою «начинку» наружу (`status.Error(codes.Internal, "internal error")`). Детали — в логах с request_id; наружу — только код. Иначе через сообщения об ошибках утекают детали схемы БД и путей.

---

## 11. Грабли и частые вопросы

**«connection refused» при обращении к сервису.** Проверь, что инфраструктура поднята (`docker compose ps`), что три процесса `make run-*` запущены и в каких портах они залогировались. Адреса по умолчанию: gateway :8080, auth :50051 (+JWKS :8081), customer :50052.

**«pq: unknown database» / auth_db есть, customer_db нет.** `customer_db` создаётся init-скриптом только при **первом** создании тома Postgres. Если том уже существовал: `docker compose down -v` (сотрёт данные!) и `up` заново, либо `CREATE DATABASE customer_db OWNER dev;` вручную.

**Таблиц нет (relation "users" does not exist).** Примени миграции — раздел 2.3. sqlc не применяет миграции, он только читает их схему при генерации.

**buf ругается загадочными ошибками про directory/package.** Ты запустил buf не из той папки — конфиг buf.yaml он ищет в текущей директории. Запускай через `make proto-lint` / `make proto-gen` (они делают `cd services/*/proto`).

**Поменял .proto — ничего не изменилось.** Забыл `make proto-gen`. Поменял queries/*.sql — забыл `make sqlc-gen`. Генерация не «подхватывает» изменения сама.

**Почему gateway идёт insecure?** Локальная разработка без TLS. В проде между сервисами — TLS/mTLS; код добавления credentials меняется в одном месте (grpc.NewClient).

**Почему токены не хранятся на сервере?** Access — носитель: подпись подтверждает подлинность, серверу не нужна таблица сессий. Состояние (отзыв) отслеживается только для refresh — по хешу в БД.

**Почему Refresh возвращает LoginResponse, а не RefreshResponse?** Поля совпадают один-в-один (access + refresh). Отдельный message — копипаста без пользы; lint-правило RPC_RESPONSE_STANDARD_NAME выключено в buf.yaml с этим объяснением. В проде за такое решение спрашивают дважды (эволюция сообщения отдельным путём — риск), но для учебного это честный компромисс против дублирования.

**Почему ListCustomers в sqlc — два запроса?** PostgreSQL не допускает именованные параметры в LIMIT/OFFSET, а «необязательный» фильтр в одном запросе (`@f = '' OR status = @f`) требует смешивания именованных и позиционных параметров, которое sqlc не поддерживает. Два метода + выбор в Go — проще и честнее.

**Почему регистрация не падает, когда Kafka лежит?** Событие «не критично для сделки»: учётка уже сохранена. Утерянные события — цена упрощения; лечение: outbox-паттерн (раздел 12).

**Почему состояние rate limit в Redis, а не в памяти gateway?** Инстансов gateway может быть несколько — счётчик должен быть общим и переживать рестарты.

**А почему не Gin/Fiber/Echo?** Штатный `net/http` с Go 1.22 умеет методные паттерны и по-прежнему не тянет зависимостей. Для учебного ядра этого достаточно; поменять роутер — изолированная правка в httpapi.

**Где хранить ключи в проде?** Не в git: секрет-менеджер (Vault, KMS, docker secrets), переменные окружения/смонтированные файлы при запуске. Пути уже конфигурируются через `JWT_PRIVATE_KEY_PATH`/`JWT_PUBLIC_KEY_PATH`.

---

## 12. Что развивать дальше

Список отсортирован по «польза/сложность» и соответствует TODO в коде:

1. **goose для миграций** — применить весь накопленный инструмент автоматически: `goose up` вместо ручного psql. Файлы уже в формате goose.
2. **Outbox-паттерн** для событий auth: событие пишется в ту же транзакцию, что и INSERT юзера (таблица `outbox`), отдельный воркер публикует и помечает. Убирает потерю событий при недоступной Kafka.
3. **Retry + DLQ для consumer**: ретраи с экспоненциальной задержкой, после N попыток — в мёртвую очередь, чтобы ядовитое сообщение не блокировало партицию.
4. **RBAC в gateway**: middleware, проверяющая `claims.Role` против требуемой роли роута (офицер — списки клиентов, админ — всё). Claims уже несут role.
5. **JWKS вместо файла в gateway**: при старте/по интервалу ходить на `:8081/jwks` (kid + ротация ключей) — gateway перестанет зависеть от файла на диске.
6. **TLS/mTLS** между gateway и сервисами.
7. **Курсорная пагинация** в ListCustomers (pageToken = курсор по created_at+id) — offset деградирует на больших таблицах.
8. **Интеграционные тесты с реальным Postgres** (dockertest или testcontainers) на репозитории — сейчас покрыты сценарии, но не SQL.
9. **KYC-сервис** — следующий шаг ТЗ: заявки, проверка офицером, переходы ON_KYC → ACTIVE/REJECTED (статусная машина уже готова к этому).

---

## 13. Шпаргалка команд

```bash
# --- инфраструктура ---
docker compose up -d                 # поднять Postgres, Kafka, Redis
docker compose down                  # остановить
docker compose down -v               # остановить и стереть данные

# --- кодогенерация (после правки proto или SQL) ---
make proto-lint                      # проверка стиля .proto
make proto-gen                       # обновить services/*/gen
make sqlc-gen                        # обновить services/*/internal/adapters/postgres/db
make gen                             # всё сразу

# --- сборка и запуск ---
make build                           # бинарники в bin/
make test                            # go test в auth + customer, vet в gateway
make run-gateway                     # терминал 1
make run-auth                        # терминал 2
make run-customer                    # терминал 3

# --- kafka ---
make kafka-topics                    # список топиков
make kafka-read                      # читать auth.events с начала
make kafka-lag                       # отставание группы customer-service

# --- база ---
docker exec -it $(docker ps -qf name=postgres) psql -U dev -d auth_db -c 'SELECT id, email, role FROM users;'
docker exec -it $(docker ps -qf name=postgres) psql -U dev -d customer_db -c 'SELECT user_id, status, first_name FROM customers;'

# --- руки-ноги при дебаге ---
curl -s localhost:8081/jwks          # публичный ключ auth-service
curl -i localhost:8080/auth/login -d '{"email":"anna@example.com","password":"password123"}'
                                     # -i покажет статус и заголовки (X-Request-ID!)
```

Что читать в каком порядке, если ты только пришёл в проект: раздел 3 (язык) → 6.7–6.11 (auth: домен и сценарии — вся суть архитектуры) → 8 (сгенерированный код) → 9 (сценарий целиком) → остальное по мере правок. Запусти проект по разделу 2 до чтения — так всё, что описано, будет «живым».
