# GLOSSARY — английские слова из кода, по-русски

> Зачем этот файл: ты сказал, что плаваешь в английском, а код на 90% состоит из английских слов.
> Здесь собраны слова и штампы, которые реально встречаются в Mini-Core и в любом Go-коде.
> Читай таблицу по категориям, а со временем — только те слова, что мелькнули в коде и застряли.
> Слова в третьей колонке — где это встречается в **нашем** проекте.

## 1. Ключевые слова Go

| English | Русский | Комментарий |
|---|---|---|
| package | пакет | `package main` — исполняемая программа |
| import | импортировать, подключать | подключение чужих пакетов |
| func | функция | `func main()` |
| var | переменная | объявление переменной |
| const | константа | `const keyClaims = "claims"` |
| type | тип | `type Server struct {...}` |
| struct | структура | аналог «объекта с полями» |
| interface | интерфейс | набор методов без реализации |
| map | словарь, отображение | `map[string]any` — ключ → значение |
| slice / range | срез / диапазон, перебрать | `for i, v := range items` |
| return | вернуть (значение) | возврат из функции |
| if / else | если / иначе | |
| switch / case | выбор / вариант | вместо лестницы if-else |
| default | по умолчанию | ветка switch «во всех прочих случаях» |
| for | для, цикл | в Go цикл только один — for |
| go | запусти параллельно | `go func()` — горутина |
| chan | канал | `chan int` — труба между горутинами |
| select | выбрать (из каналов) | ожидание нескольких каналов |
| defer | отложить | `defer pool.Close()` — выполнится при выходе из функции |
| nil | «ничего», нулевое значение | `err != nil` — «есть ошибка» |
| break / continue | прервать / продолжить | управление циклом и switch |
| fallthrough | провалиться дальше | в switch — перейти в следующую ветку |
| new / make | создать | make — для map/slice/chan |

## 2. Слова из имён файлов и пакетов проекта

| English | Русский | Комментарий |
|---|---|---|
| gateway | шлюз, «ворота» | api-gateway — единственная дверь снаружи |
| server | сервер | |
| service | сервис, служба | auth-service, customer-service |
| handler | обработчик | функция, обрабатывающая запрос |
| middleware | прослойка, промежуточный обработчик | код между роутером и хендлером |
| route / router | маршрут / роутер | «какой URL → какой хендлер» |
| group | группа | `r.Group("/auth")` — общие прослойки на пачку роутов |
| engine | движок | `gin.New()` возвращает *gin.Engine — главный объект роутера |
| config | конфигурация | internal/config |
| domain | домен | бизнес-суть без зависимостей |
| port | порт (вход/выход) | интерфейс домена в гексагональной архитектуре |
| adapter | адаптер | переводчик между мирами (HTTP↔gRPC) |
| repository (repo) | хранилище, репозиторий | работа с БД |
| entity | сущность | объект домена: user, customer |
| verifier | проверяющий | internal/verifier — проверка подписи токена |
| issuer | выдающий, эмитент | jwtauth/issuer.go — создаёт (выдаёт) токены |
| limiter | ограничитель | ratelimit/limiter.go |
| publisher | издатель, публикатор | kafka/producer.go — отправляет события |
| consumer | потребитель | kafka/consumer.go — читает события |
| producer | производитель | синоним publisher |
| pool | пул | pgxpool — бассейн соединений с БД |
| claim(s) | утверждение, данные в токене | sub/role в JWT; «токен утверждает, что это user X» |
| token | токен, жетон | access-token, refresh-token |
| token bucket | «ведро с токенами» | алгоритм rate limit |
| worker | работник | фоновая единица работы |
| helper | помощник, вспомогательная функция | |
| recovery / recover | восстановление | recover() ловит панику |
| graceful shutdown | плавное (аккуратное) завершение | дожидаемся текущих запросов и гасимся |
| reflection | рефлексия | grpc reflection — самоописание сервера |

## 3. HTTP и Gin

| English | Русский | Комментарий |
|---|---|---|
| request (req) | запрос | |
| response (resp) | ответ | |
| header | заголовок | `c.GetHeader("Authorization")` |
| body | тело (запроса/ответа) | JSON внутри запроса |
| status code | код состояния | 200, 404, 500 |
| bind / binding | связать, привязать | `c.ShouldBindJSON(&in)` — разбор JSON в структуру |
| decode / encode | декодировать / кодировать | json.Decode / Encode |
| parse | разобрать | jwt.Parse — разобрать строку токена |
| marshal / unmarshal | сериализовать / десериализовать | то же, что encode/decode |
| abort | прервать | `c.Abort()` — остановить цепочку обработчиков |
| next | дальше, следующий | `c.Next()` — передать управление следующим слоям |
| preflight | предварительный запрос | OPTIONS-запрос браузера перед основным |
| unauthorized | не авторизован | 401 — «кто ты?» |
| forbidden | запрещено | 403 — «я тебя знаю, но нельзя» |
| not found | не найдено | 404 |
| bad request | некорректный запрос | 400 |
| conflict | конфликт | 409 — «email уже занят» |
| too many requests | слишком много запросов | 429 — rate limit |
| internal server error | внутренняя ошибка сервера | 500 |
| bad gateway | плохой шлюз | 502 — «внутренний сервис сломался» |
| service unavailable | сервис недоступен | 503 |
| no content | «без содержимого» | 204 — успех без тела ответа |
| created | создано | 201 — после POST, который что-то создал |
| CORS | «совместное использование ресурсов между источниками» | браузерная политика доступа с чужого домена |
| wildcard | подстановочный знак | путь-джокер `*path` |
| query param | параметр запроса | `?status=NEW` — часть URL после `?` |
| path param | параметр пути | `/customers/:id` |
| rate limit | лимит частоты запросов | ratelimit |
| fail-open | «при поломке — открыть» | Redis упал → пропускаем запрос |
| fail-closed | «при поломке — закрыть» | Redis упал → отказываем (так строже) |
| verbose | подробный | `go test -v` — verbose, выводить всё |
| deprecated | устаревший | «не используй, будет удалено» |

## 4. JWT, подписи, криптография

| English | Русский | Комментарий |
|---|---|---|
| sign | подписать | SignedString — подписать токен |
| signature | подпись | третья часть JWT |
| verify | проверить (подпись) | Verifier.Verify |
| issue | выпустить | IssueAccess — «выдать» токен |
| revoke | отозвать | refresh-токен отзывается при ротации |
| expire / expired | истекать / истёкший | exp — срок жизни токена |
| issued at | выпущен в (время) | iat — claim «когда выпущен» |
| payload | полезная нагрузка | содержимое токена |
| secret | секрет | приватные данные для подписи |
| public key | публичный ключ | проверяет подпись |
| private key | приватный ключ | ставит подпись; доступен только auth |
| key pair | пара ключей | приватный + публичный |
| PEM | формат-обёртка ключей | `-----BEGIN PUBLIC KEY-----` |
| whitelist | белый список | WithValidMethods — разрешён только RS256 |
| algorithm (alg) | алгоритм | |
| hash | хеш | sha256 от refresh-токена |
| salt | соль | случайная добавка к паролю |
| rotate / rotation | ротировать / ротация | refresh-токен живёт один раз |
| lifetime / TTL | срок жизни / время жизни | TTL — time to live |

## 5. Базы данных и sqlc

| English | Русский | Комментарий |
|---|---|---|
| query | запрос (к БД) | queries/*.sql |
| scan | сканировать (строку) | rows.Scan — разложить колонки в поля |
| rows | строки (результата) | |
| migrate / migration | мигрировать / миграция | migrations/*.sql — эволюция схемы |
| schema | схема (структура БД) | |
| constraint | ограничение | правило целостности данных |
| unique violation | нарушение уникальности | код 23505 → «email уже существует» |
| nullable | допускающий NULL | поле может быть пустым |
| on conflict | «при конфликте» | `ON CONFLICT DO NOTHING` — идемпотентный insert |
| idempotent | идемпотентный | повтор безопасен: результат тот же |
| transaction | транзакция | группа операций «всё или ничего» |
| commit | зафиксировать | завершить транзакцию успешно |
| rollback | откатить | отменить транзакцию |
| foreign key | внешний ключ | ссылка на строку другой таблицы |
| primary key | первичный ключ | id строки |
| index | индекс | ускоряет поиск |
| pool | пул соединений | pgxpool |
| generated code | сгенерированный код | db/ из sqlc |
| override | переопределение | overrides в sqlc.yaml — поменять тип |

## 6. Kafka

| English | Русский | Комментарий |
|---|---|---|
| broker | брокер | сервер Kafka (localhost:9092) |
| topic | топик, тема | канал-каталог сообщений (auth.events) |
| partition | партиция, раздел | часть топика для параллелизма |
| offset | смещение | номер сообщения в партиции |
| produce / publish | произвести / опубликовать | отправить сообщение |
| consume | потреблять, читать | консьюмер читает топик |
| consumer group | группа потребителей | несколько читателей делят партиции |
| lag | отставание | сколько сообщений ещё не обработано |
| at-least-once | «минимум один раз» | гарантия доставки Kafka |
| retry | повторить попытку | |
| batch | батч, пачка | несколько сообщений разом |

## 7. Глаголы и штампы, из которых состоит код

| English | Русский | Комментарий |
|---|---|---|
| create / destroy | создать / уничтожить | |
| get / set | получить / установить | |
| update | обновить | |
| delete / remove | удалить | |
| list | перечислить, вывести списком | ListCustomers |
| find / lookup | найти / поискать | FindByEmail |
| check | проверить | |
| validate | проверить на валидность | |
| handle | обработать | |
| wrap / unwrap | обернуть / развернуть | fmt.Errorf("%w") — обернуть ошибку |
| allow / deny | разрешить / запретить | limiter.Allow |
| load | загрузить | config.Load |
| read / write | читать / писать | |
| open / close | открыть / закрыть | |
| start / stop | запустить / остановить | |
| run | запустить (выполнить) | |
| build | собрать | go build |
| generate | сгенерировать | buf generate, sqlc generate |
| inject | внедрить | инъекция зависимостей |
| attach | прикрепить | |
| detach | отсоединить | |
| push / pull | вытолкнуть / вытянуть | |
| fetch | получить (издалека) | fetch data — забрать данные |
| submit | отправить (на обработку) | |
| dispatch | отправить, направить | |
| listen | слушать | net.Listen — слушать порт |
| serve | обслуживать | srv.Serve — обслуживать запросы |
| watch | следить за | |
| notify | уведомить | signal.NotifyContext |
| prevent | предотвратить | |
| enforce | принудительно обеспечивать | enforce policy — «следить, чтобы правило соблюдалось» |
| fallback | запасной вариант | что делаем, если основное не сработало |
| cleanup | очистка | уборка ресурсов |
| setup / teardown | настройка / демонтаж | подготовка и уборка в тестах |

## 8. Прилагательные из документации и комментариев

| English | Русский | Комментарий |
|---|---|---|
| required / optional | обязательный / необязательный | |
| valid / invalid | валидный / невалидный | |
| valid until | действителен до | |
| immutable | неизменяемый | |
| mutable | изменяемый | |
| lazy | ленивый | откладывает работу до нужды; lazy init |
| eager | жадный, заблаговременный | делает сразу |
| internal / external | внутренний / внешний | |
| nested | вложенный | |
| empty | пустой | |
| missing | отсутствующий | missing header — «нет заголовка» |
| unexpected | неожиданный | unexpected error |
| duplicate | дубликат | |
| expired | истёкший | |
| revoked | отозванный | |
| locked | заблокированный | |
| pending | ожидающий | «висит в ожидании» |
| available / available at | доступный / доступен на | |
| concurrent | параллельный, одновременный | concurrent requests |
| thread-safe | безопасный для потоков | можно звать из горутин |
| race condition | состояние гонки | два потока ломают данные друг другу |
| deadlock | взаимная блокировка | все ждут друг друга — всё стоит |
| leak | утечка | goroutine leak — горутина не завершается |
| buffer | буфер | |
| stream | поток (данных) | |
| flush | вытолкнуть, сбросить | дослать накопленное получателю |
| opaque | непрозрачный | opaque token — «чёрный ящик», содержимое не читается |
| atomic | атомарный | выполняется целиком, без промежутков |

## 9. Штампы из сообщений об ошибках

| English | Русский | Комментарий |
|---|---|---|
| failed to ... | не удалось ... | failed to connect — «не смог подключиться» |
| unable to ... | невозможно ... | |
| invalid ... | некорректный ... | invalid token |
| malformed ... | искажённый, битый ... | malformed JSON |
| unexpected ... | неожиданный ... | unexpected end of JSON |
| missing ... | отсутствует ... | |
| unsupported ... | не поддерживается ... | |
| already exists | уже существует | email taken |
| not found | не найдено | |
| out of range | вне диапазона | |
| permission denied | доступ запрещён | |
| connection refused | в соединении отказано | Redis/Postgres не подняты |
| connection reset | соединение сброшено | |
| deadline exceeded | время истекло | превышен дедлайн контекста |
| context canceled | контекст отменён | |
| resource exhausted | ресурс исчерпан | gRPC-код для 429 |
| precondition failed | не выполнено предварительное условие | 412 |
| internal | внутренняя (ошибка) | |
| unknown | неизвестный | |
| busy | занят(о) | |
| timeout | таймаут, превышение времени ожидания | |
| retry later | повторите позже | |

## 10. Сокращения, которые мелькают везде

| Сокращение | Полностью | Русский |
|---|---|---|
| ctx | context | контекст |
| req | request | запрос |
| resp / res | response | ответ |
| cfg | config | конфигурация |
| repo | repository | репозиторий, хранилище |
| svc | service | сервис |
| h / hdl | handler | обработчик |
| db | database | база данных |
| err | error | ошибка |
| addr | address | адрес |
| conn | connection | соединение |
| buf | buffer | буфер |
| str | string | строка |
| num | number | число |
| idx | index | индекс |
| len | length | длина |
| id | identifier | идентификатор |
| pwd / pass | password | пароль |
| msg | message | сообщение |
| TTL | time to live | время жизни |
| JWT | JSON Web Token | веб-токен в формате JSON |
| JWKS | JSON Web Key Set | набор публичных ключей в JSON |
| RS256 | RSA Signature, SHA-256 | алгоритм подписи JWT |
| RPC | remote procedure call | «вызов процедуры на другой машине» |
| gRPC | Google RPC | RPC-фреймворк проекта |
| HTTP/HTTPS | hyper text transfer protocol | протокол веба |
| JSON | JavaScript Object Notation | формат данных |
| SQL | structured query language | язык запросов к БД |
| CRUD | create, read, update, delete | четыре базовые операции |
| DTO | data transfer object | объект для переноса данных |
| PK / FK | primary / foreign key | первичный / внешний ключ |
| TLS | transport layer security | шифрование транспорта |
| API | application programming interface | программный интерфейс |
| env | environment | окружение (переменные среды) |
| deps | dependencies | зависимости |
| impl | implementation | реализация |
| param | parameter | параметр |
| arg | argument | аргумент |
| val / v | value | значение |
| pref | prefix | префикс, начало строки |
| suff | suffix | суффикс, окончание |

## 11. Фразы из документации Go и чужих библиотек

| English | Русский |
|---|---|
| returns ... or an error | возвращает ... или ошибку |
| must be ... | должно быть ... (иначе паника/ошибка) |
| should be ... | следует делать ... (мягче требования) |
| it is safe to call ... from multiple goroutines | можно безопасно вызывать из нескольких горутин |
| do not use ... , use ... instead | не используй ..., вместо этого используй ... |
| note that ... | заметь, что ... |
| see also ... | смотри также ... |
| e.g. | например (exempli gratia) |
| i.e. | то есть (id est) |
| etc. | и так далее |
| a.k.a. | также известный как |
| unless | если только не |
| deprecated since v1.10 | устарел начиная с версии 1.10 |
| this function panics if ... | эта функция паникует, если ... |
| caller must ensure ... | вызывающий обязан обеспечить ... |
| zero value | нулевое значение типа |
| it is a no-op | это пустая операция (ничего не делает) |
| for backward compatibility | для обратной совместимости |
| in production | в продакшене (на реальной эксплуатации) |
| experimental | экспериментальное (может измениться) |
| stable | стабильное (можно полагаться) |
| under the hood | «под капотом», внутри механизма |
