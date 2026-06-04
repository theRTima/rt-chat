# rt-chat

Чат-система с WebSocket и комнатами. Высоконагруженный чат-сервер на Go (goroutine на каждое соединение) с персистентностью в PostgreSQL.

Технологии: Go + WebSocket + React + PostgreSQL

## Архитектура

### Hub-and-Spoke модель

Проект использует классическую архитектуру Hub-and-Spoke для управления WebSocket соединениями:

- **Hub** -- центральный координатор, управляет всеми активными клиентами
- **Client** -- представляет одно WebSocket соединение с двумя goroutine (readPump и writePump)
- **Каналы** -- обеспечивают thread-safe коммуникацию между компонентами
- **Storage** -- интерфейс для работы с PostgreSQL через pgxpool

### Структура проекта

```
server/
  main.go              # Точка входа, HTTP сервер
  models/
    hub.go             # Hub для управления клиентами и комнатами
    client.go          # Client с read/write goroutine
    room.go            # Room для управления группой клиентов
    message.go         # Структуры сообщений и протокол
    user.go            # Модель пользователя для БД
  handlers/
    websocket.go       # WebSocket upgrade handler
  storage/
    database.go        # Подключение к PostgreSQL (pgxpool)
    repository.go      # Все методы запросов к БД
    persister.go       # Асинхронное сохранение сообщений (worker pool)
client/
  src/
    components/        # React компоненты UI
    hooks/             # Custom hooks (useChat)
    context/           # Context API для глобального состояния
    utils/             # Константы и утилиты
db/
  migrations/
    001_initial_schema.sql  # SQL схема базы данных
docker-compose.yml           # Docker Compose для запуска всех сервисов
```

### Компоненты

#### Hub (models/hub.go)

Hub управляет всеми активными соединениями, комнатами и маршрутизацией сообщений через каналы:

- `Register chan *Client` -- регистрация новых клиентов
- `Unregister chan *Client` -- отключение клиентов
- `Message chan *ClientMessage` -- обработка входящих сообщений
- `Broadcast chan []byte` -- рассылка сообщений всем клиентам (legacy)

Hub также управляет:
- Картой клиентов по UserID для приватных сообщений
- Картой комнат (Room) для группового общения
- Автоматическим удалением пустых комнат

Использует `sync.RWMutex` для защиты карты клиентов от race conditions.

#### Room (models/room.go)

Room представляет чат-комнату с группой клиентов:
- Добавление и удаление клиентов
- Broadcast сообщений всем участникам комнаты
- Thread-safe операции с использованием `sync.RWMutex`

#### Message (models/message.go)

Структура JSON протокола для всех типов сообщений:
- `join_room` -- присоединение к комнате
- `leave_room` -- выход из комнаты
- `chat` -- сообщение в комнату
- `private` -- приватное сообщение пользователю
- `error` -- сообщение об ошибке
- `user_joined` -- уведомление о входе пользователя (с `participant_count`)
- `user_left` -- уведомление о выходе пользователя (с `participant_count`)
- `user_lookup` -- запрос поиска пользователя по имени (с `content`)
- `user_found` -- ответ с найденным пользователем (с `user_id`, `username`)
- `user_not_found` -- ответ: пользователь не найден
- `load_dm_history` -- запрос загрузки истории личных сообщений (с `to_user_id`)

#### Client (models/client.go)

Каждый клиент имеет два goroutine:

- **ReadPump** -- читает JSON сообщения из WebSocket, парсит их и отправляет в Hub.Message для обработки
- **WritePump** -- читает из канала Send и отправляет в WebSocket

Параметры производительности:
- `writeWait: 10s` -- таймаут записи
- `pongWait: 60s` -- таймаут ожидания pong от клиента
- `pingPeriod: 54s` -- интервал ping сообщений
- `maxMessageSize: 8192 bytes` -- максимальный размер сообщения

Клиент хранит:
- `UserID` и `Username` для идентификации
- Карту комнат, в которых он состоит
- Ссылку на `MessagePersister` для асинхронного сохранения сообщений

#### WebSocket Handler (handlers/websocket.go)

Обрабатывает HTTP запросы и апгрейдит их до WebSocket:

1. Принимает HTTP запрос на `/ws?user_id=<id>&username=<name>`
2. Проверяет обязательные параметры (user_id)
3. Апгрейдит соединение до WebSocket
4. Создает нового Client с UserID, Username и Persister
5. Регистрирует Client в Hub
6. Запускает ReadPump и WritePump goroutine

### Thread Safety

Проект обеспечивает безопасность при конкурентном доступе:

- **Каналы** -- основной механизм коммуникации между goroutine
- **sync.RWMutex** -- защита карты клиентов в Hub и Room
- **Отдельные goroutine** -- каждый Client имеет выделенные reader и writer goroutine
- **Неблокирующие операции** -- Enqueue использует select с default для избежания блокировки broadcast

### Персистентность (PostgreSQL)

#### Схема базы данных

- **users** -- хранение пользователей (user_id, username, created_at, last_seen)
- **rooms** -- хранение комнат (room_id, name, created_at)
- **messages** -- хранение сообщений с индексами по room_id, user_id, to_user_id
- **room_members** -- отслеживание участников комнат с поддержкой истории входов/выходов

#### Асинхронная запись сообщений

Система использует паттерн worker pool для асинхронного сохранения сообщений:

- Буферизированный канал (capacity: 1024) для очереди сообщений
- 3 worker, которые собирают сообщения в батчи по 50 штук
- Принудительный сброс батча каждые 2 секунды (даже если батч не полон)
- При заполнении очереди новые сообщения отбрасываются (не блокируют broadcast)
- Graceful shutdown с сохранением оставшихся сообщений

#### История сообщений

При подключении к комнате клиент автоматически получает последние 50 сообщений через WebSocket. Сообщения фильтруются по типу (chat, user_joined, user_left) и возвращаются в хронологическом порядке.

При открытии DM диалога клиент отправляет `load_dm_history` и получает историю приватных сообщений между двумя пользователями. История загружается при каждом открытии диалога и при переподключении WebSocket (если DM активен).

## Протокол WebSocket сообщений

Все сообщения передаются в JSON формате.

### Подключение к серверу

```
ws://localhost:8080/ws?user_id=user123&username=John
```

Параметры:
- `user_id` (обязательный) -- уникальный идентификатор пользователя
- `username` (опциональный) -- имя для отображения (по умолчанию: User_<user_id>)

### Типы сообщений

#### 1. Присоединение к комнате (join_room)

**Отправка клиентом:**
```json
{
  "type": "join_room",
  "room_id": "general"
}
```

**Получение всеми в комнате:**
```json
{
  "type": "user_joined",
  "room_id": "general",
  "user_id": "user123",
  "username": "John",
  "timestamp": "2026-06-02T20:00:00Z"
}
```

#### 2. Выход из комнаты (leave_room)

**Отправка клиентом:**
```json
{
  "type": "leave_room",
  "room_id": "general"
}
```

**Получение всеми в комнате:**
```json
{
  "type": "user_left",
  "room_id": "general",
  "user_id": "user123",
  "username": "John",
  "timestamp": "2026-06-02T20:05:00Z"
}
```

#### 3. Сообщение в комнату (chat)

**Отправка клиентом:**
```json
{
  "type": "chat",
  "room_id": "general",
  "content": "Hello everyone!"
}
```

**Получение всеми в комнате:**
```json
{
  "type": "chat",
  "room_id": "general",
  "user_id": "user123",
  "username": "John",
  "content": "Hello everyone!",
  "timestamp": "2026-06-02T20:10:00Z"
}
```

#### 4. Приватное сообщение (private)

**Отправка клиентом:**
```json
{
  "type": "private",
  "to_user_id": "user456",
  "content": "Hi there!"
}
```

**Получение получателем:**
```json
{
  "type": "private",
  "user_id": "user123",
  "username": "John",
  "to_user_id": "user456",
  "content": "Hi there!",
  "timestamp": "2026-06-02T20:15:00Z"
}
```

#### 5. Сообщение об ошибке (error)

**Получение при ошибке:**
```json
{
  "type": "error",
  "error": "Room ID is required",
  "timestamp": "2026-06-02T20:20:00Z"
}
```

#### 6. Поиск пользователя (user_lookup)

**Отправка клиентом:**
```json
{
  "type": "user_lookup",
  "content": "Alice"
}
```

**Получение при успехе:**
```json
{
  "type": "user_found",
  "user_id": "alice_uid",
  "username": "Alice",
  "timestamp": "2026-06-02T20:25:00Z"
}
```

**Получение при отсутствии:**
```json
{
  "type": "user_not_found",
  "content": "Alice",
  "timestamp": "2026-06-02T20:25:00Z"
}
```

#### 7. Загрузка истории приватных сообщений (load_dm_history)

**Отправка клиентом:**
```json
{
  "type": "load_dm_history",
  "to_user_id": "user456"
}
```

**Ответ:** сервер отправляет историю как последовательность сообщений типа `private`.

### Структура Message

```go
type Message struct {
    Type             MessageType `json:"type"`                         // Тип сообщения
    RoomID           string      `json:"room_id,omitempty"`            // ID комнаты
    UserID           string      `json:"user_id,omitempty"`            // ID отправителя
    Username         string      `json:"username,omitempty"`           // Имя отправителя
    ToUserID         string      `json:"to_user_id,omitempty"`         // ID получателя (для private)
    Content          string      `json:"content,omitempty"`            // Содержимое
    Timestamp        time.Time   `json:"timestamp"`                    // Временная метка
    Error            string      `json:"error,omitempty"`              // Текст ошибки
    ParticipantCount int         `json:"participant_count,omitempty"`  // Кол-во участников в комнате
}
```

## Запуск сервера

### Требования

- Go 1.26+
- PostgreSQL (доступный по `DATABASE_URL` или на localhost:5432)

### Настройка базы данных

```bash
createdb rtchat
```

### Переменные окружения

- `DATABASE_URL` -- строка подключения к PostgreSQL (по умолчанию: `postgres://postgres:postgres@localhost:5432/rtchat?sslmode=disable`)

### Сборка

```bash
cd server
go build -o bin/server .
```

### Запуск

```bash
./bin/server
# или с custom портом
./bin/server -addr :3000
```

### Endpoints

- `ws://localhost:8080/ws?user_id=<id>&username=<name>` -- WebSocket endpoint для подключения клиентов
- `http://localhost:8080/health` -- health check endpoint
- `http://localhost:8080/stats` -- информация о количестве подключенных клиентов

### Тестирование вручную (websocat)

```bash
brew install websocat
websocat "ws://localhost:8080/ws?user_id=alice&username=Alice"
# в другом терминале:
websocat "ws://localhost:8080/ws?user_id=bob&username=Bob"
```

#### Пример: общение в комнате

**Alice:**
```json
{"type": "join_room", "room_id": "general"}
```
**Bob:**
```json
{"type": "join_room", "room_id": "general"}
```
**Alice:**
```json
{"type": "chat", "room_id": "general", "content": "Hello everyone!"}
```
**Bob получает:**
```json
{"type":"chat","room_id":"general","user_id":"alice","username":"Alice","content":"Hello everyone!","timestamp":"2026-06-02T20:30:00Z"}
```

#### Пример: приватное сообщение

**Alice:**
```json
{"type": "private", "to_user_id": "bob", "content": "Hi Bob!"}
```
**Bob получает:**
```json
{"type":"private","user_id":"alice","username":"Alice","to_user_id":"bob","content":"Hi Bob!","timestamp":"2026-06-02T20:35:00Z"}
```

### Автоматические тесты Go

Всего **51 тест** в 6 файлах (~2600 строк). Запуск:

```bash
cd server
go test ./... -count=1 -v
```

#### Файлы тестов

| Файл | Кол-во | Уровень | Что тестирует |
|---|---|---|---|
| `models/message_test.go` | 8 | unit | Создание сообщений (chat, private, error, user_joined, user_left), Client (AddRoom, RemoveRoom, SendMessage) |
| `models/room_test.go` | 5 | unit | Room (AddClient, RemoveClient, Broadcast, BroadcastExclude, IsEmpty) |
| `models/hub_test.go` | 7 | unit | Hub (RegisterClient, UnregisterClient, Broadcast, JoinRoom, ChatMessage, PrivateMessage, LeaveRoom) |
| `models/hub_extended_test.go` | 11 | unit | Hub extended (DuplicateRegister, UnknownMessageType, BroadcastSenderIncluded, ConcurrentClients, RoomIsolation, EmptyRoomID, JoinSameRoomTwice, HistoryOnJoin, UnregisterWithRooms, PrivateMessageToOfflineUser, ChatMessageFillsSenderInfo) |
| `handlers/websocket_test.go` | 9 | integration | WebSocket (Connection, ConnectionRequiresUserID, JoinRoom, ChatMessage, PrivateMessage, LeaveRoom, MultipleRooms, MessagePersistence, BroadcastToMultipleClients) |
| `handlers/websocket_extended_test.go` | 11 | integration | WebSocket extended (InvalidJSON, HistoryOnJoin, ConcurrentMessages, DisconnectCleanup, RejoinRoom, MessageOrdering, BroadcastDoesNotLeakBetweenRooms, ServerHandlesMultipleDisconnects, UsernameFallback, PrivateMessageOnlyReachesRecipient, UnknownMessageType) |

#### Описание тестов

**`models/message_test.go`** (8 unit-тестов):

| Тест | Описание |
|---|---|
| `TestNewChatMessage` | Проверяет создание chat-сообщения: заполнение Type, RoomID, UserID, Content, Timestamp |
| `TestNewPrivateMessage` | Проверяет создание private-сообщения: Type, UserID, ToUserID, Content |
| `TestNewErrorMessage` | Проверяет создание error-сообщения: Type, Error |
| `TestNewUserJoinedMessage` | Проверяет создание уведомления user_joined: Type, RoomID, UserID |
| `TestNewUserLeftMessage` | Проверяет создание уведомления user_left: Type, RoomID, UserID |
| `TestClientAddRoom` | Client.AddRoom добавляет комнату в карту клиента, IsInRoom возвращает true |
| `TestClientRemoveRoom` | Client.RemoveRoom удаляет комнату, IsInRoom возвращает false |
| `TestClientSendMessage` | Client.SendMessage отправляет сообщение в буферизированный канал Send |

**`models/room_test.go`** (5 unit-тестов):

| Тест | Описание |
|---|---|
| `TestRoomAddClient` | Добавление клиента в комнату, проверка GetClientCount |
| `TestRoomRemoveClient` | Удаление клиента из комнаты, проверка GetClientCount |
| `TestRoomBroadcast` | Broadcast сообщения всем клиентам в комнате (3 клиента) |
| `TestRoomBroadcastExclude` | Broadcast с исключением отправителя (excluded не получает) |
| `TestRoomIsEmpty` | Новая комната пуста, после AddClient не пуста, после RemoveClient снова пуста |

**`models/hub_test.go`** (7 unit-тестов):

| Тест | Описание |
|---|---|
| `TestHubRegisterClient` | Регистрация клиента: проверка GetClientCount и сохранение User в Storage |
| `TestHubUnregisterClient` | Отмена регистрации: проверка GetClientCount и закрытие канала Send |
| `TestHubBroadcast` | Broadcast через Hub всем клиентам (3 клиента, без комнат) |
| `TestHubJoinRoom` | Присоединение к комнате: IsInRoom, GetRoomCount, сохранение Room в Storage |
| `TestHubChatMessage` | Отправка chat: оба клиента получают, сообщение сохраняется в Persister |
| `TestHubPrivateMessage` | Приватное сообщение: получатель получает, отправитель получает echo с ToUserID |
| `TestHubLeaveRoom` | Выход из комнаты: IsInRoom=false, пустая комната удаляется (GetRoomCount=0) |

**`models/hub_extended_test.go`** (11 unit-тестов):

| Тест | Описание |
|---|---|
| `TestHubDuplicateRegister` | Повторная регистрация того же клиента не увеличивает счётчик |
| `TestHubUnknownMessageType` | Неизвестный тип сообщения возвращает error |
| `TestHubBroadcastSenderIncluded` | Отправитель chat получает своё сообщение (broadcast без exclude) |
| `TestHubConcurrentClients` | 10 конкурентных регистраций + 10 join + chat: все получают сообщение |
| `TestHubRoomIsolation` | Два клиента в разных комнатах: сообщения не просачиваются |
| `TestHubEmptyRoomID` | Join с пустым RoomID возвращает error |
| `TestHubJoinSameRoomTwice` | Двойной join в одну комнату: клиент в комнате, счётчик = 1 |
| `TestHubHistoryOnJoin` | При join клиент получает историю сообщений из Storage |
| `TestHubUnregisterWithRooms` | Отмена регистрации очищает все комнаты клиента |
| `TestHubPrivateMessageToOfflineUser` | PM офлайн-пользователю: echo отправителю, сообщение сохраняется |
| `TestHubChatMessageFillsSenderInfo` | Chat сообщение: проставляются UserID, Username, Timestamp |

**`handlers/websocket_test.go`** (9 интеграционных тестов):

| Тест | Описание |
|---|---|
| `TestWebSocketConnection` | Подключение к WebSocket с валидными параметрами |
| `TestWebSocketConnectionRequiresUserID` | Подключение без user_id возвращает 400 |
| `TestWebSocketJoinRoom` | Join room: получение user_joined уведомления |
| `TestWebSocketChatMessage` | Два клиента в комнате: chat достигает обоих, UserID заполнен |
| `TestWebSocketPrivateMessage` | PM: получатель получает, отправитель получает echo, UserID/ToUserID корректны |
| `TestWebSocketLeaveRoom` | Leave room: получение user_left уведомления |
| `TestWebSocketMultipleRooms` | Два клиента в разных комнатах: сообщения изолированы |
| `TestWebSocketMessagePersistence` | Chat сообщение сохраняется в Persister после отправки |
| `TestWebSocketBroadcastToMultipleClients` | 3 клиента в одной комнате: все получают broadcast |

**`handlers/websocket_extended_test.go`** (11 интеграционных тестов):

| Тест | Описание |
|---|---|
| `TestWebSocketInvalidJSON` | Невалидный JSON от клиента возвращает error |
| `TestWebSocketHistoryOnJoin` | При join клиент получает предзаполненную историю (3 сообщения) |
| `TestWebSocketConcurrentMessages` | 5 клиентов отправляют сообщения конкурентно: все n\*n сообщений доставлены |
| `TestWebSocketDisconnectCleanup` | После отключения клиента Hub очищает комнаты (GetClientCount=0, GetRoomCount=0) |
| `TestWebSocketRejoinRoom` | Join → leave → join: корректное user_joined уведомление |
| `TestWebSocketMessageOrdering` | Последовательные сообщения: порядок сохраняется на обоих клиентах |
| `TestWebSocketBroadcastDoesNotLeakBetweenRooms` | 3 клиента, 2 комнаты: сообщения не просачиваются между комнатами |
| `TestWebSocketServerHandlesMultipleDisconnects` | 20 конкурентных подключений/отключений: отсутствие panics |
| `TestWebSocketUsernameFallback` | Подключение без username: работает с дефолтным "User_{id}" |
| `TestWebSocketPrivateMessageOnlyReachesRecipient` | 3 клиента, PM от Alice к Bob: Charlie не получает |
| `TestWebSocketUnknownMessageType` | Неизвестный тип сообщения: получение error |

#### Mocks

`models/mock.go` содержит `MockStorage` и `MockPersister` с thread-safe картами (`sync.Mutex`), так как тесты запускают Hub/WritePump goroutines, обращающиеся к ним конкурентно. Mocks расположены в пакете `models` (рядом с интерфейсами), что предотвращает циклический импорт.

`MockStorage` поддерживает: Users, Rooms, RoomHistory, RoomMembers, Messages, `GetPrivateMessageHistory`, а также `UpsertUserFn` для кастомной логики. `MockPersister` собирает сообщения в слайс `Messages` для проверки сохранения.

#### WritePump batching

WritePump может объединять несколько JSON-сообщений в один WebSocket фрейм через `\n`. В тестах это обрабатывается на уровне чтения: `receiveMessage` в `websocket_test.go` разбивает пришедший фрейм по `\n`, парсит каждый JSON отдельно и буферизует излишки для последующих вызовов. Буфер хранится в пакетной `msgBufs map[*websocket.Conn][]*models.Message`.

### Нагрузочное тестирование

`loadtest/main.go` -- скрипт на Go для stress-test WebSocket сервера с 10,000+ коннектов.

```bash
cd loadtest
go run . -target 10000 -rate 500 -server localhost:8080 -messengers 10 -duration 30s
```

Параметры:
- `-target` -- количество соединений (по умолчанию 10000)
- `-rate` -- скорость подключения в коннектах/сек (по умолчанию 500)
- `-server` -- адрес WebSocket сервера в формате `host:port` (по умолчанию `localhost:8080`)
- `-duration` -- длительность теста после разогрева (по умолчанию 30s)
- `-messengers` -- количество клиентов, отправляющих сообщения (по умолчанию 10)
- `-interval` -- интервал между сообщениями от одного мессенджера (по умолчанию 5s)
- `-skip-check` -- пропустить preflight проверку подключения (по умолчанию false)

#### Preflight проверка

Перед запуском теста скрипт автоматически проверяет доступность сервера:

1. HTTP запрос к `http://<server>/health`
2. WebSocket подключение с параметрами `user_id=preflight&username=Preflight`
3. Отправка `join_room` в комнату `general` и ожидание ответа

Если любой из шагов не удался, тест завершается с понятным сообщением об ошибке. Это предотвращает ситуацию, когда тест "успешно" выполняется с 0 подключениями из-за неверного адреса сервера.

Для пропуска preflight (например, при нестандартной конфигурации сервера):

```bash
go run . -target 1000 -server myhost:8080 -skip-check
```

#### Как это работает

1. **Ramp-up**: клиенты подключаются с заданной скоростью через `time.NewTicker`. Каждый клиент сразу вступает в комнату `general`.
2. **Messengers**: случайное подмножество клиентов периодически отправляет `chat` сообщения с встроенной nanosecond timestamp.
3. **ReadPump**: каждый клиент читает входящие фреймы и детектирует свой собственный echo (сверяя `user_id`). Замеряется round-trip latency от отправки до получения.
4. **Сбор статистики**: thread-safe `statsCollector` с `atomic` счётчиками и `sync.Mutex` для гистограммы задержек.

#### Вывод

```
Connected:         10000
Failed:            0
Success rate:      100.0%
Messages sent:     80
Received (echo):   80
Delivery rate:     100.0%
Latency:
  Average:          2.8ms
  P50 (median):     2.5ms
  P95:              5.1ms
  P99:              12.3ms
  Min:              1.2ms
  Max:              45.6ms
```

Во время теста каждые 5 секунд выводится промежуточная статистика: количество подключений, отправленных/полученных сообщений, средняя и P95 задержка.

#### Лимиты ОС

Для 10,000 одновременных WebSocket соединений необходимо увеличить лимит открытых файловых дескрипторов. Скрипт содержит комментарии с инструкциями для macOS и Linux в начале `main.go`.

#### Запуск локального генератора

macOS может выступать генератором нагрузки на удалённый Linux-сервер. 10,000 исходящих TCP-соединений к одному `IP:PORT` вполне укладываются в стандартный диапазон эфемерных портов macOS (`49152-65535`, ~16384 порта).

Настройка перед запуском:

```bash
# 1. Лимит открытых файлов (системный)
sudo launchctl limit maxfiles 1048576 1048576

# 2. Лимит открытых файлов (текущая сессия)
ulimit -n 1048576

# 3. Максимальное количество файлов в системе
sudo sysctl -w kern.maxfiles=1048576
sudo sysctl -w kern.maxfilesperproc=1048576

# 4. Увеличить диапазон эфемерных портов (опционально, если нужно больше)
sudo sysctl -w net.inet.ip.portrange.first=16384
sudo sysctl -w net.inet.ip.portrange.last=65535

# 5. Проверка
ulimit -n
sysctl net.inet.ip.portrange.first net.inet.ip.portrange.last kern.maxfiles
```

**Важно**: `kern.maxfiles` и `launchctl limit maxfiles` требуют перезагрузки для постоянного эффекта. Без шага 4 динамический диапазон 49152-65535 даёт ~16384 порта, чего достаточно для 10K + резерв.

Запуск теста на macOS:

```bash
cd loadtest
go run . -target 10000 -rate 500 -server <LINUX_SERVER_IP>:8080 -messengers 20 -duration 60s
```

Ожидаемая latency при тесте через интернет: 10-50ms (добавляется сетевой RTT). Остальные метрики (успешность подключений, delivery rate) не зависят от ОС генератора.

#### Запуск с отдельного Linux сервера

Для нагрузочного тестирования с выделенной машины используется схема: **сервер** (запущен чат) + **генератор** (запущен `loadtest/main.go`).

##### 1. Подготовка сервера

Сервер с чатом должен быть доступен генератору по сети. Разверните через Docker Compose:

```bash
git clone <repo> /opt/rt-chat
cd /opt/rt-chat
docker compose up --build -d
```

Проверьте, что backend слушает на порту 8080 (или проброшен через nginx). Убедитесь, что файрволл открывает порт для генератора.

На сервере также нужно увеличить лимиты ОС — он держит 10,000+ входящих соединений:

```bash
# /etc/security/limits.conf
*         hard    nofile      1048576
*         soft    nofile      1048576

# /etc/sysctl.conf (или sysctl -w)
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=65535
net.ipv4.ip_local_port_range="1024 65535"
net.ipv4.tcp_tw_reuse=1
```

После изменений перезайти в сессию (`ulimit -n` покажет 1048576).

##### 2. Подготовка генератора

Генератор -- отдельная Linux машина (чем больше ядер/сети, тем лучше). Клонируйте только директорию `loadtest` или весь репозиторий:

```bash
git clone <repo> /opt/rt-chat
cd /opt/rt-chat/loadtest
go mod download
```

На генераторе также поднимите лимиты ОС (аналогично серверу), т.к. 10,000 исходящих сокетов требуют тех же дескрипторов.

##### 3. Запуск теста

```bash
cd /opt/rt-chat/loadtest

# Быстрая проверка (10 коннектов)
go run . -target 10 -rate 10 -server <SERVER_IP>:8080 -messengers 2 -duration 10s

# Полноценный тест (10,000 коннектов)
go run . -target 10000 -rate 500 -server <SERVER_IP>:8080 -messengers 20 -duration 60s -interval 3s
```

- `-server <SERVER_IP>:8080` — указывает на сервер с чатом (вместо localhost)
- `-rate 500` — 500 новых коннектов в секунду. Ramp-up займёт ~20 секунд, затем 60 секунд нагрузки.
- `-messengers 20` — 20 клиентов отправляют сообщения каждые 3 секунды = ~6.6 msg/s.
- `rate` не должен превышать возможности сети и CPU сервера. Начните с 200-500.

##### 4. Мониторинг во время теста

На генераторе:

```bash
# Статистика от самого скрипта (выводится каждые 5с):
#   [5s] conn: 5000  failed: 0  msgs: 15 sent / 15 recv  latency (avg/p95): 3.1ms / 5.2ms

# Нагрузка на генератор:
top -bn1 | head -5          # CPU и память
sar -n TCP,DEV 1 5          # TCP-статистика и трафик по интерфейсам
ss -s                       # Сводка по сокетам
```

На сервере:

```bash
docker stats                   # Потребление контейнеров
ss -s                          # Сводка сокетов (ожидается ~10,000 ESTAB)
netstat -an | grep :8080 | wc -l  # Количество соединений на порту 8080
sar -n TCP,DEV 1 5             # TCP-статистика
```

##### 5. Типичные проблемы и их решения

| Проблема | Причина | Решение |
|---|---|---|
| `connection refused` / preflight failed | Сервер не доступен или неверный адрес | Проверить `-server host:port`, файрволл, `docker compose ps` |
| Preflight failed на health check | Неверный адрес или сервер не запущен | Проверить `curl http://host:port/health`; использовать `-skip-check` для кастомных конфигураций |
| Preflight failed на WebSocket | Сервер работает, но не отвечает на WS | Проверить backend: `docker compose logs backend` |
| `too many open files` | Лимит ОС | `ulimit -n 1048576`, проверить `limits.conf` |
| `cannot assign requested address` | Исчерпаны локальные порты | Увеличить `ip_local_port_range`, включить `tcp_tw_reuse` |
| Массовые отваливания после 5K | nginx/server backlog | Увеличить `net.core.somaxconn` |
| Высокая latency (>100ms) | Утилизация CPU/сети | Уменьшить `rate` или увеличить ресурсы сервера |

##### 6. Результаты

После завершения теста скрипт выводит итоговую сводку. Ожидаемые метрики для чата на Go:

- **Успешных подключений**: 99.9%+ (единичные ошибки при пиковой нагрузке допустимы)
- **Delivery rate**: 99.5%+ (потери 0.5% при переполнении Send каналов допустимы)
- **Average latency**: 2-10ms на локальной сети, 10-50ms через интернет
- **P99 latency**: не более 3x от среднего при стабильной нагрузке

## React Frontend

### Установка и запуск

```bash
cd client
npm install
npm run dev
```

Приложение будет доступно по адресу `http://localhost:5173`

### Архитектура фронтенда

#### Структура

- **components/** - React компоненты
  - `ChatRoom.jsx` - главный контейнер чата
  - `RoomSelector.jsx` - выбор комнаты
  - `MessageFeed.jsx` - лента сообщений с автоскроллом
  - `MessageInput.jsx` - поле ввода сообщения
- **hooks/** - кастомные хуки
  - `useChat.js` - управление WebSocket соединением
- **context/** - глобальное состояние
  - `ChatContext.jsx` - Context API для user state
- **utils/** - константы и утилиты

#### useChat Hook

Кастомный хук `useChat(roomId)` обеспечивает:

- **WebSocket соединение** - автоматическое подключение при монтировании
- **Автоматический реконнект** - до 5 попыток с задержкой 3 секунды
- **История сообщений** - получение последних 50 сообщений при входе в комнату
- **История приватных сообщений** - загрузка через `load_dm_history` при открытии диалога
- **Participant count** - отслеживание количества участников в комнате
- **Отправка сообщений** - `sendMessage(content, type, toUserId)` (chat и private)
- **Переключение комнат** - `joinRoom(newRoomId)` с автоматическим выходом из предыдущей
- **Поиск пользователей** - `lookupUser(username)` с Promise-based API
- **DM контакты** - автоматическое сохранение и восстановление из localStorage
- **Фильтрация ошибок** - ERROR сообщения не попадают в ленту комнаты
- **Состояние соединения** - `isConnected`, `isReconnecting`

```javascript
const {
  messages, isConnected, isReconnecting, participantCount,
  sendMessage, joinRoom, disconnect,
  dmContacts, setDmContacts, dmMessages, lookupUser,
} = useChat(roomId);
```

#### Context API

`ChatContext` управляет глобальным состоянием:

- `userId` - уникальный ID пользователя (сохраняется в localStorage)
- `username` - имя пользователя (сохраняется в localStorage)
- `currentRoom` - текущая комната
- `setCurrentRoom()` - переключение комнаты
- `updateUser()` - обновление данных пользователя
- `theme` - текущая тема (`light` / `dark`)
- `toggleTheme()` - переключение темы
- `activeDmUser` - ID пользователя в активном DM диалоге (`null`, если не в DM)
- `setActiveDmUser()` - открыть / закрыть DM диалог

#### Компоненты

**MessageFeed**
- Автоматический скролл к последнему сообщению с `useRef` и `useEffect`
- Рендеринг разных типов сообщений (chat, private, system, error)
- Визуальное отличие своих сообщений от чужих
- Форматирование времени

**MessageInput**
- Отправка по Enter (Shift+Enter для новой строки)
- Disabled состояние при отключении
- Автоочистка после отправки

**RoomSelector**
- Список доступных комнат
- Индикация активной комнаты
- Переключение комнат одним кликом

### Настройка

Создайте файл `.env` в директории `client`:

```bash
VITE_WS_URL=ws://localhost:8080/ws
```

### Особенности реализации

- **Функциональные компоненты** - только hooks (useState, useEffect, useRef, useCallback)
- **Auto-scroll** - MessageFeed автоматически прокручивается к новым сообщениям
- **Reconnection logic** - автоматический реконнект с exponential backoff
- **Room switching** - автоматический выход из предыдущей комнаты при переключении
- **Message history** - загрузка истории через WebSocket при входе в комнату
- **Participant count** - отображение количества участников в заголовке комнаты
- **Приватные сообщения** - DM между пользователями с поиском по имени
- **DM история** - загрузка истории при открытии диалога, персистентность при офлайн-получателе
- **Echo сообщений** - отправитель приватного сообщения получает echo для отображения в своей ленте
- **Фильтрация ошибок** - ERROR сообщения не отображаются в ленте комнаты
- **Тёмная тема** - переключение через CSS custom properties с сохранением в localStorage
- **Runtime WebSocket URL** - автоопределение адреса из `window.location.host` (без build-time VITE_WS_URL)
- **LocalStorage** - сохранение userId, username, темы и списка DM контактов между сессиями

## Docker

### Структура

Проект включает три Docker-контейнера, описанных в `docker-compose.yml`:

- **db** -- PostgreSQL 16 (Alpine) с постоянным томом для данных
- **backend** -- Go сервер (мультистейдж билд: golang:1.26-alpine -> alpine:3.21)
- **frontend** -- React клиент (мультистейдж билд: node:22 -> nginx:alpine)

### Запуск

```bash
docker compose up --build
```

После запуска:
- Frontend: http://localhost:3000
- Backend WebSocket: ws://localhost:8080/ws
- Health check: http://localhost:8080/health
- PostgreSQL: localhost:5432

### Остановка

```bash
docker compose down
```

Для удаления томов с данными БД:

```bash
docker compose down -v
```

### Сборка образов по отдельности

```bash
# Только backend
docker build -t rt-chat-backend ./server

# Только frontend (с указанием адреса WebSocket)
docker build --build-arg VITE_WS_URL=ws://localhost:8080/ws -t rt-chat-frontend ./client
```

### Переменные окружения

- `DATABASE_URL` -- строка подключения к PostgreSQL для backend (в Docker Compose настроена автоматически)
- `VITE_WS_URL` -- адрес WebSocket сервера для frontend (передаётся как build arg)

### Особенности

- База данных инициализируется автоматически при старте backend (схема создаётся через `InitSchema`)
- Frontend использует nginx для раздачи статики
- Backend ожидает готовности PostgreSQL через healthcheck (pg_isready)
- Данные PostgreSQL сохраняются в Docker volume `pgdata`