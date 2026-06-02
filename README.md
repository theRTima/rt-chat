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
db/
  migrations/
    001_initial_schema.sql  # SQL схема базы данных
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
- `user_joined` -- уведомление о входе пользователя
- `user_left` -- уведомление о выходе пользователя

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

### Структура Message

```go
type Message struct {
    Type      string    `json:"type"`              // Тип сообщения
    RoomID    string    `json:"room_id,omitempty"` // ID комнаты
    UserID    string    `json:"user_id,omitempty"` // ID отправителя
    Username  string    `json:"username,omitempty"`// Имя отправителя
    ToUserID  string    `json:"to_user_id,omitempty"` // ID получателя (для private)
    Content   string    `json:"content,omitempty"` // Содержимое
    Timestamp time.Time `json:"timestamp"`         // Временная метка
    Error     string    `json:"error,omitempty"`   // Текст ошибки
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

### Тестирование

Тестирование с помощью websocat:

```bash
# Установка websocat (если еще не установлен)
brew install websocat

# Подключение первого пользователя
websocat "ws://localhost:8080/ws?user_id=alice&username=Alice"

# В другом терминале -- второй пользователь
websocat "ws://localhost:8080/ws?user_id=bob&username=Bob"
```

#### Пример 1: Общение в комнате

**Терминал Alice:**
```json
{"type": "join_room", "room_id": "general"}
```

**Терминал Bob:**
```json
{"type": "join_room", "room_id": "general"}
```

**Alice отправляет сообщение:**
```json
{"type": "chat", "room_id": "general", "content": "Hello everyone!"}
```

**Bob получает:**
```json
{"type":"chat","room_id":"general","user_id":"alice","username":"Alice","content":"Hello everyone!","timestamp":"2026-06-02T20:30:00Z"}
```

#### Пример 2: Приватное сообщение

**Alice отправляет Bob:**
```json
{"type": "private", "to_user_id": "bob", "content": "Hi Bob, this is private!"}
```

**Bob получает:**
```json
{"type":"private","user_id":"alice","username":"Alice","to_user_id":"bob","content":"Hi Bob, this is private!","timestamp":"2026-06-02T20:35:00Z"}
```

#### Пример 3: Несколько комнат

**Alice:**
```json
{"type": "join_room", "room_id": "dev-team"}
{"type": "chat", "room_id": "dev-team", "content": "Meeting in 5 minutes"}
```

**Bob (в другой комнате):**
```json
{"type": "join_room", "room_id": "general"}
{"type": "chat", "room_id": "general", "content": "Anyone here?"}
```

Сообщения Alice видны только в комнате `dev-team`, сообщения Bob -- только в `general`.

## Текущий статус

- [x] Базовая Hub-and-Spoke архитектура
- [x] WebSocket handler с upgrade
- [x] Client с readPump и writePump goroutine
- [x] Thread-safe управление соединениями
- [x] Система комнат (rooms)
- [x] Приватные сообщения между пользователями
- [x] JSON протокол для всех типов сообщений
- [x] Маршрутизация сообщений по комнатам и пользователям
- [x] Уведомления о входе/выходе пользователей
- [x] Персистентность (PostgreSQL)
- [x] Схема базы данных (users, rooms, messages, room_members)
- [x] Асинхронное сохранение сообщений (worker pool, batch inserts)
- [x] История сообщений (последние 50 при входе в комнату)
- [ ] Frontend на React
- [ ] Нагрузочное тестирование (10K соединений)
