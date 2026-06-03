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

```bash
cd server
go test ./...
```

#### Структура

- **`models/hub_test.go`** -- unit-тесты Hub (регистрация, сообщения, выход из комнаты)
- **`models/hub_extended_test.go`** -- расширенные тесты Hub (дубликаты регистрации, изоляция комнат, конкурентные клиенты, приватные сообщения, история при входе, очистка пустых комнат, заполнение полей отправителя)
- **`handlers/websocket_test.go`** -- интеграционные тесты WebSocket (чат, порядок сообщений, изоляция комнат, reconnect, burst disconnect, приватные сообщения, fallback username)
- **`handlers/websocket_extended_test.go`** -- расширенные интеграционные тесты (невалидный JSON, история при входе, конкурентные сообщения)

#### Mocks

`models/mock.go` содержит `MockStorage` и `MockPersister` с thread-safe картами (`sync.Mutex`), так как тесты запускают Hub/WritePump goroutines, обращающиеся к ним конкурентно. Mocks расположены в пакете `models` (рядом с интерфейсами), что предотвращает циклический импорт.

#### WritePump batching

WritePump может объединять несколько JSON-сообщений в один WebSocket фрейм через `\n`. В тестах это обрабатывается на уровне чтения: `receiveMessage` в `websocket_test.go` разбивает пришедший фрейм по `\n`, парсит каждый JSON отдельно и буферизует излишки для последующих вызовов. Буфер хранится в пакетной `msgBufs map[*websocket.Conn][]*models.Message`.

### Нагрузочное тестирование

`loadtest/main.go` -- скрипт на Go для stress-test WebSocket сервера с 10,000+ коннектов.

```bash
cd loadtest
go run . -target 10000 -rate 500 -messengers 10 -duration 30s
```

Параметры:
- `-target` -- количество соединений (по умолчанию 10000)
- `-rate` -- скорость подключения в коннектах/сек (по умолчанию 500)
- `-server` -- адрес WebSocket сервера (по умолчанию localhost:8080)
- `-duration` -- длительность теста после разогрева (по умолчанию 30s)
- `-messengers` -- количество клиентов, отправляющих сообщения (по умолчанию 10)
- `-interval` -- интервал между сообщениями от одного мессенджера (по умолчанию 5s)

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
| `connection refused` | Сервер не доступен | Проверить `SERVER_IP`, порт, файрволл |
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
- **Отправка сообщений** - `sendMessage(content, type, toUserId)`
- **Переключение комнат** - `joinRoom(newRoomId)` с автоматическим выходом из предыдущей
- **Состояние соединения** - `isConnected`, `isReconnecting`

```javascript
const { messages, isConnected, sendMessage, joinRoom } = useChat(roomId);
```

#### Context API

`ChatContext` управляет глобальным состоянием:

- `userId` - уникальный ID пользователя (сохраняется в localStorage)
- `username` - имя пользователя (сохраняется в localStorage)
- `currentRoom` - текущая комната
- `setCurrentRoom()` - переключение комнаты
- `updateUser()` - обновление данных пользователя

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
- **LocalStorage** - сохранение userId и username между сессиями

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