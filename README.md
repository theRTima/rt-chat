# rt-chat
chat system with websocket and rooms

# Вариант 30. Чат-система с WebSocket и комнатами
Предметная область: Коммуникации

Технологии: Go + WebSocket + React

Описание: Высоконагруженный чат-сервер на Go (горутины на каждое
соединение). 

Поддержка комнат, приватных сообщений, истории, push-
уведомлений. 

Веб-клиент на React. Тестирование под нагрузкой до 10K
одновременных соединений [ЛР №13].

---

## Архитектура

### Hub-and-Spoke модель

Проект использует классическую архитектуру Hub-and-Spoke для управления WebSocket соединениями:

- **Hub** - центральный координатор, управляет всеми активными клиентами
- **Client** - представляет одно WebSocket соединение с двумя goroutines (readPump и writePump)
- **Каналы** - обеспечивают thread-safe коммуникацию между компонентами

### Структура проекта

```
server/
├── main.go              # Точка входа, HTTP сервер
├── models/
│   ├── hub.go          # Hub для управления клиентами
│   └── client.go       # Client с read/write goroutines
└── handlers/
    └── websocket.go    # WebSocket upgrade handler
```

### Компоненты

#### Hub (models/hub.go)

Hub управляет всеми активными соединениями и обрабатывает три типа операций через каналы:

- `Register chan *Client` - регистрация новых клиентов
- `Unregister chan *Client` - отключение клиентов
- `Broadcast chan []byte` - рассылка сообщений всем клиентам

Использует `sync.RWMutex` для защиты карты клиентов от race conditions.

#### Client (models/client.go)

Каждый клиент имеет два goroutine:

- **ReadPump** - читает сообщения из WebSocket и отправляет в Hub.Broadcast
- **WritePump** - читает из канала Send и отправляет в WebSocket

Параметры производительности:
- `writeWait: 10s` - таймаут записи
- `pongWait: 60s` - таймаут ожидания pong от клиента
- `pingPeriod: 54s` - интервал ping сообщений
- `maxMessageSize: 512 bytes` - максимальный размер сообщения

#### WebSocket Handler (handlers/websocket.go)

Обрабатывает HTTP запросы и апгрейдит их до WebSocket:

1. Принимает HTTP запрос на `/ws`
2. Апгрейдит соединение до WebSocket
3. Создает нового Client
4. Регистрирует Client в Hub
5. Запускает ReadPump и WritePump goroutines

### Thread Safety

Проект обеспечивает безопасность при конкурентном доступе:

- **Каналы** - основной механизм коммуникации между goroutines
- **sync.RWMutex** - защита карты клиентов в Hub
- **Отдельные goroutines** - каждый Client имеет выделенные reader и writer goroutines

## Запуск сервера

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

- `ws://localhost:8080/ws` - WebSocket endpoint для подключения клиентов
- `http://localhost:8080/health` - health check endpoint
- `http://localhost:8080/stats` - информация о количестве подключенных клиентов

### Тестирование

Простой тест с помощью websocat:

```bash
# Установка websocat (если еще не установлен)
brew install websocat

# Подключение к серверу
websocat ws://localhost:8080/ws

# Теперь можно отправлять сообщения
# Они будут broadcast всем подключенным клиентам
```

Тест с несколькими клиентами:

```bash
# Терминал 1
websocat ws://localhost:8080/ws

# Терминал 2
websocat ws://localhost:8080/ws

# Терминал 3
websocat ws://localhost:8080/ws

# Отправьте сообщение из любого терминала - оно появится во всех остальных
```

## Текущий статус

- [x] Базовая Hub-and-Spoke архитектура
- [x] WebSocket handler с upgrade
- [x] Client с readPump и writePump goroutines
- [x] Thread-safe управление соединениями
- [x] Broadcast сообщений всем клиентам
- [ ] Система комнат (rooms)
- [ ] Приватные сообщения
- [ ] Персистентность (PostgreSQL)
- [ ] React frontend
- [ ] Нагрузочное тестирование (10K соединений)
