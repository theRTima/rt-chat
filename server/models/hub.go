package models

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"
)

// Hub управляет активными клиентами, комнатами и маршрутизацией сообщений
// Использует каналы для thread-safe операций с клиентами
type Hub struct {
	// Зарегистрированные клиенты (map для быстрого доступа)
	clients map[*Client]bool

	// Карта клиентов по UserID для приватных сообщений
	userClients map[string]*Client

	// Карта клиентов по Username (в нижнем регистре) для поиска
	userByUsername map[string]*Client

	// Комнаты (map: roomID -> Room)
	rooms map[string]*Room

	// Канал для broadcast сообщений всем клиентам
	Broadcast chan []byte

	// Канал для регистрации новых клиентов
	Register chan *Client

	// Канал для отключения клиентов
	Unregister chan *Client

	// Канал для обработки сообщений
	Message chan *ClientMessage

	// Storage интерфейс для работы с базой данных
	Storage Storage

	// Mutex для защиты доступа к clients map (дополнительная защита)
	mu sync.RWMutex
}

// Storage интерфейс для работы с базой данных
type Storage interface {
	UpsertUser(ctx context.Context, userID, username string) error
	UpsertRoom(ctx context.Context, roomID, name string) error
	GetRoomHistory(ctx context.Context, roomID string, limit int) ([]*Message, error)
	AddRoomMember(ctx context.Context, roomID, userID string) error
	RemoveRoomMember(ctx context.Context, roomID, userID string) error
	GetPrivateMessageHistory(ctx context.Context, userID1, userID2 string, limit int) ([]*Message, error)
}

// MessagePersister интерфейс для асинхронного сохранения сообщений
type MessagePersister interface {
	Enqueue(msg *Message)
}

// ClientMessage представляет сообщение от клиента с контекстом
type ClientMessage struct {
	Client  *Client
	Message *Message
}

// NewHub создает новый Hub
func NewHub(storage Storage) *Hub {
	return &Hub{
		Broadcast:      make(chan []byte, 256),
		Register:       make(chan *Client),
		Unregister:     make(chan *Client),
		Message:        make(chan *ClientMessage, 256),
		clients:        make(map[*Client]bool),
		userClients:    make(map[string]*Client),
		userByUsername: make(map[string]*Client),
		rooms:          make(map[string]*Room),
		Storage:        storage,
	}
}

// Run запускает основной цикл Hub
// Должен быть запущен в отдельной goroutine
// Обрабатывает регистрацию/отключение клиентов и маршрутизацию сообщений
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			// Регистрация нового клиента
			h.mu.Lock()
			h.clients[client] = true
			if client.UserID != "" {
				h.userClients[client.UserID] = client
				if client.Username != "" {
					h.userByUsername[strings.ToLower(client.Username)] = client
				}
			}
			h.mu.Unlock()

			// Сохраняем пользователя в БД (асинхронно, не блокируем регистрацию)
			if h.Storage != nil {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					if err := h.Storage.UpsertUser(ctx, client.UserID, client.Username); err != nil {
						log.Printf("Failed to save user to database: %v", err)
					}
				}()
			}

			log.Printf("Client %s connected. Total clients: %d", client.UserID, len(h.clients))

		case client := <-h.Unregister:
			// Отключение клиента
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				// Удаляем клиента из всех комнат
				for _, room := range h.rooms {
					room.RemoveClient(client)
				}

				// Удаляем пустые комнаты
				for id, room := range h.rooms {
					if room.IsEmpty() {
						delete(h.rooms, id)
						log.Printf("Deleted empty room: %s", id)
					}
				}

				// Удаляем клиента из общих карт
				delete(h.clients, client)
				if client.UserID != "" {
					delete(h.userClients, client.UserID)
					if client.Username != "" {
						key := strings.ToLower(client.Username)
						if c, ok := h.userByUsername[key]; ok && c == client {
							delete(h.userByUsername, key)
						}
					}
				}
				close(client.Send)
				log.Printf("Client %s disconnected. Total clients: %d", client.UserID, len(h.clients))
			}
			h.mu.Unlock()

		case clientMsg := <-h.Message:
			// Обработка сообщения от клиента
			h.handleMessage(clientMsg.Client, clientMsg.Message)

		case message := <-h.Broadcast:
			// Broadcast сообщения всем подключенным клиентам (legacy support)
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.Send <- message:
					// Сообщение успешно отправлено в канал клиента
				default:
					// Канал клиента заполнен, отключаем клиента
					close(client.Send)
					delete(h.clients, client)
					log.Printf("Client send channel full, disconnecting")
				}
			}
			h.mu.RUnlock()
		}
	}
}

// GetClientCount возвращает количество подключенных клиентов (thread-safe)
func (h *Hub) GetClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// handleMessage обрабатывает входящее сообщение и маршрутизирует его
func (h *Hub) handleMessage(client *Client, msg *Message) {
	switch msg.Type {
	case MessageTypeJoinRoom:
		h.handleJoinRoom(client, msg)

	case MessageTypeLeaveRoom:
		h.handleLeaveRoom(client, msg)

	case MessageTypeChat:
		h.handleChatMessage(client, msg)

	case MessageTypePrivate:
		h.handlePrivateMessage(client, msg)

	case MessageTypeUserLookup:
		h.handleUserLookup(client, msg)

	case MessageTypeLoadDMHistory:
		h.handleLoadDMHistory(client, msg)

	default:
		// Неизвестный тип сообщения
		errMsg := NewErrorMessage("Unknown message type")
		client.SendMessage(errMsg)
	}
}

// handleJoinRoom обрабатывает присоединение клиента к комнате
func (h *Hub) handleJoinRoom(client *Client, msg *Message) {
	if msg.RoomID == "" {
		client.SendMessage(NewErrorMessage("Room ID is required"))
		return
	}

	h.mu.Lock()
	// Создаем комнату, если она не существует
	room, exists := h.rooms[msg.RoomID]
	if !exists {
		room = NewRoom(msg.RoomID)
		h.rooms[msg.RoomID] = room
		log.Printf("Created new room: %s", msg.RoomID)

		// Сохраняем комнату в БД (асинхронно)
		if h.Storage != nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := h.Storage.UpsertRoom(ctx, msg.RoomID, msg.RoomID); err != nil {
					log.Printf("Failed to save room to database: %v", err)
				}
			}()
		}
	}
	h.mu.Unlock()

	// Добавляем клиента в комнату
	room.AddClient(client)
	client.AddRoom(msg.RoomID)

	// Сохраняем членство в БД (асинхронно)
	if h.Storage != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := h.Storage.AddRoomMember(ctx, msg.RoomID, client.UserID); err != nil {
				log.Printf("Failed to save room member to database: %v", err)
			}
		}()
	}

	// Отправляем историю сообщений клиенту
	if h.Storage != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			history, err := h.Storage.GetRoomHistory(ctx, msg.RoomID, 50)
			if err != nil {
				log.Printf("Failed to load room history: %v", err)
				return
			}

			// Отправляем историю клиенту
			for _, historyMsg := range history {
				client.SendMessage(historyMsg)
			}
			log.Printf("Sent %d history messages to user %s in room %s", len(history), client.UserID, msg.RoomID)
		}()
	}

	// Уведомляем всех в комнате о новом пользователе
	notification := NewUserJoinedMessage(msg.RoomID, client.UserID, client.Username)
	notification.ParticipantCount = room.GetClientCount()
	h.broadcastToRoom(msg.RoomID, notification, nil)

	// Сохраняем уведомление в БД через persister
	if client.Persister != nil {
		client.Persister.Enqueue(notification)
	}
}

// handleLeaveRoom обрабатывает выход клиента из комнаты
func (h *Hub) handleLeaveRoom(client *Client, msg *Message) {
	if msg.RoomID == "" {
		client.SendMessage(NewErrorMessage("Room ID is required"))
		return
	}

	h.mu.RLock()
	room, exists := h.rooms[msg.RoomID]
	h.mu.RUnlock()

	if !exists {
		client.SendMessage(NewErrorMessage("Room not found"))
		return
	}

	// Создаем уведомление до удаления клиента из комнаты
	notification := NewUserLeftMessage(msg.RoomID, client.UserID, client.Username)

	// Получаем количество до удаления (включает выходящего клиента)
	beforeCount := room.GetClientCount()

	// Отправляем уведомление выходящему клиенту напрямую
	client.SendMessage(notification)

	// Удаляем клиента из комнаты
	room.RemoveClient(client)
	client.RemoveRoom(msg.RoomID)

	// Количество после выхода
	notification.ParticipantCount = beforeCount - 1

	// Сохраняем выход в БД (асинхронно)
	if h.Storage != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := h.Storage.RemoveRoomMember(ctx, msg.RoomID, client.UserID); err != nil {
				log.Printf("Failed to update room member in database: %v", err)
			}
		}()
	}

	// Уведомляем остальных в комнате о выходе пользователя
	h.broadcastToRoom(msg.RoomID, notification, client)

	// Сохраняем уведомление в БД через persister
	if client.Persister != nil {
		client.Persister.Enqueue(notification)
	}

	// Удаляем комнату, если она пуста
	if room.IsEmpty() {
		h.mu.Lock()
		delete(h.rooms, msg.RoomID)
		h.mu.Unlock()
		log.Printf("Deleted empty room: %s", msg.RoomID)
	}
}

// handleChatMessage обрабатывает сообщение в комнату
func (h *Hub) handleChatMessage(client *Client, msg *Message) {
	if msg.RoomID == "" {
		client.SendMessage(NewErrorMessage("Room ID is required"))
		return
	}

	// Заполняем информацию об отправителе
	msg.UserID = client.UserID
	msg.Username = client.Username
	msg.Timestamp = time.Now()

	// Отправляем сообщение в комнату
	h.broadcastToRoom(msg.RoomID, msg, nil)

	// Асинхронно сохраняем сообщение через persister (не блокируем broadcast)
	if client.Persister != nil {
		client.Persister.Enqueue(msg)
	}
}

// handlePrivateMessage обрабатывает приватное сообщение
func (h *Hub) handlePrivateMessage(client *Client, msg *Message) {
	if msg.ToUserID == "" {
		client.SendMessage(NewErrorMessage("Recipient user ID is required"))
		return
	}

	// Заполняем информацию об отправителе
	msg.UserID = client.UserID
	msg.Username = client.Username
	msg.Timestamp = time.Now()

	// Находим получателя и отправляем, если онлайн
	h.mu.RLock()
	recipient, exists := h.userClients[msg.ToUserID]
	h.mu.RUnlock()

	if exists {
		recipient.SendMessage(msg)
	}

	// Отправляем эхо отправителю
	client.SendMessage(msg)

	// Сохраняем сообщение в БД (даже если получатель офлайн)
	if client.Persister != nil {
		client.Persister.Enqueue(msg)
	}

	if exists {
		log.Printf("Private message from %s to %s delivered", client.UserID, msg.ToUserID)
	}
}

// handleLoadDMHistory обрабатывает запрос загрузки истории личных сообщений
func (h *Hub) handleLoadDMHistory(client *Client, msg *Message) {
	if msg.ToUserID == "" {
		client.SendMessage(NewErrorMessage("User ID is required"))
		return
	}

	if h.Storage == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		history, err := h.Storage.GetPrivateMessageHistory(ctx, client.UserID, msg.ToUserID, 100)
		if err != nil {
			log.Printf("Failed to load DM history: %v", err)
			return
		}

		for _, historyMsg := range history {
			client.SendMessage(historyMsg)
		}
		log.Printf("Sent %d DM history messages to user %s for conversation with %s", len(history), client.UserID, msg.ToUserID)
	}()
}

// handleUserLookup обрабатывает запрос поиска пользователя по имени
func (h *Hub) handleUserLookup(client *Client, msg *Message) {
	if msg.Content == "" {
		client.SendMessage(NewErrorMessage("Username is required"))
		return
	}

	h.mu.RLock()
	found, exists := h.userByUsername[strings.ToLower(msg.Content)]
	h.mu.RUnlock()

	if exists {
		client.SendMessage(NewUserFoundMessage(found.UserID, found.Username))
	} else {
		client.SendMessage(NewUserNotFoundMessage(msg.Content))
	}
}

// broadcastToRoom отправляет сообщение всем клиентам в комнате
func (h *Hub) broadcastToRoom(roomID string, msg *Message, exclude *Client) {
	h.mu.RLock()
	room, exists := h.rooms[roomID]
	h.mu.RUnlock()

	if !exists {
		log.Printf("Room %s not found for broadcast", roomID)
		return
	}

	// Сериализуем сообщение
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		return
	}

	// Отправляем через Room
	room.Broadcast(data, exclude)
}

// GetRoomCount возвращает количество активных комнат
func (h *Hub) GetRoomCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms)
}
