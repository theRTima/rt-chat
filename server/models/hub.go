package models

import (
	"encoding/json"
	"log"
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

	// Mutex для защиты доступа к clients map (дополнительная защита)
	mu sync.RWMutex
}

// ClientMessage представляет сообщение от клиента с контекстом
type ClientMessage struct {
	Client  *Client
	Message *Message
}

// NewHub создает новый Hub
func NewHub() *Hub {
	return &Hub{
		Broadcast:   make(chan []byte, 256),
		Register:    make(chan *Client),
		Unregister:  make(chan *Client),
		Message:     make(chan *ClientMessage, 256),
		clients:     make(map[*Client]bool),
		userClients: make(map[string]*Client),
		rooms:       make(map[string]*Room),
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
			}
			h.mu.Unlock()
			log.Printf("Client %s connected. Total clients: %d", client.UserID, len(h.clients))

		case client := <-h.Unregister:
			// Отключение клиента
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				// Удаляем клиента из всех комнат
				for _, room := range h.rooms {
					room.RemoveClient(client)
				}

				// Удаляем клиента из общих карт
				delete(h.clients, client)
				if client.UserID != "" {
					delete(h.userClients, client.UserID)
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
	}
	h.mu.Unlock()

	// Добавляем клиента в комнату
	room.AddClient(client)
	client.AddRoom(msg.RoomID)

	// Уведомляем всех в комнате о новом пользователе
	notification := NewUserJoinedMessage(msg.RoomID, client.UserID, client.Username)
	h.broadcastToRoom(msg.RoomID, notification, nil)
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

	// Удаляем клиента из комнаты
	room.RemoveClient(client)
	client.RemoveRoom(msg.RoomID)

	// Уведомляем всех в комнате о выходе пользователя
	notification := NewUserLeftMessage(msg.RoomID, client.UserID, client.Username)
	h.broadcastToRoom(msg.RoomID, notification, nil)

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

	// Находим получателя
	h.mu.RLock()
	recipient, exists := h.userClients[msg.ToUserID]
	h.mu.RUnlock()

	if !exists {
		client.SendMessage(NewErrorMessage("Recipient not found or offline"))
		return
	}

	// Отправляем сообщение получателю
	recipient.SendMessage(msg)

	// Отправляем подтверждение отправителю (опционально)
	log.Printf("Private message from %s to %s delivered", client.UserID, msg.ToUserID)
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
