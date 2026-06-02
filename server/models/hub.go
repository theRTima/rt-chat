package models

import (
	"log"
	"sync"
)

// Hub управляет активными клиентами и broadcast сообщений
// Использует каналы для thread-safe операций с клиентами
type Hub struct {
	// Зарегистрированные клиенты (map для быстрого доступа)
	clients map[*Client]bool

	// Канал для broadcast сообщений всем клиентам
	Broadcast chan []byte

	// Канал для регистрации новых клиентов
	Register chan *Client

	// Канал для отключения клиентов
	Unregister chan *Client

	// Mutex для защиты доступа к clients map (дополнительная защита)
	mu sync.RWMutex
}

// NewHub создает новый Hub
func NewHub() *Hub {
	return &Hub{
		Broadcast:  make(chan []byte, 256),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

// Run запускает основной цикл Hub
// Должен быть запущен в отдельной goroutine
// Обрабатывает регистрацию/отключение клиентов и broadcast сообщений
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			// Регистрация нового клиента
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("Client connected. Total clients: %d", len(h.clients))

		case client := <-h.Unregister:
			// Отключение клиента
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
				log.Printf("Client disconnected. Total clients: %d", len(h.clients))
			}
			h.mu.Unlock()

		case message := <-h.Broadcast:
			// Broadcast сообщения всем подключенным клиентам
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
