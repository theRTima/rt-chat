package models

import (
	"log"
	"sync"
)

// Room представляет чат-комнату с множеством клиентов
type Room struct {
	// ID комнаты
	ID string

	// Клиенты в комнате
	clients map[*Client]bool

	// Mutex для защиты доступа к clients
	mu sync.RWMutex
}

// NewRoom создает новую комнату
func NewRoom(id string) *Room {
	return &Room{
		ID:      id,
		clients: make(map[*Client]bool),
	}
}

// AddClient добавляет клиента в комнату
func (r *Room) AddClient(client *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[client] = true
	log.Printf("Client %s joined room %s. Total in room: %d", client.UserID, r.ID, len(r.clients))
}

// RemoveClient удаляет клиента из комнаты
func (r *Room) RemoveClient(client *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.clients[client]; ok {
		delete(r.clients, client)
		log.Printf("Client %s left room %s. Total in room: %d", client.UserID, r.ID, len(r.clients))
	}
}

// Broadcast отправляет сообщение всем клиентам в комнате
func (r *Room) Broadcast(message []byte, exclude *Client) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for client := range r.clients {
		// Не отправляем сообщение отправителю, если exclude указан
		if exclude != nil && client == exclude {
			continue
		}

		select {
		case client.Send <- message:
			// Сообщение отправлено
		default:
			// Канал клиента заполнен - пропускаем
			log.Printf("Client %s send channel full in room %s", client.UserID, r.ID)
		}
	}
}

// GetClientCount возвращает количество клиентов в комнате
func (r *Room) GetClientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

// IsEmpty проверяет, пуста ли комната
func (r *Room) IsEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients) == 0
}
