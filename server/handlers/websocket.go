package handlers

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/theRTima/rt-chat/models"
)

// upgrader используется для апгрейда HTTP соединения до WebSocket
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Разрешаем соединения с любых origin (для разработки)
	// В production следует ограничить конкретными доменами
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// ServeWs обрабатывает WebSocket запросы от клиентов
func ServeWs(hub *models.Hub, w http.ResponseWriter, r *http.Request) {
	// Апгрейдим HTTP соединение до WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	// Создаем нового клиента
	client := &models.Client{
		Hub:  hub,
		Conn: conn,
		Send: make(chan []byte, 256),
	}

	// Регистрируем клиента в hub
	client.Hub.Register <- client

	// Запускаем goroutines для чтения и записи
	// Каждая goroutine работает независимо для обеспечения thread-safety
	go client.WritePump()
	go client.ReadPump()
}
