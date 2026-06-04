package handlers

import (
	"log"
	"net/http"
	"sync/atomic"

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

var TotalAttempts int64
var UpgradeFailures int64

// ServeWs обрабатывает WebSocket запросы от клиентов
func ServeWs(hub *models.Hub, persister models.MessagePersister, w http.ResponseWriter, r *http.Request) {
	curr := atomic.AddInt64(&TotalAttempts, 1)
	if curr%100 == 0 || curr <= 5 {
		log.Printf("[diag] ServeWs called: %d (hub clients: %d)", curr, hub.GetClientCount())
	}

	// Получаем параметры из query string
	userID := r.URL.Query().Get("user_id")
	username := r.URL.Query().Get("username")

	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	if username == "" {
		username = "User_" + userID
	}

	// Апгрейдим HTTP соединение до WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		atomic.AddInt64(&UpgradeFailures, 1)
		log.Printf("Upgrade error #%d: %v", atomic.LoadInt64(&UpgradeFailures), err)
		return
	}

	// Создаем нового клиента
	client := &models.Client{
		Hub:       hub,
		Conn:      conn,
		Send:      make(chan []byte, 256),
		UserID:    userID,
		Username:  username,
		Persister: persister,
	}

	// Регистрируем клиента в hub
	client.Hub.Register <- client

	// Запускаем goroutines для чтения и записи
	// Каждая goroutine работает независимо для обеспечения thread-safety
	go client.WritePump()
	go client.ReadPump()
}
