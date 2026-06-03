package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/theRTima/rt-chat/models"
	"github.com/theRTima/rt-chat/storage"
)

func setupTestServer() (*httptest.Server, *models.Hub, *storage.MockPersister) {
	mockStorage := storage.NewMockStorage()
	mockPersister := storage.NewMockPersister()
	hub := models.NewHub(mockStorage)

	go hub.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub, mockPersister, w, r)
	})

	server := httptest.NewServer(mux)
	return server, hub, mockPersister
}

func connectWebSocket(t *testing.T, serverURL, userID, username string) *websocket.Conn {
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + "/ws?user_id=" + userID + "&username=" + username

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket: %v", err)
	}

	return ws
}

func sendMessage(t *testing.T, ws *websocket.Conn, msg *models.Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}
}

func receiveMessage(t *testing.T, ws *websocket.Conn, timeout time.Duration) *models.Message {
	ws.SetReadDeadline(time.Now().Add(timeout))

	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read message: %v", err)
	}

	var msg models.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	return &msg
}

func TestWebSocketConnection(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "test_user", "TestUser")
	defer ws.Close()

	// Connection successful if we got here
	if ws == nil {
		t.Fatal("WebSocket connection is nil")
	}
}

func TestWebSocketConnectionRequiresUserID(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	// Try to connect without user_id
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?username=TestUser"

	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("Expected connection to fail without user_id")
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.StatusCode)
	}
}

func TestWebSocketJoinRoom(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws.Close()

	// Send join room message
	joinMsg := &models.Message{
		Type:   models.MessageTypeJoinRoom,
		RoomID: "general",
	}
	sendMessage(t, ws, joinMsg)

	// Should receive user_joined notification
	msg := receiveMessage(t, ws, 2*time.Second)
	if msg.Type != models.MessageTypeUserJoined {
		t.Errorf("Expected user_joined message, got %s", msg.Type)
	}
	if msg.RoomID != "general" {
		t.Errorf("Expected room_id 'general', got %s", msg.RoomID)
	}
}

func TestWebSocketChatMessage(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	// Connect two clients
	ws1 := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws1.Close()

	ws2 := connectWebSocket(t, server.URL, "bob", "Bob")
	defer ws2.Close()

	// Both join the same room
	joinMsg := &models.Message{
		Type:   models.MessageTypeJoinRoom,
		RoomID: "test_room",
	}

	sendMessage(t, ws1, joinMsg)
	sendMessage(t, ws2, joinMsg)

	// Read join notifications
	receiveMessage(t, ws1, 1*time.Second) // alice joined
	receiveMessage(t, ws2, 1*time.Second) // alice joined
	receiveMessage(t, ws1, 1*time.Second) // bob joined
	receiveMessage(t, ws2, 1*time.Second) // bob joined

	// Alice sends a chat message
	chatMsg := &models.Message{
		Type:    models.MessageTypeChat,
		RoomID:  "test_room",
		Content: "Hello Bob!",
	}
	sendMessage(t, ws1, chatMsg)

	// Both clients should receive the message
	msg1 := receiveMessage(t, ws1, 1*time.Second)
	msg2 := receiveMessage(t, ws2, 1*time.Second)

	for _, msg := range []*models.Message{msg1, msg2} {
		if msg.Type != models.MessageTypeChat {
			t.Errorf("Expected chat message, got %s", msg.Type)
		}
		if msg.Content != "Hello Bob!" {
			t.Errorf("Expected content 'Hello Bob!', got %s", msg.Content)
		}
		if msg.UserID != "alice" {
			t.Errorf("Expected user_id 'alice', got %s", msg.UserID)
		}
	}
}

func TestWebSocketPrivateMessage(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws1 := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws1.Close()

	ws2 := connectWebSocket(t, server.URL, "bob", "Bob")
	defer ws2.Close()

	time.Sleep(100 * time.Millisecond) // Let clients register

	// Alice sends private message to Bob
	privateMsg := &models.Message{
		Type:     models.MessageTypePrivate,
		ToUserID: "bob",
		Content:  "Secret message",
	}
	sendMessage(t, ws1, privateMsg)

	// Only Bob should receive the message
	msg := receiveMessage(t, ws2, 1*time.Second)

	if msg.Type != models.MessageTypePrivate {
		t.Errorf("Expected private message, got %s", msg.Type)
	}
	if msg.Content != "Secret message" {
		t.Errorf("Expected content 'Secret message', got %s", msg.Content)
	}
	if msg.UserID != "alice" {
		t.Errorf("Expected user_id 'alice', got %s", msg.UserID)
	}

	// Alice should not receive her own message
	ws1.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := ws1.ReadMessage()
	if err == nil {
		t.Error("Alice should not receive her own private message")
	}
}

func TestWebSocketLeaveRoom(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws.Close()

	// Join room
	joinMsg := &models.Message{
		Type:   models.MessageTypeJoinRoom,
		RoomID: "temp_room",
	}
	sendMessage(t, ws, joinMsg)
	receiveMessage(t, ws, 1*time.Second) // user_joined notification

	// Leave room
	leaveMsg := &models.Message{
		Type:   models.MessageTypeLeaveRoom,
		RoomID: "temp_room",
	}
	sendMessage(t, ws, leaveMsg)

	// Should receive user_left notification
	msg := receiveMessage(t, ws, 1*time.Second)
	if msg.Type != models.MessageTypeUserLeft {
		t.Errorf("Expected user_left message, got %s", msg.Type)
	}
}

func TestWebSocketMultipleRooms(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws1 := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws1.Close()

	ws2 := connectWebSocket(t, server.URL, "bob", "Bob")
	defer ws2.Close()

	// Alice joins room1, Bob joins room2
	sendMessage(t, ws1, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "room1"})
	sendMessage(t, ws2, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "room2"})

	// Read join notifications
	receiveMessage(t, ws1, 1*time.Second)
	receiveMessage(t, ws2, 1*time.Second)

	// Alice sends message to room1
	sendMessage(t, ws1, &models.Message{
		Type:    models.MessageTypeChat,
		RoomID:  "room1",
		Content: "Message in room1",
	})

	// Alice should receive the message
	msg1 := receiveMessage(t, ws1, 1*time.Second)
	if msg1.Content != "Message in room1" {
		t.Errorf("Alice didn't receive her message")
	}

	// Bob should NOT receive the message (different room)
	ws2.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := ws2.ReadMessage()
	if err == nil {
		t.Error("Bob should not receive message from room1")
	}
}

func TestWebSocketMessagePersistence(t *testing.T) {
	server, _, mockPersister := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws.Close()

	// Join room
	sendMessage(t, ws, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "general"})
	receiveMessage(t, ws, 1*time.Second) // join notification

	// Send chat message
	sendMessage(t, ws, &models.Message{
		Type:    models.MessageTypeChat,
		RoomID:  "general",
		Content: "Test persistence",
	})
	receiveMessage(t, ws, 1*time.Second) // echo

	time.Sleep(100 * time.Millisecond) // Let persister enqueue

	// Verify message was persisted
	if len(mockPersister.Messages) == 0 {
		t.Error("No messages were persisted")
	}

	found := false
	for _, msg := range mockPersister.Messages {
		if msg.Type == models.MessageTypeChat && msg.Content == "Test persistence" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Chat message was not persisted")
	}
}

func TestWebSocketBroadcastToMultipleClients(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	// Connect 3 clients to the same room
	clients := make([]*websocket.Conn, 3)
	for i := 0; i < 3; i++ {
		clients[i] = connectWebSocket(t, server.URL, "user"+string(rune('1'+i)), "User"+string(rune('1'+i)))
		defer clients[i].Close()

		sendMessage(t, clients[i], &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "broadcast_room"})
	}

	// Read all join notifications
	for i := 0; i < 3; i++ {
		for j := 0; j <= i; j++ {
			receiveMessage(t, clients[j], 1*time.Second)
		}
	}

	// First client sends a message
	sendMessage(t, clients[0], &models.Message{
		Type:    models.MessageTypeChat,
		RoomID:  "broadcast_room",
		Content: "Broadcasting to all",
	})

	// All 3 clients should receive the message
	for i := 0; i < 3; i++ {
		msg := receiveMessage(t, clients[i], 1*time.Second)
		if msg.Content != "Broadcasting to all" {
			t.Errorf("Client %d didn't receive broadcast", i)
		}
	}
}
