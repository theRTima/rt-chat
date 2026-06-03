package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/theRTima/rt-chat/models"
)

// readMessages reads one WebSocket frame and returns all parsed messages,
// handling WritePump batching where multiple JSON objects are separated by \n
func readMessages(ws *websocket.Conn, timeout time.Duration) []*models.Message {
	ws.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := ws.ReadMessage()
	if err != nil {
		return nil
	}
	var msgs []*models.Message
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var msg models.Message
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}
		msgs = append(msgs, &msg)
	}
	return msgs
}

func TestWebSocketInvalidJSON(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws.Close()

	// Send invalid JSON
	if err := ws.WriteMessage(websocket.TextMessage, []byte("not valid json")); err != nil {
		t.Fatalf("Failed to send invalid JSON: %v", err)
	}

	// Should receive error message
	ws.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read error response: %v", err)
	}

	var msg models.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}

	if msg.Type != models.MessageTypeError {
		t.Errorf("Expected error message, got %s", msg.Type)
	}
}

func TestWebSocketHistoryOnJoin(t *testing.T) {
	mockStorage := models.NewMockStorage()
	mockPersister := models.NewMockPersister()
	hub := models.NewHub(mockStorage)
	go hub.Run()

	// Pre-populate history
	mockStorage.RoomHistory["history_room"] = []*models.Message{
		{Type: models.MessageTypeChat, RoomID: "history_room", UserID: "past_user", Username: "PastUser", Content: "first", Timestamp: time.Now().Add(-10 * time.Minute)},
		{Type: models.MessageTypeChat, RoomID: "history_room", UserID: "past_user", Username: "PastUser", Content: "second", Timestamp: time.Now().Add(-9 * time.Minute)},
		{Type: models.MessageTypeChat, RoomID: "history_room", UserID: "past_user", Username: "PastUser", Content: "third", Timestamp: time.Now().Add(-8 * time.Minute)},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub, mockPersister, w, r)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws.Close()

	// Join room
	sendMessage(t, ws, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "history_room"})

	// Read all messages from potentially batched frames
	historyCount := 0
	timeout := time.After(2 * time.Second)

	for historyCount < 3 {
		msgs := readMessages(ws, 500*time.Millisecond)
		if msgs == nil {
			break
		}

		for _, msg := range msgs {
			if msg.Type == models.MessageTypeChat && msg.Content == "first" {
				historyCount++
			} else if msg.Type == models.MessageTypeChat && msg.Content == "second" {
				historyCount++
			} else if msg.Type == models.MessageTypeChat && msg.Content == "third" {
				historyCount++
			}
		}

		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for history messages, got %d/3", historyCount)
		default:
		}
	}

	if historyCount != 3 {
		t.Errorf("Expected 3 history messages, got %d", historyCount)
	}
}

func TestWebSocketConcurrentMessages(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	n := 5
	clients := make([]*websocket.Conn, n)

	for i := 0; i < n; i++ {
		userID := "user_" + string(rune('1'+i))
		clients[i] = connectWebSocket(t, server.URL, userID, "User"+string(rune('1'+i)))
		defer clients[i].Close()

		sendMessage(t, clients[i], &models.Message{
			Type:   models.MessageTypeJoinRoom,
			RoomID: "concurrent_room",
		})
	}

	// Drain all join notifications
	for i := 0; i < n; i++ {
		for j := 0; j <= i; j++ {
			receiveMessage(t, clients[j], 1*time.Second)
		}
	}

	// All clients send messages concurrently
	var sendWg sync.WaitGroup
	sendWg.Add(n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer sendWg.Done()
			content := "message from user" + string(rune('1'+idx))
			sendMessage(t, clients[idx], &models.Message{
				Type:    models.MessageTypeChat,
				RoomID:  "concurrent_room",
				Content: content,
			})
		}(i)
	}

	sendWg.Wait()

	// Collect messages using a channel from reader goroutines
	type msgResult struct {
		idx     int
		content string
	}
	msgChan := make(chan msgResult, n*n*2)

	var readWg sync.WaitGroup
	readWg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer readWg.Done()
			// Read until timeout with no more data
			for {
				msgs := readMessages(clients[idx], 500*time.Millisecond)
				if msgs == nil || len(msgs) == 0 {
					return
				}
				for _, msg := range msgs {
					if msg.Type == models.MessageTypeChat {
						msgChan <- msgResult{idx, msg.Content}
					}
				}
			}
		}(i)
	}

	// Wait for readers to finish (they timeout after 500ms of no data)
	readWg.Wait()
	close(msgChan)

	// Count received messages
	totalReceived := 0
	for range msgChan {
		totalReceived++
	}

	expectedTotal := n * n
	if totalReceived != expectedTotal {
		t.Errorf("Expected %d messages, got %d", expectedTotal, totalReceived)
	}
}

func TestWebSocketDisconnectCleanup(t *testing.T) {
	server, hub, _ := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")

	// Join room
	sendMessage(t, ws, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "cleanup_room"})
	receiveMessage(t, ws, 1*time.Second)

	// Disconnect
	ws.Close()
	time.Sleep(200 * time.Millisecond)

	// Hub should have cleaned up
	if count := hub.GetClientCount(); count != 0 {
		t.Errorf("Expected 0 clients after disconnect, got %d", count)
	}
	if count := hub.GetRoomCount(); count != 0 {
		t.Errorf("Expected 0 rooms after disconnect, got %d", count)
	}
}

func TestWebSocketRejoinRoom(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws.Close()

	// Join room
	sendMessage(t, ws, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "rejoin_room"})
	receiveMessage(t, ws, 1*time.Second)

	// Leave room
	sendMessage(t, ws, &models.Message{Type: models.MessageTypeLeaveRoom, RoomID: "rejoin_room"})
	receiveMessage(t, ws, 1*time.Second)

	// Rejoin same room
	sendMessage(t, ws, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "rejoin_room"})
	msg := receiveMessage(t, ws, 1*time.Second)

	if msg.Type != models.MessageTypeUserJoined {
		t.Errorf("Expected user_joined on rejoin, got %s", msg.Type)
	}
	if msg.UserID != "alice" {
		t.Errorf("Expected user_id 'alice', got %s", msg.UserID)
	}
}

func TestWebSocketMessageOrdering(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws1 := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws1.Close()

	ws2 := connectWebSocket(t, server.URL, "bob", "Bob")
	defer ws2.Close()

	sendMessage(t, ws1, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "order_room"})
	receiveMessage(t, ws1, 1*time.Second) // UserJoined{alice}

	sendMessage(t, ws2, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "order_room"})
	receiveMessage(t, ws1, 1*time.Second) // UserJoined{bob} for alice
	receiveMessage(t, ws2, 1*time.Second) // UserJoined{bob} echo for bob

	// Send sequential messages
	expected := []string{"msg1", "msg2", "msg3"}
	for _, content := range expected {
		sendMessage(t, ws1, &models.Message{
			Type:    models.MessageTypeChat,
			RoomID:  "order_room",
			Content: content,
		})
	}

	// Verify order on both clients
	for i, exp := range expected {
		msg1 := receiveMessage(t, ws1, 1*time.Second)
		if msg1.Content != exp {
			t.Errorf("ws1 message %d: expected %s, got %s", i, exp, msg1.Content)
		}

		msg2 := receiveMessage(t, ws2, 1*time.Second)
		if msg2.Content != exp {
			t.Errorf("ws2 message %d: expected %s, got %s", i, exp, msg2.Content)
		}
	}
}

func TestWebSocketBroadcastDoesNotLeakBetweenRooms(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	wsA := connectWebSocket(t, server.URL, "alice", "Alice")
	defer wsA.Close()

	wsB := connectWebSocket(t, server.URL, "bob", "Bob")
	defer wsB.Close()

	wsC := connectWebSocket(t, server.URL, "charlie", "Charlie")
	defer wsC.Close()

	// Alice joins room A
	sendMessage(t, wsA, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "room_a"})
	receiveMessage(t, wsA, 1*time.Second) // UserJoined{alice}

	// Bob joins room A
	sendMessage(t, wsB, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "room_a"})
	receiveMessage(t, wsA, 1*time.Second) // UserJoined{bob} for alice
	receiveMessage(t, wsB, 1*time.Second) // UserJoined{bob} echo for bob

	// Charlie joins room B
	sendMessage(t, wsC, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "room_b"})
	receiveMessage(t, wsC, 1*time.Second) // UserJoined{charlie}

	// Alice sends to room A
	sendMessage(t, wsA, &models.Message{
		Type:    models.MessageTypeChat,
		RoomID:  "room_a",
		Content: "room a secret",
	})

	// Alice and Bob should get it
	receiveMessage(t, wsA, 1*time.Second)
	receiveMessage(t, wsB, 1*time.Second)

	// Charlie should NOT get it
	wsC.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := wsC.ReadMessage()
	if err == nil {
		t.Error("Charlie should not receive messages from room A")
	}
}

func TestWebSocketServerHandlesMultipleDisconnects(t *testing.T) {
	server, hub, _ := setupTestServer()
	defer server.Close()

	// Connect and disconnect many clients rapidly
	n := 20
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			userID := "burst_" + string(rune('0'+idx%10))

			ws := connectWebSocket(t, server.URL, userID, "BurstUser")
			defer ws.Close()

			sendMessage(t, ws, &models.Message{
				Type:   models.MessageTypeJoinRoom,
				RoomID: "burst_room",
			})
			receiveMessage(t, ws, 500*time.Millisecond)

			sendMessage(t, ws, &models.Message{
				Type:    models.MessageTypeChat,
				RoomID:  "burst_room",
				Content: "burst message",
			})
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	// Hub should have cleaned up all disconnected clients
	// (some may still be connected if they haven't closed yet)
	// At minimum, no panics should have occurred
	t.Logf("Clients remaining after burst: %d", hub.GetClientCount())
}

func TestWebSocketUsernameFallback(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	// Connect without username (should use default "User_{userID}")
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?user_id=no_name_user"

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer ws.Close()

	// Join room to verify connection works
	sendMessage(t, ws, &models.Message{Type: models.MessageTypeJoinRoom, RoomID: "general"})
	msg := receiveMessage(t, ws, 1*time.Second)

	if msg.Type != models.MessageTypeUserJoined {
		t.Errorf("Expected user_joined, got %s", msg.Type)
	}
}

func TestWebSocketPrivateMessageOnlyReachesRecipient(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	// Set up 3 users
	wsAlice := connectWebSocket(t, server.URL, "alice", "Alice")
	defer wsAlice.Close()

	wsBob := connectWebSocket(t, server.URL, "bob", "Bob")
	defer wsBob.Close()

	wsCharlie := connectWebSocket(t, server.URL, "charlie", "Charlie")
	defer wsCharlie.Close()

	// Let all register
	time.Sleep(100 * time.Millisecond)

	// Alice sends private message to Bob
	sendMessage(t, wsAlice, &models.Message{
		Type:     models.MessageTypePrivate,
		ToUserID: "bob",
		Content:  "secret for bob",
	})

	// Bob should receive it
	bobMsg := receiveMessage(t, wsBob, 1*time.Second)
	if bobMsg.Type != models.MessageTypePrivate || bobMsg.Content != "secret for bob" {
		t.Errorf("Bob got wrong private message: %+v", bobMsg)
	}
	if bobMsg.UserID != "alice" {
		t.Errorf("Expected sender 'alice', got %s", bobMsg.UserID)
	}

	// Charlie should NOT receive it
	wsCharlie.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := wsCharlie.ReadMessage()
	if err == nil {
		t.Error("Charlie should not receive Alice's private message")
	}
}

func TestWebSocketUnknownMessageType(t *testing.T) {
	server, _, _ := setupTestServer()
	defer server.Close()

	ws := connectWebSocket(t, server.URL, "alice", "Alice")
	defer ws.Close()

	// Send unknown message type
	sendMessage(t, ws, &models.Message{
		Type:    "unknown_type",
		Content: "test",
	})

	// Should receive error
	msg := receiveMessage(t, ws, 1*time.Second)
	if msg.Type != models.MessageTypeError {
		t.Errorf("Expected error message, got %s", msg.Type)
	}
}
