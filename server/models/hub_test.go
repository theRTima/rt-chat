package models

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestHubRegisterClient(t *testing.T) {
	mockStorage := NewMockStorage()
	hub := NewHub(mockStorage)

	// Start hub in goroutine
	go hub.Run()
	defer func() {
		// Clean shutdown would require stopping hub.Run()
		// For test purposes, we'll just let it end with test
	}()

	// Create a test client
	client := &Client{
		Hub:      hub,
		Send:     make(chan []byte, 256),
		UserID:   "test_user_1",
		Username: "TestUser1",
		rooms:    make(map[string]bool),
	}

	// Register client
	hub.Register <- client

	// Give hub time to process
	time.Sleep(50 * time.Millisecond)

	// Verify client is registered
	count := hub.GetClientCount()
	if count != 1 {
		t.Errorf("Expected 1 client, got %d", count)
	}

	// Verify user was saved to storage
	if _, exists := mockStorage.Users["test_user_1"]; !exists {
		t.Error("User was not saved to storage")
	}
}

func TestHubUnregisterClient(t *testing.T) {
	mockStorage := NewMockStorage()
	hub := NewHub(mockStorage)

	go hub.Run()
	client := &Client{
		Hub:      hub,
		Send:     make(chan []byte, 256),
		UserID:   "test_user_2",
		Username: "TestUser2",
		rooms:    make(map[string]bool),
	}

	// Register then unregister
	hub.Register <- client
	time.Sleep(50 * time.Millisecond)

	hub.Unregister <- client
	time.Sleep(50 * time.Millisecond)

	// Verify client is unregistered
	count := hub.GetClientCount()
	if count != 0 {
		t.Errorf("Expected 0 clients, got %d", count)
	}

	// Verify channel is closed
	select {
	case _, ok := <-client.Send:
		if ok {
			t.Error("Client send channel should be closed")
		}
	default:
		t.Error("Client send channel was not closed")
	}
}

func TestHubBroadcast(t *testing.T) {
	mockStorage := NewMockStorage()
	hub := NewHub(mockStorage)

	go hub.Run()

	// Create multiple clients
	clients := make([]*Client, 3)
	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		clients[i] = &Client{
			Hub:      hub,
			Send:     make(chan []byte, 256),
			UserID:   "user_" + string(rune('1'+i)),
			Username: "User" + string(rune('1'+i)),
			rooms:    make(map[string]bool),
		}
		hub.Register <- clients[i]
	}

	time.Sleep(50 * time.Millisecond)

	// Broadcast a message
	testMessage := []byte("test broadcast message")
	hub.Broadcast <- testMessage

	// Verify all clients received the message
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(idx int) {
			defer wg.Done()
			select {
			case msg := <-clients[idx].Send:
				if string(msg) != string(testMessage) {
					t.Errorf("Client %d received wrong message: %s", idx, string(msg))
				}
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Client %d did not receive message", idx)
			}
		}(i)
	}

	wg.Wait()
}

func TestHubJoinRoom(t *testing.T) {
	mockStorage := NewMockStorage()
	mockPersister := NewMockPersister()
	hub := NewHub(mockStorage)

	go hub.Run()


	client := &Client{
		Hub:       hub,
		Send:      make(chan []byte, 256),
		UserID:    "test_user",
		Username:  "TestUser",
		rooms:     make(map[string]bool),
		Persister: mockPersister,
	}

	hub.Register <- client
	time.Sleep(50 * time.Millisecond)

	// Send join room message
	joinMsg := &Message{
		Type:   MessageTypeJoinRoom,
		RoomID: "general",
	}

	hub.Message <- &ClientMessage{
		Client:  client,
		Message: joinMsg,
	}

	time.Sleep(100 * time.Millisecond)

	// Verify client is in room
	if !client.IsInRoom("general") {
		t.Error("Client is not in room")
	}

	// Verify room was created
	if hub.GetRoomCount() != 1 {
		t.Errorf("Expected 1 room, got %d", hub.GetRoomCount())
	}

	// Verify room was saved to storage
	if _, exists := mockStorage.Rooms["general"]; !exists {
		t.Error("Room was not saved to storage")
	}
}

func TestHubChatMessage(t *testing.T) {
	mockStorage := NewMockStorage()
	mockPersister := NewMockPersister()
	hub := NewHub(mockStorage)

	go hub.Run()


	// Create two clients in the same room
	client1 := &Client{
		Hub:       hub,
		Send:      make(chan []byte, 256),
		UserID:    "user1",
		Username:  "User1",
		rooms:     make(map[string]bool),
		Persister: mockPersister,
	}

	client2 := &Client{
		Hub:       hub,
		Send:      make(chan []byte, 256),
		UserID:    "user2",
		Username:  "User2",
		rooms:     make(map[string]bool),
		Persister: mockPersister,
	}

	hub.Register <- client1
	hub.Register <- client2
	time.Sleep(50 * time.Millisecond)

	// Both join the same room
	joinMsg1 := &Message{Type: MessageTypeJoinRoom, RoomID: "test_room"}
	joinMsg2 := &Message{Type: MessageTypeJoinRoom, RoomID: "test_room"}

	hub.Message <- &ClientMessage{Client: client1, Message: joinMsg1}
	hub.Message <- &ClientMessage{Client: client2, Message: joinMsg2}
	time.Sleep(100 * time.Millisecond)

	// Clear the join notifications from channels
	for len(client1.Send) > 0 {
		<-client1.Send
	}
	for len(client2.Send) > 0 {
		<-client2.Send
	}

	// Client1 sends a chat message
	chatMsg := &Message{
		Type:    MessageTypeChat,
		RoomID:  "test_room",
		Content: "Hello everyone!",
	}

	hub.Message <- &ClientMessage{Client: client1, Message: chatMsg}
	time.Sleep(50 * time.Millisecond)

	// Verify both clients received the message
	received := 0
	timeout := time.After(200 * time.Millisecond)

	for received < 2 {
		select {
		case msg := <-client1.Send:
			var parsedMsg Message
			if err := json.Unmarshal(msg, &parsedMsg); err == nil {
				if parsedMsg.Type == MessageTypeChat && parsedMsg.Content == "Hello everyone!" {
					received++
				}
			}
		case msg := <-client2.Send:
			var parsedMsg Message
			if err := json.Unmarshal(msg, &parsedMsg); err == nil {
				if parsedMsg.Type == MessageTypeChat && parsedMsg.Content == "Hello everyone!" {
					received++
				}
			}
		case <-timeout:
			t.Fatalf("Timeout: only %d clients received the message", received)
		}
	}

	// Verify message was persisted
	if len(mockPersister.Messages) == 0 {
		t.Error("Message was not persisted")
	}
}

func TestHubPrivateMessage(t *testing.T) {
	mockStorage := NewMockStorage()
	mockPersister := NewMockPersister()
	hub := NewHub(mockStorage)

	go hub.Run()


	client1 := &Client{
		Hub:       hub,
		Send:      make(chan []byte, 256),
		UserID:    "alice",
		Username:  "Alice",
		rooms:     make(map[string]bool),
		Persister: mockPersister,
	}

	client2 := &Client{
		Hub:       hub,
		Send:      make(chan []byte, 256),
		UserID:    "bob",
		Username:  "Bob",
		rooms:     make(map[string]bool),
		Persister: mockPersister,
	}

	hub.Register <- client1
	hub.Register <- client2
	time.Sleep(50 * time.Millisecond)

	// Alice sends private message to Bob
	privateMsg := &Message{
		Type:     MessageTypePrivate,
		ToUserID: "bob",
		Content:  "Hi Bob!",
	}

	hub.Message <- &ClientMessage{Client: client1, Message: privateMsg}
	time.Sleep(50 * time.Millisecond)

	// Verify only Bob received the message
	select {
	case msg := <-client2.Send:
		var parsedMsg Message
		if err := json.Unmarshal(msg, &parsedMsg); err != nil {
			t.Fatalf("Failed to parse message: %v", err)
		}
		if parsedMsg.Type != MessageTypePrivate || parsedMsg.Content != "Hi Bob!" {
			t.Errorf("Bob received wrong message: %+v", parsedMsg)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Bob did not receive private message")
	}

	// Verify Alice did not receive her own message
	select {
	case <-client1.Send:
		t.Error("Alice should not receive her own private message")
	case <-time.After(50 * time.Millisecond):
		// Expected - no message for Alice
	}
}

func TestHubLeaveRoom(t *testing.T) {
	mockStorage := NewMockStorage()
	mockPersister := NewMockPersister()
	hub := NewHub(mockStorage)

	go hub.Run()


	client := &Client{
		Hub:       hub,
		Send:      make(chan []byte, 256),
		UserID:    "test_user",
		Username:  "TestUser",
		rooms:     make(map[string]bool),
		Persister: mockPersister,
	}

	hub.Register <- client
	time.Sleep(50 * time.Millisecond)

	// Join room
	joinMsg := &Message{Type: MessageTypeJoinRoom, RoomID: "temp_room"}
	hub.Message <- &ClientMessage{Client: client, Message: joinMsg}
	time.Sleep(50 * time.Millisecond)

	// Leave room
	leaveMsg := &Message{Type: MessageTypeLeaveRoom, RoomID: "temp_room"}
	hub.Message <- &ClientMessage{Client: client, Message: leaveMsg}
	time.Sleep(50 * time.Millisecond)

	// Verify client is not in room
	if client.IsInRoom("temp_room") {
		t.Error("Client is still in room after leaving")
	}

	// Verify empty room was deleted
	if hub.GetRoomCount() != 0 {
		t.Errorf("Expected 0 rooms, got %d", hub.GetRoomCount())
	}
}
