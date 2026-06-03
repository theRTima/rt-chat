package models

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// inline mock implementations to avoid import cycle with storage package

type mockStorage struct {
	mu          sync.Mutex
	users       map[string]*User
	rooms       map[string]string
	roomHistory map[string][]*Message
	roomMembers map[string][]string
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		users:       make(map[string]*User),
		rooms:       make(map[string]string),
		roomHistory: make(map[string][]*Message),
		roomMembers: make(map[string][]string),
	}
}

func (m *mockStorage) UpsertUser(_ context.Context, userID, username string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.users[userID] = &User{UserID: userID, Username: username}
	return nil
}

func (m *mockStorage) UpsertRoom(_ context.Context, roomID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rooms[roomID] = name
	return nil
}

func (m *mockStorage) GetRoomHistory(_ context.Context, roomID string, limit int) ([]*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	history := m.roomHistory[roomID]
	if len(history) > limit {
		history = history[len(history)-limit:]
	}
	return history, nil
}

func (m *mockStorage) AddRoomMember(_ context.Context, roomID, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.roomMembers[roomID] = append(m.roomMembers[roomID], userID)
	return nil
}

func (m *mockStorage) RemoveRoomMember(_ context.Context, roomID, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	members := m.roomMembers[roomID]
	for i, id := range members {
		if id == userID {
			m.roomMembers[roomID] = append(members[:i], members[i+1:]...)
			break
		}
	}
	return nil
}

type mockPersister struct {
	messages []*Message
}

func newMockPersister() *mockPersister {
	return &mockPersister{}
}

func (m *mockPersister) Enqueue(msg *Message) {
	m.messages = append(m.messages, msg)
}

func TestHubDuplicateRegister(t *testing.T) {
	stg := newMockStorage()
	hub := NewHub(stg)
	go hub.Run()

	client := &Client{
		Hub:      hub,
		Send:     make(chan []byte, 256),
		UserID:   "dup_user",
		Username: "DupUser",
		rooms:    make(map[string]bool),
	}

	hub.Register <- client
	hub.Register <- client
	time.Sleep(50 * time.Millisecond)

	if count := hub.GetClientCount(); count != 1 {
		t.Errorf("Expected 1 client, got %d", count)
	}
}

func TestHubUnknownMessageType(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	client := &Client{
		Hub:       hub,
		Send:      make(chan []byte, 256),
		UserID:    "user1",
		Username:  "User1",
		rooms:     make(map[string]bool),
		Persister: p,
	}

	hub.Register <- client
	time.Sleep(50 * time.Millisecond)

	hub.Message <- &ClientMessage{
		Client:  client,
		Message: &Message{Type: "nonexistent_type", Content: "test"},
	}

	select {
	case msg := <-client.Send:
		var parsed Message
		if err := json.Unmarshal(msg, &parsed); err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		if parsed.Type != MessageTypeError {
			t.Errorf("Expected error, got %s", parsed.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("No error for unknown type")
	}
}

func TestHubBroadcastSenderIncluded(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	c1 := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "sender", Username: "Sender", rooms: make(map[string]bool), Persister: p}
	c2 := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "receiver", Username: "Receiver", rooms: make(map[string]bool), Persister: p}

	hub.Register <- c1
	hub.Register <- c2
	time.Sleep(50 * time.Millisecond)

	hub.Message <- &ClientMessage{Client: c1, Message: &Message{Type: MessageTypeJoinRoom, RoomID: "room"}}
	hub.Message <- &ClientMessage{Client: c2, Message: &Message{Type: MessageTypeJoinRoom, RoomID: "room"}}
	time.Sleep(100 * time.Millisecond)

	// Drain join notifications
	for len(c1.Send) > 0 {
		<-c1.Send
	}
	for len(c2.Send) > 0 {
		<-c2.Send
	}

	hub.Message <- &ClientMessage{
		Client:  c1,
		Message: &Message{Type: MessageTypeChat, RoomID: "room", Content: "hello"},
	}
	time.Sleep(50 * time.Millisecond)

	// Both (including sender) should receive
	select {
	case <-c2.Send:
	default:
		t.Error("Receiver did not get message")
	}
	select {
	case <-c1.Send:
	default:
		t.Error("Sender did not receive own message")
	}
}

func TestHubConcurrentClients(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	n := 10
	clients := make([]*Client, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			c := &Client{
				Hub: hub, Send: make(chan []byte, 256),
				UserID: "u" + string(rune('a'+idx)), Username: "U",
				rooms: make(map[string]bool), Persister: p,
			}
			hub.Register <- c
			clients[idx] = c
		}(i)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	if count := hub.GetClientCount(); count != n {
		t.Fatalf("Expected %d clients, got %d", n, count)
	}

	var joinWg sync.WaitGroup
	joinWg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer joinWg.Done()
			hub.Message <- &ClientMessage{Client: clients[idx], Message: &Message{Type: MessageTypeJoinRoom, RoomID: "room"}}
		}(i)
	}
	joinWg.Wait()
	time.Sleep(200 * time.Millisecond)

	// Drain all join notifications from all clients
	for i := 0; i < n; i++ {
		for len(clients[i].Send) > 0 {
			<-clients[i].Send
		}
	}

	hub.Message <- &ClientMessage{
		Client:  clients[0],
		Message: &Message{Type: MessageTypeChat, RoomID: "room", Content: "concurrent"},
	}
	time.Sleep(100 * time.Millisecond)

	received := 0
	for i := 0; i < n; i++ {
		select {
		case msg := <-clients[i].Send:
			var parsed Message
			if json.Unmarshal(msg, &parsed) == nil && parsed.Type == MessageTypeChat {
				received++
			}
		default:
		}
	}
	if received != n {
		t.Errorf("Expected %d to receive, got %d", n, received)
	}
}

func TestHubRoomIsolation(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	cA := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "A", Username: "A", rooms: make(map[string]bool), Persister: p}
	cB := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "B", Username: "B", rooms: make(map[string]bool), Persister: p}

	hub.Register <- cA
	hub.Register <- cB
	time.Sleep(50 * time.Millisecond)

	hub.Message <- &ClientMessage{Client: cA, Message: &Message{Type: MessageTypeJoinRoom, RoomID: "room_a"}}
	hub.Message <- &ClientMessage{Client: cB, Message: &Message{Type: MessageTypeJoinRoom, RoomID: "room_b"}}
	time.Sleep(100 * time.Millisecond)

	for len(cA.Send) > 0 {
		<-cA.Send
	}
	for len(cB.Send) > 0 {
		<-cB.Send
	}

	hub.Message <- &ClientMessage{Client: cA, Message: &Message{Type: MessageTypeChat, RoomID: "room_a", Content: "only A"}}
	time.Sleep(50 * time.Millisecond)

	select {
	case <-cA.Send:
	default:
		t.Error("A did not receive")
	}
	select {
	case <-cB.Send:
		t.Error("B received from different room")
	default:
	}
}

func TestHubEmptyRoomID(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	c := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "u1", Username: "U1", rooms: make(map[string]bool), Persister: p}
	hub.Register <- c
	time.Sleep(50 * time.Millisecond)

	hub.Message <- &ClientMessage{Client: c, Message: &Message{Type: MessageTypeJoinRoom, RoomID: ""}}

	select {
	case msg := <-c.Send:
		var parsed Message
		if json.Unmarshal(msg, &parsed) == nil && parsed.Type != MessageTypeError {
			t.Errorf("Expected error, got %s", parsed.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("No error for empty room ID")
	}
}

func TestHubJoinSameRoomTwice(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	c := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "u1", Username: "U1", rooms: make(map[string]bool), Persister: p}
	hub.Register <- c
	time.Sleep(50 * time.Millisecond)

	joinMsg := &Message{Type: MessageTypeJoinRoom, RoomID: "same"}
	hub.Message <- &ClientMessage{Client: c, Message: joinMsg}
	hub.Message <- &ClientMessage{Client: c, Message: joinMsg}
	time.Sleep(100 * time.Millisecond)

	if !c.IsInRoom("same") {
		t.Error("Client should be in room")
	}
	if hub.rooms["same"].GetClientCount() != 1 {
		t.Errorf("Expected 1, got %d", hub.rooms["same"].GetClientCount())
	}
}

func TestHubHistoryOnJoin(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()

	stg.roomHistory["hist_room"] = []*Message{
		{Type: MessageTypeChat, RoomID: "hist_room", UserID: "past", Content: "old1", Timestamp: time.Now().Add(-10 * time.Minute)},
		{Type: MessageTypeChat, RoomID: "hist_room", UserID: "past", Content: "old2", Timestamp: time.Now().Add(-9 * time.Minute)},
	}

	hub := NewHub(stg)
	go hub.Run()

	c := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "new", Username: "New", rooms: make(map[string]bool), Persister: p}
	hub.Register <- c
	time.Sleep(50 * time.Millisecond)

	hub.Message <- &ClientMessage{Client: c, Message: &Message{Type: MessageTypeJoinRoom, RoomID: "hist_room"}}
	time.Sleep(200 * time.Millisecond)

	// Drain send channel and count history messages
	historyCount := 0
	done := false
	for !done {
		select {
		case msg := <-c.Send:
			var parsed Message
			if json.Unmarshal(msg, &parsed) == nil && parsed.Type == MessageTypeChat {
				historyCount++
			}
		case <-time.After(50 * time.Millisecond):
			done = true
		}
	}

	if historyCount < 2 {
		t.Errorf("Expected at least 2 history messages, got %d", historyCount)
	}
}

func TestHubUnregisterWithRooms(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	c := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "u", Username: "U", rooms: make(map[string]bool), Persister: p}
	hub.Register <- c
	time.Sleep(50 * time.Millisecond)

	for _, room := range []string{"r1", "r2", "r3"} {
		hub.Message <- &ClientMessage{Client: c, Message: &Message{Type: MessageTypeJoinRoom, RoomID: room}}
	}
	time.Sleep(100 * time.Millisecond)

	if count := hub.GetRoomCount(); count != 3 {
		t.Fatalf("Expected 3 rooms, got %d", count)
	}

	hub.Unregister <- c
	time.Sleep(100 * time.Millisecond)

	if count := hub.GetClientCount(); count != 0 {
		t.Errorf("Expected 0 clients, got %d", count)
	}
}

func TestHubPrivateMessageToOfflineUser(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	c := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "online", Username: "Online", rooms: make(map[string]bool), Persister: p}
	hub.Register <- c
	time.Sleep(50 * time.Millisecond)

	hub.Message <- &ClientMessage{
		Client:  c,
		Message: &Message{Type: MessageTypePrivate, ToUserID: "nonexistent", Content: "hi"},
	}

	select {
	case msg := <-c.Send:
		var parsed Message
		if json.Unmarshal(msg, &parsed) == nil && parsed.Type != MessageTypeError {
			t.Errorf("Expected error, got %s", parsed.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("No error for offline recipient")
	}
}

func TestHubChatMessageFillsSenderInfo(t *testing.T) {
	stg := newMockStorage()
	p := newMockPersister()
	hub := NewHub(stg)
	go hub.Run()

	c := &Client{Hub: hub, Send: make(chan []byte, 256), UserID: "sender", Username: "Sender", rooms: make(map[string]bool), Persister: p}
	hub.Register <- c
	time.Sleep(50 * time.Millisecond)

	hub.Message <- &ClientMessage{Client: c, Message: &Message{Type: MessageTypeJoinRoom, RoomID: "room"}}
	time.Sleep(50 * time.Millisecond)
	for len(c.Send) > 0 {
		<-c.Send
	}

	hub.Message <- &ClientMessage{
		Client:  c,
		Message: &Message{Type: MessageTypeChat, RoomID: "room", Content: "who sent this?"},
	}
	time.Sleep(50 * time.Millisecond)

	select {
	case msg := <-c.Send:
		var parsed Message
		if err := json.Unmarshal(msg, &parsed); err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		if parsed.UserID != "sender" {
			t.Errorf("Expected UserID 'sender', got %s", parsed.UserID)
		}
		if parsed.Username != "Sender" {
			t.Errorf("Expected Username 'Sender', got %s", parsed.Username)
		}
		if parsed.Timestamp.IsZero() {
			t.Error("Timestamp should be set")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Message not received")
	}
}
