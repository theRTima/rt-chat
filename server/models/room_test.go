package models

import (
	"testing"
)

func TestRoomAddClient(t *testing.T) {
	room := NewRoom("test_room")

	client := &Client{
		UserID:   "user1",
		Username: "User1",
		Send:     make(chan []byte, 256),
	}

	room.AddClient(client)

	if room.GetClientCount() != 1 {
		t.Errorf("Expected 1 client, got %d", room.GetClientCount())
	}
}

func TestRoomRemoveClient(t *testing.T) {
	room := NewRoom("test_room")

	client := &Client{
		UserID:   "user1",
		Username: "User1",
		Send:     make(chan []byte, 256),
	}

	room.AddClient(client)
	room.RemoveClient(client)

	if room.GetClientCount() != 0 {
		t.Errorf("Expected 0 clients, got %d", room.GetClientCount())
	}
}

func TestRoomBroadcast(t *testing.T) {
	room := NewRoom("test_room")

	clients := make([]*Client, 3)
	for i := 0; i < 3; i++ {
		clients[i] = &Client{
			UserID:   "user" + string(rune('1'+i)),
			Username: "User" + string(rune('1'+i)),
			Send:     make(chan []byte, 256),
		}
		room.AddClient(clients[i])
	}

	message := []byte("test message")
	room.Broadcast(message, nil)

	// All clients should receive the message
	for i := 0; i < 3; i++ {
		select {
		case msg := <-clients[i].Send:
			if string(msg) != string(message) {
				t.Errorf("Client %d received wrong message", i)
			}
		default:
			t.Errorf("Client %d did not receive message", i)
		}
	}
}

func TestRoomBroadcastExclude(t *testing.T) {
	room := NewRoom("test_room")

	client1 := &Client{
		UserID:   "user1",
		Username: "User1",
		Send:     make(chan []byte, 256),
	}

	client2 := &Client{
		UserID:   "user2",
		Username: "User2",
		Send:     make(chan []byte, 256),
	}

	room.AddClient(client1)
	room.AddClient(client2)

	message := []byte("test message")
	room.Broadcast(message, client1) // Exclude client1

	// Client1 should NOT receive the message
	select {
	case <-client1.Send:
		t.Error("Client1 should not receive message (excluded)")
	default:
		// Expected - no message
	}

	// Client2 should receive the message
	select {
	case msg := <-client2.Send:
		if string(msg) != string(message) {
			t.Error("Client2 received wrong message")
		}
	default:
		t.Error("Client2 did not receive message")
	}
}

func TestRoomIsEmpty(t *testing.T) {
	room := NewRoom("test_room")

	if !room.IsEmpty() {
		t.Error("New room should be empty")
	}

	client := &Client{
		UserID:   "user1",
		Username: "User1",
		Send:     make(chan []byte, 256),
	}

	room.AddClient(client)

	if room.IsEmpty() {
		t.Error("Room should not be empty after adding client")
	}

	room.RemoveClient(client)

	if !room.IsEmpty() {
		t.Error("Room should be empty after removing client")
	}
}
