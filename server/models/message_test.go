package models

import (
	"testing"
	"time"
)

func TestNewChatMessage(t *testing.T) {
	msg := NewChatMessage("general", "user1", "Alice", "Hello world")

	if msg.Type != MessageTypeChat {
		t.Errorf("Expected type %s, got %s", MessageTypeChat, msg.Type)
	}
	if msg.RoomID != "general" {
		t.Errorf("Expected room_id 'general', got %s", msg.RoomID)
	}
	if msg.UserID != "user1" {
		t.Errorf("Expected user_id 'user1', got %s", msg.UserID)
	}
	if msg.Content != "Hello world" {
		t.Errorf("Expected content 'Hello world', got %s", msg.Content)
	}
	if msg.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
}

func TestNewPrivateMessage(t *testing.T) {
	msg := NewPrivateMessage("alice", "Alice", "bob", "Private msg")

	if msg.Type != MessageTypePrivate {
		t.Errorf("Expected type %s, got %s", MessageTypePrivate, msg.Type)
	}
	if msg.UserID != "alice" {
		t.Errorf("Expected user_id 'alice', got %s", msg.UserID)
	}
	if msg.ToUserID != "bob" {
		t.Errorf("Expected to_user_id 'bob', got %s", msg.ToUserID)
	}
	if msg.Content != "Private msg" {
		t.Errorf("Expected content 'Private msg', got %s", msg.Content)
	}
}

func TestNewErrorMessage(t *testing.T) {
	msg := NewErrorMessage("Something went wrong")

	if msg.Type != MessageTypeError {
		t.Errorf("Expected type %s, got %s", MessageTypeError, msg.Type)
	}
	if msg.Error != "Something went wrong" {
		t.Errorf("Expected error 'Something went wrong', got %s", msg.Error)
	}
}

func TestNewUserJoinedMessage(t *testing.T) {
	msg := NewUserJoinedMessage("general", "user1", "Alice")

	if msg.Type != MessageTypeUserJoined {
		t.Errorf("Expected type %s, got %s", MessageTypeUserJoined, msg.Type)
	}
	if msg.RoomID != "general" {
		t.Errorf("Expected room_id 'general', got %s", msg.RoomID)
	}
	if msg.UserID != "user1" {
		t.Errorf("Expected user_id 'user1', got %s", msg.UserID)
	}
}

func TestNewUserLeftMessage(t *testing.T) {
	msg := NewUserLeftMessage("general", "user1", "Alice")

	if msg.Type != MessageTypeUserLeft {
		t.Errorf("Expected type %s, got %s", MessageTypeUserLeft, msg.Type)
	}
	if msg.RoomID != "general" {
		t.Errorf("Expected room_id 'general', got %s", msg.RoomID)
	}
}

func TestClientAddRoom(t *testing.T) {
	client := &Client{
		UserID:   "user1",
		Username: "User1",
		Send:     make(chan []byte, 256),
	}

	client.AddRoom("general")

	if !client.IsInRoom("general") {
		t.Error("Client should be in room 'general'")
	}
}

func TestClientRemoveRoom(t *testing.T) {
	client := &Client{
		UserID:   "user1",
		Username: "User1",
		Send:     make(chan []byte, 256),
	}

	client.AddRoom("general")
	client.RemoveRoom("general")

	if client.IsInRoom("general") {
		t.Error("Client should not be in room 'general'")
	}
}

func TestClientSendMessage(t *testing.T) {
	client := &Client{
		UserID:   "user1",
		Username: "User1",
		Send:     make(chan []byte, 256),
	}

	msg := NewChatMessage("general", "user1", "User1", "Test")
	client.SendMessage(msg)

	select {
	case <-client.Send:
		// Message received successfully
	case <-time.After(100 * time.Millisecond):
		t.Error("Message was not sent to client channel")
	}
}
