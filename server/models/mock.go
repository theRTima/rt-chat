package models

import (
	"context"
	"sync"
	"time"
)

// MockStorage is a mock implementation of Storage for testing
type MockStorage struct {
	mu           sync.Mutex
	Users        map[string]*User
	Rooms        map[string]string
	Messages     []*Message
	RoomMembers  map[string][]string
	RoomHistory  map[string][]*Message
	UpsertUserFn func(ctx context.Context, userID, username string) error
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		Users:       make(map[string]*User),
		Rooms:       make(map[string]string),
		Messages:    make([]*Message, 0),
		RoomMembers: make(map[string][]string),
		RoomHistory: make(map[string][]*Message),
	}
}

func (m *MockStorage) UpsertUser(ctx context.Context, userID, username string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.UpsertUserFn != nil {
		return m.UpsertUserFn(ctx, userID, username)
	}
	m.Users[userID] = &User{
		UserID:    userID,
		Username:  username,
		CreatedAt: time.Now(),
		LastSeen:  time.Now(),
	}
	return nil
}

func (m *MockStorage) UpsertRoom(ctx context.Context, roomID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Rooms[roomID] = name
	return nil
}

func (m *MockStorage) GetRoomHistory(ctx context.Context, roomID string, limit int) ([]*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	history, exists := m.RoomHistory[roomID]
	if !exists {
		return []*Message{}, nil
	}
	start := 0
	if len(history) > limit {
		start = len(history) - limit
	}
	return history[start:], nil
}

func (m *MockStorage) AddRoomMember(ctx context.Context, roomID, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RoomMembers[roomID] = append(m.RoomMembers[roomID], userID)
	return nil
}

func (m *MockStorage) RemoveRoomMember(ctx context.Context, roomID, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	members := m.RoomMembers[roomID]
	for i, member := range members {
		if member == userID {
			m.RoomMembers[roomID] = append(members[:i], members[i+1:]...)
			break
		}
	}
	return nil
}

func (m *MockStorage) GetPrivateMessageHistory(ctx context.Context, userID1, userID2 string, limit int) ([]*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*Message
	for _, msg := range m.Messages {
		if msg.Type == MessageTypePrivate &&
			((msg.UserID == userID1 && msg.ToUserID == userID2) ||
				(msg.UserID == userID2 && msg.ToUserID == userID1)) {
			result = append(result, msg)
		}
	}
	if len(result) > limit {
		result = result[len(result)-limit:]
	}
	return result, nil
}

// MockPersister is a mock implementation of MessagePersister for testing
type MockPersister struct {
	Messages []*Message
}

func NewMockPersister() *MockPersister {
	return &MockPersister{
		Messages: make([]*Message, 0),
	}
}

func (m *MockPersister) Enqueue(msg *Message) {
	m.Messages = append(m.Messages, msg)
}
