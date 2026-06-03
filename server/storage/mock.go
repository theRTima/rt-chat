package storage

import (
	"context"
	"time"

	"github.com/theRTima/rt-chat/models"
)

// MockStorage is a mock implementation of Storage interface for testing
type MockStorage struct {
	Users        map[string]*models.User
	Rooms        map[string]string
	Messages     []*models.Message
	RoomMembers  map[string][]string
	RoomHistory  map[string][]*models.Message
	UpsertUserFn func(ctx context.Context, userID, username string) error
}

// NewMockStorage creates a new MockStorage
func NewMockStorage() *MockStorage {
	return &MockStorage{
		Users:       make(map[string]*models.User),
		Rooms:       make(map[string]string),
		Messages:    make([]*models.Message, 0),
		RoomMembers: make(map[string][]string),
		RoomHistory: make(map[string][]*models.Message),
	}
}

func (m *MockStorage) UpsertUser(ctx context.Context, userID, username string) error {
	if m.UpsertUserFn != nil {
		return m.UpsertUserFn(ctx, userID, username)
	}
	m.Users[userID] = &models.User{
		UserID:    userID,
		Username:  username,
		CreatedAt: time.Now(),
		LastSeen:  time.Now(),
	}
	return nil
}

func (m *MockStorage) UpsertRoom(ctx context.Context, roomID, name string) error {
	m.Rooms[roomID] = name
	return nil
}

func (m *MockStorage) GetRoomHistory(ctx context.Context, roomID string, limit int) ([]*models.Message, error) {
	history, exists := m.RoomHistory[roomID]
	if !exists {
		return []*models.Message{}, nil
	}

	// Return last 'limit' messages
	start := 0
	if len(history) > limit {
		start = len(history) - limit
	}
	return history[start:], nil
}

func (m *MockStorage) AddRoomMember(ctx context.Context, roomID, userID string) error {
	m.RoomMembers[roomID] = append(m.RoomMembers[roomID], userID)
	return nil
}

func (m *MockStorage) RemoveRoomMember(ctx context.Context, roomID, userID string) error {
	members := m.RoomMembers[roomID]
	for i, member := range members {
		if member == userID {
			m.RoomMembers[roomID] = append(members[:i], members[i+1:]...)
			break
		}
	}
	return nil
}

// MockPersister is a mock implementation of MessagePersister for testing
type MockPersister struct {
	Messages []*models.Message
}

func NewMockPersister() *MockPersister {
	return &MockPersister{
		Messages: make([]*models.Message, 0),
	}
}

func (m *MockPersister) Enqueue(msg *models.Message) {
	m.Messages = append(m.Messages, msg)
}
