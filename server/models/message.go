package models

import "time"

// MessageType определяет тип сообщения
type MessageType string

const (
	// MessageTypeJoinRoom - присоединение к комнате
	MessageTypeJoinRoom MessageType = "join_room"

	// MessageTypeLeaveRoom - выход из комнаты
	MessageTypeLeaveRoom MessageType = "leave_room"

	// MessageTypeChat - обычное сообщение в комнату
	MessageTypeChat MessageType = "chat"

	// MessageTypePrivate - приватное сообщение пользователю
	MessageTypePrivate MessageType = "private"

	// MessageTypeError - сообщение об ошибке
	MessageTypeError MessageType = "error"

	// MessageTypeUserJoined - уведомление о входе пользователя в комнату
	MessageTypeUserJoined MessageType = "user_joined"

	// MessageTypeUserLeft - уведомление о выходе пользователя из комнаты
	MessageTypeUserLeft MessageType = "user_left"

	// MessageTypeUserLookup - запрос поиска пользователя
	MessageTypeUserLookup MessageType = "user_lookup"

	// MessageTypeUserFound - ответ с найденным пользователем
	MessageTypeUserFound MessageType = "user_found"

	// MessageTypeUserNotFound - ответ: пользователь не найден
	MessageTypeUserNotFound MessageType = "user_not_found"
)

// Message представляет структуру WebSocket сообщения
type Message struct {
	// Тип сообщения
	Type MessageType `json:"type"`

	// ID комнаты (для join_room, leave_room, chat)
	RoomID string `json:"room_id,omitempty"`

	// ID отправителя
	UserID string `json:"user_id,omitempty"`

	// Имя пользователя для отображения
	Username string `json:"username,omitempty"`

	// ID получателя (для private сообщений)
	ToUserID string `json:"to_user_id,omitempty"`

	// Содержимое сообщения
	Content string `json:"content,omitempty"`

	// Временная метка
	Timestamp time.Time `json:"timestamp"`

	// Сообщение об ошибке (для type: error)
	Error string `json:"error,omitempty"`

	// Количество участников в комнате (для user_joined, user_left)
	ParticipantCount int `json:"participant_count,omitempty"`
}

// NewChatMessage создает новое сообщение в комнату
func NewChatMessage(roomID, userID, username, content string) *Message {
	return &Message{
		Type:      MessageTypeChat,
		RoomID:    roomID,
		UserID:    userID,
		Username:  username,
		Content:   content,
		Timestamp: time.Now(),
	}
}

// NewPrivateMessage создает новое приватное сообщение
func NewPrivateMessage(fromUserID, fromUsername, toUserID, content string) *Message {
	return &Message{
		Type:      MessageTypePrivate,
		UserID:    fromUserID,
		Username:  fromUsername,
		ToUserID:  toUserID,
		Content:   content,
		Timestamp: time.Now(),
	}
}

// NewErrorMessage создает сообщение об ошибке
func NewErrorMessage(errorText string) *Message {
	return &Message{
		Type:      MessageTypeError,
		Error:     errorText,
		Timestamp: time.Now(),
	}
}

// NewUserJoinedMessage создает уведомление о входе пользователя
func NewUserJoinedMessage(roomID, userID, username string) *Message {
	return &Message{
		Type:      MessageTypeUserJoined,
		RoomID:    roomID,
		UserID:    userID,
		Username:  username,
		Timestamp: time.Now(),
	}
}

// NewUserLeftMessage создает уведомление о выходе пользователя
func NewUserLeftMessage(roomID, userID, username string) *Message {
	return &Message{
		Type:      MessageTypeUserLeft,
		RoomID:    roomID,
		UserID:    userID,
		Username:  username,
		Timestamp: time.Now(),
	}
}

// NewUserFoundMessage создает ответ на успешный поиск пользователя
func NewUserFoundMessage(userID, username string) *Message {
	return &Message{
		Type:     MessageTypeUserFound,
		UserID:   userID,
		Username: username,
		Timestamp: time.Now(),
	}
}

// NewUserNotFoundMessage создает ответ об отсутствии пользователя
func NewUserNotFoundMessage(username string) *Message {
	return &Message{
		Type:     MessageTypeUserNotFound,
		Content:  username,
		Timestamp: time.Now(),
	}
}
