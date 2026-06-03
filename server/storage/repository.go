package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/theRTima/rt-chat/models"
)

// UpsertUser создает или обновляет пользователя в базе данных
func (db *DB) UpsertUser(ctx context.Context, userID, username string) error {
	query := `
		INSERT INTO users (user_id, username, created_at, last_seen)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id)
		DO UPDATE SET username = $2, last_seen = $4
	`
	_, err := db.Pool.Exec(ctx, query, userID, username, time.Now(), time.Now())
	return err
}

// GetUser получает пользователя по user_id
func (db *DB) GetUser(ctx context.Context, userID string) (*models.User, error) {
	query := `SELECT user_id, username, created_at, last_seen FROM users WHERE user_id = $1`

	var user models.User
	err := db.Pool.QueryRow(ctx, query, userID).Scan(
		&user.UserID,
		&user.Username,
		&user.CreatedAt,
		&user.LastSeen,
	)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// UpsertRoom создает или обновляет комнату в базе данных
func (db *DB) UpsertRoom(ctx context.Context, roomID, name string) error {
	query := `
		INSERT INTO rooms (room_id, name, created_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (room_id)
		DO UPDATE SET name = $2
	`
	if name == "" {
		name = roomID
	}
	_, err := db.Pool.Exec(ctx, query, roomID, name, time.Now())
	return err
}

// SaveMessage сохраняет одно сообщение в базу данных
func (db *DB) SaveMessage(ctx context.Context, msg *models.Message) error {
	query := `
		INSERT INTO messages (message_type, room_id, user_id, username, to_user_id, content, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := db.Pool.Exec(ctx, query,
		msg.Type,
		msg.RoomID,
		msg.UserID,
		msg.Username,
		msg.ToUserID,
		msg.Content,
		msg.Timestamp,
	)
	return err
}

// SaveMessagesBatch сохраняет несколько сообщений одним запросом (batch insert)
func (db *DB) SaveMessagesBatch(ctx context.Context, messages []*models.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Используем batch insert для производительности
	query := `
		INSERT INTO messages (message_type, room_id, user_id, username, to_user_id, content, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	batch := &pgBatch{db: db, query: query}
	for _, msg := range messages {
		batch.Queue(
			msg.Type,
			msg.RoomID,
			msg.UserID,
			msg.Username,
			msg.ToUserID,
			msg.Content,
			msg.Timestamp,
		)
	}

	return batch.Send(ctx)
}

// GetRoomHistory получает последние N сообщений из комнаты
func (db *DB) GetRoomHistory(ctx context.Context, roomID string, limit int) ([]*models.Message, error) {
	query := `
		SELECT message_type, room_id, user_id, username, to_user_id, content, created_at
		FROM messages
		WHERE room_id = $1 AND message_type IN ('chat', 'user_joined', 'user_left')
		ORDER BY created_at DESC
		LIMIT $2
	`

	rows, err := db.Pool.Query(ctx, query, roomID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*models.Message
	for rows.Next() {
		var msg models.Message
		var toUserID *string
		var roomID *string

		err := rows.Scan(
			&msg.Type,
			&roomID,
			&msg.UserID,
			&msg.Username,
			&toUserID,
			&msg.Content,
			&msg.Timestamp,
		)
		if err != nil {
			return nil, err
		}

		if toUserID != nil {
			msg.ToUserID = *toUserID
		}
		if roomID != nil {
			msg.RoomID = *roomID
		}

		messages = append(messages, &msg)
	}

	// Переворачиваем массив, чтобы сообщения были в хронологическом порядке
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	return messages, rows.Err()
}

// GetPrivateMessageHistory получает последние N приватных сообщений между двумя пользователями
func (db *DB) GetPrivateMessageHistory(ctx context.Context, userID1, userID2 string, limit int) ([]*models.Message, error) {
	query := `
		SELECT message_type, room_id, user_id, username, to_user_id, content, created_at
		FROM messages
		WHERE message_type = 'private'
		AND ((user_id = $1 AND to_user_id = $2) OR (user_id = $2 AND to_user_id = $1))
		ORDER BY created_at DESC
		LIMIT $3
	`

	rows, err := db.Pool.Query(ctx, query, userID1, userID2, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*models.Message
	for rows.Next() {
		var msg models.Message
		var toUserID *string
		var roomID *string

		err := rows.Scan(
			&msg.Type,
			&roomID,
			&msg.UserID,
			&msg.Username,
			&toUserID,
			&msg.Content,
			&msg.Timestamp,
		)
		if err != nil {
			return nil, err
		}

		if toUserID != nil {
			msg.ToUserID = *toUserID
		}
		if roomID != nil {
			msg.RoomID = *roomID
		}

		messages = append(messages, &msg)
	}

	// Переворачиваем массив
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	return messages, rows.Err()
}

// AddRoomMember добавляет пользователя в комнату
func (db *DB) AddRoomMember(ctx context.Context, roomID, userID string) error {
	query := `
		INSERT INTO room_members (room_id, user_id, joined_at, left_at)
		VALUES ($1, $2, $3, NULL)
		ON CONFLICT (room_id, user_id) WHERE left_at IS NULL DO NOTHING
	`
	_, err := db.Pool.Exec(ctx, query, roomID, userID, time.Now())
	return err
}

// RemoveRoomMember помечает выход пользователя из комнаты
func (db *DB) RemoveRoomMember(ctx context.Context, roomID, userID string) error {
	query := `
		UPDATE room_members
		SET left_at = $3
		WHERE room_id = $1 AND user_id = $2 AND left_at IS NULL
	`
	_, err := db.Pool.Exec(ctx, query, roomID, userID, time.Now())
	return err
}

// pgBatch is a helper for batch inserts
type pgBatch struct {
	db    *DB
	query string
	args  [][]interface{}
}

func (b *pgBatch) Queue(args ...interface{}) {
	b.args = append(b.args, args)
}

func (b *pgBatch) Send(ctx context.Context) error {
	if len(b.args) == 0 {
		return nil
	}

	tx, err := b.db.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, args := range b.args {
		if _, err := tx.Exec(ctx, b.query, args...); err != nil {
			return fmt.Errorf("failed to execute batch insert: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
