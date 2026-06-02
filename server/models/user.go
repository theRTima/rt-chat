package models

import "time"

// User представляет пользователя в базе данных
type User struct {
	UserID    string    `json:"user_id"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
	LastSeen  time.Time `json:"last_seen"`
}
