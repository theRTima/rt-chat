package storage

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB представляет подключение к базе данных
type DB struct {
	Pool *pgxpool.Pool
}

// NewDB создает новое подключение к PostgreSQL
func NewDB(ctx context.Context) (*DB, error) {
	// Получаем строку подключения из переменной окружения
	// Формат: postgres://username:password@localhost:5432/database_name
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/rtchat?sslmode=disable"
		log.Printf("DATABASE_URL not set, using default: %s", databaseURL)
	}

	// Настраиваем конфигурацию пула соединений
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database URL: %w", err)
	}

	// Настройки пула для высокой нагрузки
	config.MaxConns = 50                          // Максимум соединений
	config.MinConns = 5                           // Минимум соединений
	config.MaxConnLifetime = time.Hour            // Время жизни соединения
	config.MaxConnIdleTime = 30 * time.Minute     // Время idle перед закрытием
	config.HealthCheckPeriod = 1 * time.Minute    // Проверка здоровья соединений
	config.ConnConfig.ConnectTimeout = 5 * time.Second

	// Создаем пул соединений
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Проверяем подключение
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	log.Printf("Successfully connected to database")

	return &DB{Pool: pool}, nil
}

// Close закрывает все соединения с базой данных
func (db *DB) Close() {
	db.Pool.Close()
}

// InitSchema инициализирует схему базы данных
func (db *DB) InitSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		user_id VARCHAR(255) UNIQUE NOT NULL,
		username VARCHAR(255) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		last_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);

	CREATE TABLE IF NOT EXISTS rooms (
		id SERIAL PRIMARY KEY,
		room_id VARCHAR(255) UNIQUE NOT NULL,
		name VARCHAR(255),
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_rooms_room_id ON rooms(room_id);

	CREATE TABLE IF NOT EXISTS messages (
		id SERIAL PRIMARY KEY,
		message_type VARCHAR(50) NOT NULL,
		room_id VARCHAR(255),
		user_id VARCHAR(255) NOT NULL,
		username VARCHAR(255) NOT NULL,
		to_user_id VARCHAR(255),
		content TEXT,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_messages_room_id ON messages(room_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id);
	CREATE INDEX IF NOT EXISTS idx_messages_to_user_id ON messages(to_user_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at DESC);

	CREATE TABLE IF NOT EXISTS room_members (
		id SERIAL PRIMARY KEY,
		room_id VARCHAR(255) NOT NULL,
		user_id VARCHAR(255) NOT NULL,
		joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		left_at TIMESTAMP WITH TIME ZONE
	);

	CREATE INDEX IF NOT EXISTS idx_room_members_room_id ON room_members(room_id);
	CREATE INDEX IF NOT EXISTS idx_room_members_user_id ON room_members(user_id);
	`

	if _, err := db.Pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Migration: drop old unique constraint that treated NULLs as distinct
	db.Pool.Exec(ctx, `ALTER TABLE room_members DROP CONSTRAINT IF EXISTS room_members_room_id_user_id_left_at_key`)

	// Deduplicate active members before creating unique partial index
	db.Pool.Exec(ctx, `
		DELETE FROM room_members
		WHERE id IN (
			SELECT id FROM (
				SELECT id,
					ROW_NUMBER() OVER (PARTITION BY room_id, user_id ORDER BY joined_at DESC) as rn
				FROM room_members
				WHERE left_at IS NULL
			) sub WHERE rn > 1
		)
	`)

	// Replace old non-unique index with a unique one
	db.Pool.Exec(ctx, `DROP INDEX IF EXISTS idx_room_members_active`)
	if _, err := db.Pool.Exec(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_room_members_active ON room_members(room_id, user_id) WHERE left_at IS NULL`); err != nil {
		log.Printf("Warning: could not create unique index (non-fatal): %v", err)
	}

	log.Printf("Database schema initialized successfully")
	return nil
}
