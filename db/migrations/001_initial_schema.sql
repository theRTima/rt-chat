-- Создание таблицы пользователей
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска по user_id
CREATE INDEX idx_users_user_id ON users(user_id);

-- Создание таблицы комнат
CREATE TABLE IF NOT EXISTS rooms (
    id SERIAL PRIMARY KEY,
    room_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска по room_id
CREATE INDEX idx_rooms_room_id ON rooms(room_id);

-- Создание таблицы сообщений
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

-- Индексы для быстрого поиска сообщений
CREATE INDEX idx_messages_room_id ON messages(room_id, created_at DESC);
CREATE INDEX idx_messages_user_id ON messages(user_id);
CREATE INDEX idx_messages_to_user_id ON messages(to_user_id, created_at DESC);
CREATE INDEX idx_messages_created_at ON messages(created_at DESC);

-- Создание таблицы участников комнат (для отслеживания кто в какой комнате)
CREATE TABLE IF NOT EXISTS room_members (
    id SERIAL PRIMARY KEY,
    room_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    left_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(room_id, user_id, left_at)
);

-- Индексы для участников комнат
CREATE INDEX idx_room_members_room_id ON room_members(room_id);
CREATE INDEX idx_room_members_user_id ON room_members(user_id);
CREATE INDEX idx_room_members_active ON room_members(room_id, user_id) WHERE left_at IS NULL;
