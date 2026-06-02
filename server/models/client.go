package models

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Время ожидания записи в WebSocket
	writeWait = 10 * time.Second

	// Время ожидания следующего pong сообщения от клиента
	pongWait = 60 * time.Second

	// Интервал отправки ping сообщений клиенту (должен быть меньше pongWait)
	pingPeriod = (pongWait * 9) / 10

	// Максимальный размер сообщения
	maxMessageSize = 512
)

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

// Client представляет одно WebSocket соединение
type Client struct {
	// Hub к которому принадлежит клиент
	Hub *Hub

	// WebSocket соединение
	Conn *websocket.Conn

	// Буферизованный канал исходящих сообщений
	Send chan []byte

	// ID пользователя (можно расширить для авторизации)
	ID string
}

// ReadPump читает сообщения из WebSocket соединения и отправляет их в hub
// Запускается в отдельной goroutine для каждого соединения
// ReadPump гарантирует, что для одного соединения работает только один reader
func (c *Client) ReadPump() {
	defer func() {
		// При завершении работы отключаем клиента от hub
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	// Настраиваем параметры чтения
	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Бесконечный цикл чтения сообщений
	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		// Отправляем полученное сообщение в hub для broadcast
		c.Hub.Broadcast <- message
	}
}

// WritePump отправляет сообщения из hub в WebSocket соединение
// Запускается в отдельной goroutine для каждого соединения
// WritePump гарантирует, что для одного соединения работает только один writer
func (c *Client) WritePump() {
	// Ticker для отправки ping сообщений
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub закрыл канал
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Добавляем все ожидающие сообщения из канала в текущую запись
			n := len(c.Send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.Send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			// Отправляем ping для проверки соединения
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
