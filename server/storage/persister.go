package storage

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/theRTima/rt-chat/models"
)

// MessagePersister обрабатывает асинхронное сохранение сообщений в базу данных
type MessagePersister struct {
	db            *DB
	messageQueue  chan *models.Message
	batchSize     int
	flushInterval time.Duration
	workerCount   int
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewMessagePersister создает новый MessagePersister с worker pool
func NewMessagePersister(db *DB, bufferSize, batchSize, workerCount int, flushInterval time.Duration) *MessagePersister {
	ctx, cancel := context.WithCancel(context.Background())

	return &MessagePersister{
		db:            db,
		messageQueue:  make(chan *models.Message, bufferSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		workerCount:   workerCount,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start запускает worker pool для обработки сообщений
func (mp *MessagePersister) Start() {
	log.Printf("Starting message persister with %d workers, batch size %d, buffer size %d",
		mp.workerCount, mp.batchSize, cap(mp.messageQueue))

	for i := 0; i < mp.workerCount; i++ {
		mp.wg.Add(1)
		go mp.worker(i)
	}
}

// Stop останавливает все workers и закрывает каналы
func (mp *MessagePersister) Stop() {
	log.Printf("Stopping message persister...")
	mp.cancel()
	close(mp.messageQueue)
	mp.wg.Wait()
	log.Printf("Message persister stopped")
}

// Enqueue добавляет сообщение в очередь для сохранения (неблокирующее)
func (mp *MessagePersister) Enqueue(msg *models.Message) {
	select {
	case mp.messageQueue <- msg:
		// Сообщение добавлено в очередь
	default:
		// Очередь заполнена - логируем и пропускаем (не блокируем broadcast)
		log.Printf("Message queue full, dropping message from user %s", msg.UserID)
	}
}

// worker обрабатывает сообщения из очереди пакетами
func (mp *MessagePersister) worker(id int) {
	defer mp.wg.Done()

	batch := make([]*models.Message, 0, mp.batchSize)
	ticker := time.NewTicker(mp.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := mp.db.SaveMessagesBatch(ctx, batch); err != nil {
			log.Printf("Worker %d: failed to save batch of %d messages: %v", id, len(batch), err)
		} else {
			log.Printf("Worker %d: saved batch of %d messages", id, len(batch))
		}

		// Очищаем batch
		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-mp.messageQueue:
			if !ok {
				// Канал закрыт, сохраняем оставшиеся сообщения
				flush()
				return
			}

			// Добавляем сообщение в batch
			batch = append(batch, msg)

			// Если batch заполнен - сохраняем
			if len(batch) >= mp.batchSize {
				flush()
			}

		case <-ticker.C:
			// Периодически сохраняем накопленные сообщения, даже если batch не заполнен
			flush()

		case <-mp.ctx.Done():
			// Контекст отменен, сохраняем оставшиеся сообщения
			flush()
			return
		}
	}
}
