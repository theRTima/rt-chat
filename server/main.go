package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/theRTima/rt-chat/handlers"
	"github.com/theRTima/rt-chat/models"
)

func main() {
	// Парсим флаги командной строки
	addr := flag.String("addr", ":8080", "HTTP service address")
	flag.Parse()

	// Создаем hub для управления клиентами
	hub := models.NewHub()

	// Запускаем hub в отдельной goroutine
	go hub.Run()

	// Настраиваем HTTP routes
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handlers.ServeWs(hub, w, r)
	})

	// Простой health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Endpoint для получения количества подключенных клиентов
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		count := hub.GetClientCount()
		w.Write([]byte("{\"clients\": " + string(rune(count+'0')) + "}"))
	})

	// Запускаем HTTP сервер
	log.Printf("Starting server on %s", *addr)
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe error: ", err)
	}
}
