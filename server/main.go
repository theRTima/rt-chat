package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/theRTima/rt-chat/handlers"
	"github.com/theRTima/rt-chat/models"
	"github.com/theRTima/rt-chat/storage"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP service address")
	flag.Parse()

	ctx := context.Background()

	db, err := storage.NewDB(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.InitSchema(ctx); err != nil {
		log.Fatalf("Failed to initialize database schema: %v", err)
	}

	persister := storage.NewMessagePersister(db, 1024, 50, 3, 2*time.Second)
	persister.Start()

	hub := models.NewHub(db)

	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handlers.ServeWs(hub, persister, w, r)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		count := hub.GetClientCount()
		w.Write([]byte(`{"clients": ` + strconv.Itoa(count) + `}`))
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		persister.Stop()
		db.Close()
		os.Exit(0)
	}()

	log.Printf("Starting server on %s", *addr)

	srv := &http.Server{
		Addr:              *addr,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB
	}

	err = srv.ListenAndServe()
	if err != nil {
		log.Fatal("ListenAndServe error: ", err)
	}
}
