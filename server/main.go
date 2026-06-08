package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/theRTima/rt-chat/handlers"
	"github.com/theRTima/rt-chat/models"
	"github.com/theRTima/rt-chat/storage"
)

var acceptedConns int64

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

	http.HandleFunc("/rooms", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		rooms, err := db.GetRooms(ctx)
		if err != nil {
			log.Printf("Failed to get rooms: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(rooms); err != nil {
			log.Printf("Failed to encode rooms: %v", err)
		}
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"clients": %d, "attempts": %d, "upgrade_failures": %d, "accepts": %d, "goroutines": %d, "rooms": %d}`,
			hub.GetClientCount(), handlers.TotalAttempts, handlers.UpgradeFailures, atomic.LoadInt64(&acceptedConns), runtime.NumGoroutine(), hub.GetRoomCount())
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

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	srv := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB
	}

	err = srv.Serve(&countingListener{Listener: ln})
	if err != nil {
		log.Fatal("ListenAndServe error: ", err)
	}
}

type countingListener struct {
	net.Listener
}

func (cl *countingListener) Accept() (net.Conn, error) {
	conn, err := cl.Listener.Accept()
	if err == nil {
		curr := atomic.AddInt64(&acceptedConns, 1)
		if curr%100 == 0 || curr <= 5 {
			log.Printf("[diag] Accepted TCP conn: %d", curr)
		}
	}
	return conn, err
}
