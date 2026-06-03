package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

/*
OS limits for 10,000 concurrent connections:

macOS:
  sudo launchctl limit maxfiles 1048576 1048576
  ulimit -n 1048576
  (check: ulimit -n; launchctl limit maxfiles)

Linux:
  /etc/security/limits.conf:
    *         hard    nofile      1048576
    *         soft    nofile      1048576
  Then: ulimit -n 1048576
  (check: ulimit -n; cat /proc/sys/fs/file-max)

Kernel tweaks (Linux):
  sysctl -w net.core.somaxconn=65535
  sysctl -w net.ipv4.tcp_max_syn_backlog=65535
  sysctl -w net.ipv4.ip_local_port_range="1024 65535"
  sysctl -w net.ipv4.tcp_tw_reuse=1
*/

// ---------- protocol types (mirrors server's models.Message) ----------

type clientMessage struct {
	Type      string `json:"type"`
	RoomID    string `json:"room_id,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	Username  string `json:"username,omitempty"`
	ToUserID  string `json:"to_user_id,omitempty"`
	Content   string `json:"content,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ---------- client wrapper ----------

type loadClient struct {
	conn   *websocket.Conn
	userID string
}

// ---------- stats collector (thread-safe) ----------

type latencySummary struct {
	Avg     time.Duration
	P50     time.Duration
	P95     time.Duration
	P99     time.Duration
	Min     time.Duration
	Max     time.Duration
	Samples int
}

type statsCollector struct {
	connected int64
	failed    int64
	msgSent   int64
	msgRecv   int64

	mu        sync.Mutex
	latencies []time.Duration
}

func (s *statsCollector) recordLatency(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
}

func (s *statsCollector) snapshot() (connected, failed, msgSent, msgRecv int64) {
	return atomic.LoadInt64(&s.connected),
		atomic.LoadInt64(&s.failed),
		atomic.LoadInt64(&s.msgSent),
		atomic.LoadInt64(&s.msgRecv)
}

func (s *statsCollector) latencySummary() latencySummary {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.latencies) == 0 {
		return latencySummary{}
	}

	sorted := make([]time.Duration, len(s.latencies))
	copy(sorted, s.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}

	n := len(sorted)
	return latencySummary{
		Avg:     sum / time.Duration(n),
		P50:     sorted[n*50/100],
		P95:     sorted[n*95/100],
		P99:     sorted[n*99/100],
		Min:     sorted[0],
		Max:     sorted[n-1],
		Samples: n,
	}
}

// ---------- latency probe helpers ----------

// probeContent builds a unique message content that embeds a nanosecond timestamp.
// Format: __lat_<unix_nano>__   (the reader checks for this prefix)
func probeContent() string {
	return fmt.Sprintf("__lat_%d__", time.Now().UnixNano())
}

func extractTimestamp(content string) (int64, bool) {
	s := strings.TrimPrefix(content, "__lat_")
	s = strings.TrimSuffix(s, "__")
	if s == content || s == "" {
		return 0, false
	}
	ts, err := strconv.ParseInt(s, 10, 64)
	return ts, err == nil
}

// ---------- readPump: per-connection read goroutine ----------

func readPump(conn *websocket.Conn, userID string, stats *statsCollector) {
	defer conn.Close()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		// Fast path: skip frames without latency probe
		if !bytes.Contains(data, []byte("__lat_")) {
			continue
		}

		var msg clientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		// Only measure latency for our own echo (server broadcasts to all)
		if msg.UserID == userID && strings.HasPrefix(msg.Content, "__lat_") {
			ts, ok := extractTimestamp(msg.Content)
			if ok {
				latency := time.Duration(time.Now().UnixNano() - ts)
				stats.recordLatency(latency)
				atomic.AddInt64(&stats.msgRecv, 1)
			}
		}
	}
}

// ---------- connect a single client ----------

func connectClient(id int, server string, stats *statsCollector) *loadClient {
	userID := fmt.Sprintf("loadtest_%d", id)
	username := fmt.Sprintf("Bot_%d", id)

	u := url.URL{
		Scheme: "ws",
		Host:   server,
		Path:   "/ws",
		RawQuery: url.Values{
			"user_id":  {userID},
			"username": {username},
		}.Encode(),
	}

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&stats.failed, 1)
		return nil
	}

	// Join the general room so messages get broadcast to us
	joinMsg := clientMessage{Type: "join_room", RoomID: "general"}
	data, _ := json.Marshal(joinMsg)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		conn.Close()
		atomic.AddInt64(&stats.failed, 1)
		return nil
	}

	atomic.AddInt64(&stats.connected, 1)
	go readPump(conn, userID, stats)

	return &loadClient{conn: conn, userID: userID}
}

// ---------- messengerLoop: periodically sends probe messages ----------

func messengerLoop(client *loadClient, stats *statsCollector, interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			msg := clientMessage{
				Type:    "chat",
				RoomID:  "general",
				Content: probeContent(),
			}
			data, _ := json.Marshal(msg)

			client.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := client.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
			atomic.AddInt64(&stats.msgSent, 1)
		}
	}
}

// ---------- main ----------

func main() {
	target := flag.Int("target", 10000, "Number of WebSocket connections to establish")
	rate := flag.Int("rate", 500, "New connections per second (ramp rate)")
	server := flag.String("server", "localhost:8080", "WebSocket server address (host:port)")
	duration := flag.Duration("duration", 30*time.Second, "How long to run after ramp-up completes")
	messengers := flag.Int("messengers", 10, "Number of connected clients that send messages")
	interval := flag.Duration("interval", 5*time.Second, "Interval between messages per messenger")
	flag.Parse()

	if *messengers > *target {
		*messengers = *target
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("  WebSocket Load Test")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("  Target connections:  %d\n", *target)
	fmt.Printf("  Ramp rate:           %d conn/s\n", *rate)
	fmt.Printf("  Server:              ws://%s/ws\n", *server)
	fmt.Printf("  Test duration:       %s\n", *duration)
	fmt.Printf("  Messengers:          %d\n", *messengers)
	fmt.Printf("  Msg interval:        %s\n", *interval)
	fmt.Println(strings.Repeat("=", 60))

	stats := &statsCollector{}
	done := make(chan struct{})

	// Signal handler for graceful Ctrl+C
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	// ── Phase 1: ramp-up ──────────────────────────────────────────────

	rampInterval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(rampInterval)
	defer ticker.Stop()

	totalInitiated := 0
	start := time.Now()
	fmt.Printf("\n► Ramping up %d connections at %d/s...\n\n", *target, *rate)

	for i := 0; i < *target; i++ {
		select {
		case <-sig:
			fmt.Println("\nInterrupted during ramp-up.")
			close(done)
			printFinal(stats, start, totalInitiated)
			return
		case <-ticker.C:
		}

		go func(id int) {
			client := connectClient(id, *server, stats)
			if client == nil {
				return
			}
			if id < *messengers {
				go messengerLoop(client, stats, *interval, done)
			}
		}(i)

		totalInitiated++

		if i > 0 && i%1000 == 0 {
			c, f, _, _ := stats.snapshot()
			fmt.Printf("  %5d / %d initiated  |  connected: %d  failed: %d\n", i, *target, c, f)
		}
	}

	// Wait for all connections to finish dialling
	fmt.Println("\n  Waiting for remaining dials to complete...")
	for {
		c, f := atomic.LoadInt64(&stats.connected), atomic.LoadInt64(&stats.failed)
		if c+f >= int64(*target) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	rampElapsed := time.Since(start)
	c, f, _, _ := stats.snapshot()
	fmt.Printf("\n  ✓ Ramp-up complete: %d connected, %d failed  [%s]\n\n",
		c, f, rampElapsed.Round(time.Second))

	// ── Phase 2: sustained load ───────────────────────────────────────

	fmt.Printf("► Running load for %s...\n", *duration)
	testStart := time.Now()

	// Background stats ticker
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-done:
				return
			case <-statsTicker.C:
				conn, fail, sent, recv := stats.snapshot()
				ls := stats.latencySummary()
				fmt.Printf("  [%s] conn: %d  failed: %d  msgs: %d sent / %d recv  latency (avg/p95): %s / %s\n",
					time.Since(testStart).Round(time.Second),
					conn, fail, sent, recv,
					ls.Avg.Round(time.Microsecond),
					ls.P95.Round(time.Microsecond))
			}
		}
	}()

	select {
	case <-sig:
		fmt.Println("\nInterrupted during load phase.")
	case <-time.After(*duration):
	}

	close(done)
	time.Sleep(2 * time.Second) // let in-flight echo messages arrive

	// ── Phase 3: report ───────────────────────────────────────────────

	printFinal(stats, start, totalInitiated)
}

func printFinal(stats *statsCollector, start time.Time, initiated int) {
	c, f, sent, recv := stats.snapshot()
	ls := stats.latencySummary()
	elapsed := time.Since(start).Round(time.Second)

	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("  RESULTS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("  Initiated:         %d\n", initiated)
	fmt.Printf("  Connected:         %d\n", c)
	fmt.Printf("  Failed:            %d\n", f)
	fmt.Printf("  Success rate:      %.1f%%\n", 100*float64(c)/float64(initiated))
	fmt.Printf("  Elapsed:           %s\n", elapsed)
	fmt.Println("  ── Messages ──")
	fmt.Printf("  Sent:              %d\n", sent)
	fmt.Printf("  Received (echo):   %d\n", recv)
	if sent > 0 {
		fmt.Printf("  Delivery rate:     %.1f%%\n", 100*float64(recv)/float64(sent))
	}
	fmt.Println("  ── Latency ──")
	fmt.Printf("  Average:           %s\n", ls.Avg.Round(time.Microsecond))
	fmt.Printf("  P50 (median):      %s\n", ls.P50.Round(time.Microsecond))
	fmt.Printf("  P95:               %s\n", ls.P95.Round(time.Microsecond))
	fmt.Printf("  P99:               %s\n", ls.P99.Round(time.Microsecond))
	fmt.Printf("  Min:               %s\n", ls.Min.Round(time.Microsecond))
	fmt.Printf("  Max:               %s\n", ls.Max.Round(time.Microsecond))
	fmt.Printf("  Samples:           %d\n", ls.Samples)
	fmt.Println(strings.Repeat("=", 60))
}
