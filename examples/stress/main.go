// stress is a comprehensive load test for the OCTAR broker.
//
// It spawns N publisher goroutines and M subscriber goroutines, each on their
// own TCP connection. Publishers send messages as fast as possible; subscribers
// ACK or NACK randomly. Exhausted retries route to a DLQ queue consumed by a
// dedicated goroutine.
//
// Metrics are printed every second:
//
//	[  1s]  pub: 12.3K/s  recv: 11.2K/s  ack: 10.1K/s  nack: 1.1K/s  dlq: 0/s  lat: 2.1ms
//
// Usage:
//
//	# Terminal 1 — start broker
//	go run ./cmd/broker
//
//	# Terminal 2 — run stress test (auto-creates queues and configures DLQ)
//	go run ./examples/stress \
//	    -publishers 8 -subscribers 8 \
//	    -nack-rate 15 -duration 60s
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/83codes/octar/internal/protocol"
)

// ── Shared counters (all atomic) ─────────────────────────────────────────────

var (
	cntPublished atomic.Int64
	cntReceived  atomic.Int64
	cntAcked     atomic.Int64
	cntNacked    atomic.Int64
	cntDLQ       atomic.Int64
	cntErrors    atomic.Int64
	cntLatNs     atomic.Int64 // total latency nanoseconds (for avg)
	cntLatCount  atomic.Int64 // number of latency samples
	cntLatMaxNs  atomic.Int64 // max latency nanoseconds (updated with CAS)
)

// ── Client helper ─────────────────────────────────────────────────────────────

type client struct {
	conn net.Conn
	enc  *protocol.Encoder
	dec  *protocol.Decoder
}

func dial(addr, user, pass, ns string) (*client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	if err := enc.WriteConnect(protocol.ConnectFrame{
		Username: user, Password: pass, Namespace: ns,
	}); err != nil {
		conn.Close()
		return nil, err
	}

	ft, frame, err := dec.ReadFrame()
	if err != nil {
		conn.Close()
		return nil, err
	}
	if ft == protocol.FrameConnectErr {
		conn.Close()
		return nil, fmt.Errorf("auth: %s", frame.(protocol.ConnectErrFrame).Reason)
	}
	if ft != protocol.FrameConnectOK {
		conn.Close()
		return nil, fmt.Errorf("unexpected frame 0x%02x", byte(ft))
	}
	return &client{conn: conn, enc: enc, dec: dec}, nil
}

// ── Publisher ─────────────────────────────────────────────────────────────────

func runPublisher(id int, addr, user, pass, ns, queue, group string, payloadSize int, stop <-chan struct{}) {
	c, err := dial(addr, user, pass, ns)
	if err != nil {
		log.Printf("publisher-%d: %v", id, err)
		cntErrors.Add(1)
		return
	}
	defer c.conn.Close()

	// Payload: 8B timestamp prefix + zero-padded body
	payload := make([]byte, max(payloadSize, 8))

	for {
		select {
		case <-stop:
			return
		default:
		}

		binary.BigEndian.PutUint64(payload[:8], uint64(time.Now().UnixNano()))

		if err := c.enc.WritePublish(protocol.PublishFrame{
			Queue:   queue,
			Group:   group,
			Payload: payload,
		}); err != nil {
			cntErrors.Add(1)
			return
		}

		ft, _, err := c.dec.ReadFrame()
		if err != nil {
			cntErrors.Add(1)
			return
		}
		if ft == protocol.FramePublishOK {
			cntPublished.Add(1)
		} else {
			cntErrors.Add(1)
		}
	}
}

// ── Subscriber ────────────────────────────────────────────────────────────────

type frameResult struct {
	ft    protocol.FrameType
	frame any
	err   error
}

func runSubscriber(id int, addr, user, pass, ns, queue, group string, nackPct int, stop <-chan struct{}) {
	c, err := dial(addr, user, pass, ns)
	if err != nil {
		log.Printf("subscriber-%d: %v", id, err)
		cntErrors.Add(1)
		return
	}
	defer c.conn.Close()

	if err := c.enc.WriteSubscribe(protocol.SubscribeFrame{Queue: queue, Group: group}); err != nil {
		cntErrors.Add(1)
		return
	}

	ackCh := make(chan protocol.MessageFrame, 4096)
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case msg := <-ackCh:
				if nackPct > 0 && rand.IntN(100) < nackPct {
					c.enc.WriteNACK(protocol.NACKFrame{ //nolint:errcheck
						MsgID: msg.MsgID, Queue: msg.Queue, Group: msg.Group,
						Reason: "stress-test-simulated-failure",
					})
					cntNacked.Add(1)
				} else {
					c.enc.WriteACK(protocol.ACKFrame{ //nolint:errcheck
						MsgID: msg.MsgID, Queue: msg.Queue, Group: msg.Group,
					})
					cntAcked.Add(1)
				}

				// Drain channel
				drained := 0
			DrainLoop:
				for {
					select {
					case m := <-ackCh:
						if nackPct > 0 && rand.IntN(100) < nackPct {
							c.enc.WriteNACK(protocol.NACKFrame{ //nolint:errcheck
								MsgID: m.MsgID, Queue: m.Queue, Group: m.Group,
								Reason: "stress-test-simulated-failure",
							})
							cntNacked.Add(1)
						} else {
							c.enc.WriteACK(protocol.ACKFrame{ //nolint:errcheck
								MsgID: m.MsgID, Queue: m.Queue, Group: m.Group,
							})
							cntAcked.Add(1)
						}
						drained++
						if drained >= 100 {
							break DrainLoop
						}
					default:
						break DrainLoop
					}
				}
				c.enc.Flush()
			case <-ticker.C:
				c.enc.Flush()
			}
		}
	}()

	go func() {
		for {
			ft, frame, err := c.dec.ReadFrame()
			if err != nil {
				return
			}
			if ft != protocol.FrameMessage {
				continue
			}
			msg := frame.(protocol.MessageFrame)

			cntReceived.Add(1)

			// Parse embedded timestamp to calculate latency
			if len(msg.Payload) >= 8 {
				tsNs := binary.BigEndian.Uint64(msg.Payload[:8])
				if tsNs > 0 {
					latNs := int64(uint64(time.Now().UnixNano()) - tsNs)
					cntLatNs.Add(latNs)
					cntLatCount.Add(1)
					// Update max latency with CAS loop.
					for {
						cur := cntLatMaxNs.Load()
						if latNs <= cur {
							break
						}
						if cntLatMaxNs.CompareAndSwap(cur, latNs) {
							break
						}
					}
				}
			}

			select {
			case ackCh <- msg:
			case <-stop:
				return
			}
		}
	}()

	// Block here so defer c.conn.Close() doesn't fire until the test ends.
	<-stop
}

// ── DLQ subscriber ────────────────────────────────────────────────────────────

func runDLQSubscriber(addr, user, pass, ns, dlqQueue, group string, stop <-chan struct{}) {
	c, err := dial(addr, user, pass, ns)
	if err != nil {
		log.Printf("dlq-subscriber: %v", err)
		return
	}
	defer c.conn.Close()

	c.enc.WriteSubscribe(protocol.SubscribeFrame{Queue: dlqQueue, Group: group}) //nolint:errcheck

	frameCh := make(chan frameResult, 4)
	go func() {
		for {
			ft, frame, err := c.dec.ReadFrame()
			frameCh <- frameResult{ft, frame, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-stop:
			return
		case r := <-frameCh:
			if r.err != nil {
				return
			}
			if r.ft != protocol.FrameMessage {
				continue
			}
			msg := r.frame.(protocol.MessageFrame)
			cntDLQ.Add(1)
			// Always ACK DLQ messages — they are already dead-lettered.
			c.enc.WriteACK(protocol.ACKFrame{ //nolint:errcheck
				MsgID: msg.MsgID, Queue: msg.Queue, Group: msg.Group,
			})
		}
	}
}

// ── Stats reporter ────────────────────────────────────────────────────────────

func runReporter(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	header := fmt.Sprintf("%-6s  %10s  %10s  %10s  %10s  %10s  %10s",
		"time", "pub/s", "recv/s", "ack/s", "nack/s", "dlq/s", "avg-lat")
	fmt.Println(header)
	fmt.Println(strings.Repeat("─", len(header)))

	var prevPub, prevRecv, prevAck, prevNack, prevDLQ int64
	elapsed := 0

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			elapsed++
			pub := cntPublished.Load()
			recv := cntReceived.Load()
			ack := cntAcked.Load()
			nack := cntNacked.Load()
			dlq := cntDLQ.Load()

			latStr := "—"
			if n := cntLatCount.Load(); n > 0 {
				avgMs := float64(cntLatNs.Load()) / float64(n) / 1e6
				maxMs := float64(cntLatMaxNs.Load()) / 1e6
				latStr = fmt.Sprintf("%.2fms/%.0fms", avgMs, maxMs)
			}

			fmt.Printf("[%3ds]  %10s  %10s  %10s  %10s  %10s  %10s\n",
				elapsed,
				fmtRate(pub-prevPub),
				fmtRate(recv-prevRecv),
				fmtRate(ack-prevAck),
				fmtRate(nack-prevNack),
				fmtRate(dlq-prevDLQ),
				latStr,
			)
			prevPub, prevRecv, prevAck, prevNack, prevDLQ = pub, recv, ack, nack, dlq
		}
	}
}

func fmtRate(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ── REST API setup ────────────────────────────────────────────────────────────

func apiLogin(apiAddr, user, pass string) string {
	body, _ := json.Marshal(map[string]string{"username": user, "password": pass})
	resp, err := http.Post(apiAddr+"/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	token := result["token"]
	if token == "" {
		log.Fatalf("login failed — no token in response")
	}
	return token
}

func apiPost(apiAddr, token, path string, body any) {
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", apiAddr+path, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != 409 { // 409 is fine (already exists)
		log.Fatalf("POST %s failed with status %d", path, resp.StatusCode)
	}
}

func apiPut(apiAddr, token, path string, body any) {
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", apiAddr+path, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("PUT %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		log.Fatalf("PUT %s failed with status %d: %s", path, resp.StatusCode, string(b))
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func isStopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	addr := flag.String("addr", "localhost:7000", "broker TCP address")
	apiAddr := flag.String("api", "http://localhost:8080", "broker REST API address")
	user := flag.String("user", "admin", "username")
	pass := flag.String("pass", "admin", "password")
	ns := flag.String("ns", "main", "namespace")
	queue := flag.String("queue", "stress-main", "queue name")
	group := flag.String("group", "stress-group", "group key")
	dlqQueue := flag.String("dlq-queue", "stress-dlq", "DLQ queue name")
	publishers := flag.Int("publishers", 2, "publisher goroutines (each uses one TCP connection)")
	subscribers := flag.Int("subscribers", 16, "subscriber goroutines (each uses one TCP connection)")
	nackRate := flag.Int("nack-rate", 10, "percentage of messages to NACK (0–100)")
	payloadSize := flag.Int("payload-size", 64, "message payload size in bytes")
	maxAttempts := flag.Int("max-attempts", 3, "max retry attempts before DLQ")
	parallelism := flag.Int("parallelism", 10, "consumer parallelism per group")
	duration := flag.Duration("duration", 6*time.Second, "test duration (0 = run until Ctrl+C)")
	setup := flag.Bool("setup", true, "auto-create queues and configure DLQ via REST API")
	flag.Parse()

	// ── Setup ────────────────────────────────────────────────────────────────
	if *setup {
		fmt.Printf("setting up queues via %s...\n", *apiAddr)
		token := apiLogin(*apiAddr, *user, *pass)

		apiPost(*apiAddr, token, "/queues", map[string]string{
			"name": *queue, "namespace": *ns,
		})
		apiPost(*apiAddr, token, "/queues", map[string]string{
			"name": *dlqQueue, "namespace": *ns,
		})

		groupPath := fmt.Sprintf("/queues/%s/%s/groups/%s", *ns, *queue, *group)
		apiPut(*apiAddr, token, groupPath, map[string]any{
			"parallelism":   *parallelism,
			"lease_timeout": "2s",
			"retry": map[string]any{
				"max_attempts":  *maxAttempts,
				"backoff":       "exponential",
				"initial_delay": "100ms",
				"max_delay":     "5s",
			},
			"dlq": map[string]any{
				"enabled": true,
				"queue":   *dlqQueue,
			},
		})
		fmt.Printf("queue %-20s configured (group=%s, parallelism=%d, max_attempts=%d, dlq=%s)\n",
			*ns+"/"+*queue, *group, *parallelism, *maxAttempts, *dlqQueue)
	}

	fmt.Printf("\nstress test: %d publishers, %d subscribers, nack-rate=%d%%, payload=%dB, duration=%s\n\n",
		*publishers, *subscribers, *nackRate, *payloadSize, *duration)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Subscribers
	for i := range *subscribers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runSubscriber(id, *addr, *user, *pass, *ns, *queue, *group, *nackRate, stop)
		}(i)
	}

	// DLQ subscriber
	wg.Add(1)
	go func() {
		defer wg.Done()
		runDLQSubscriber(*addr, *user, *pass, *ns, *dlqQueue, *group, stop)
	}()

	// Publishers (start after subscribers are registered)
	time.Sleep(200 * time.Millisecond)
	for i := range *publishers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runPublisher(id, *addr, *user, *pass, *ns, *queue, *group, *payloadSize, stop)
		}(i)
	}

	// Reporter
	wg.Add(1)
	go func() {
		defer wg.Done()
		runReporter(stop)
	}()

	// Wait for duration or Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	if *duration > 0 {
		select {
		case <-sigCh:
			fmt.Println("\ninterrupted")
		case <-time.After(*duration):
			fmt.Printf("\nduration elapsed (%s)\n", *duration)
		}
	} else {
		<-sigCh
		fmt.Println("\ninterrupted")
	}

	close(stop)
	wg.Wait()

	// ── Final summary ─────────────────────────────────────────────────────────
	pub := cntPublished.Load()
	recv := cntReceived.Load()
	ack := cntAcked.Load()
	nack := cntNacked.Load()
	dlq := cntDLQ.Load()
	errs := cntErrors.Load()

	secs := duration.Seconds()
	if secs == 0 {
		secs = 1
	}

	fmt.Println()
	fmt.Println("── Summary ──────────────────────────────────────────────────────")
	fmt.Printf("  published : %8d  (~%.0f/s)\n", pub, float64(pub)/secs)
	fmt.Printf("  received  : %8d  (~%.0f/s)\n", recv, float64(recv)/secs)
	fmt.Printf("  acked     : %8d  (~%.0f/s)\n", ack, float64(ack)/secs)
	fmt.Printf("  nacked    : %8d  (~%.0f/s)\n", nack, float64(nack)/secs)
	fmt.Printf("  dlq       : %8d\n", dlq)
	fmt.Printf("  errors    : %8d\n", errs)
	if n := cntLatCount.Load(); n > 0 {
		avgMs := float64(cntLatNs.Load()) / float64(n) / 1e6
		maxMs := float64(cntLatMaxNs.Load()) / 1e6
		fmt.Printf("  latency   :  avg=%.2fms  max=%.0fms\n", avgMs, maxMs)
	}
	fmt.Println("─────────────────────────────────────────────────────────────────")
}
