// subscribe is a minimal OCTAR consumer example.
//
// Run after the broker is up and the queue "orders" has been declared (see publish example).
//
//	go run ./examples/subscribe -queue orders -group group-1
//
// The subscriber blocks and prints each message as it arrives, then ACKs it.
// Send SIGINT (Ctrl+C) to stop.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/83codes/octar/internal/protocol"
)

func main() {
	addr := flag.String("addr", "localhost:7000", "broker TCP address")
	user := flag.String("user", "admin", "username")
	pass := flag.String("pass", "admin", "password")
	ns := flag.String("ns", "main", "namespace")
	queue := flag.String("queue", "process_user", "queue name")
	group := flag.String("group", "group-1", "group key")
	failRate := flag.Int("fail-rate", 0, "simulate failures: NACK every N-th message (0 = never)")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	// ── Authenticate ──────────────────────────────────────────────────────────
	if err := enc.WriteConnect(protocol.ConnectFrame{
		Username:  *user,
		Password:  *pass,
		Namespace: *ns,
	}); err != nil {
		log.Fatalf("write CONNECT: %v", err)
	}

	ft, frame, err := dec.ReadFrame()
	if err != nil {
		log.Fatalf("read response: %v", err)
	}
	switch ft {
	case protocol.FrameConnectOK:
		ok := frame.(protocol.ConnectOKFrame)
		fmt.Printf("connected  session=%s\n", ok.SessionID)
	case protocol.FrameConnectErr:
		e := frame.(protocol.ConnectErrFrame)
		log.Fatalf("auth failed: %s", e.Reason)
	default:
		log.Fatalf("unexpected frame: 0x%02x", byte(ft))
	}

	// ── Subscribe ─────────────────────────────────────────────────────────────
	if err := enc.WriteSubscribe(protocol.SubscribeFrame{
		Queue: *queue,
		Group: *group,
	}); err != nil {
		log.Fatalf("write SUBSCRIBE: %v", err)
	}
	fmt.Printf("subscribed queue=%s group=%s — waiting for messages...\n", *queue, *group)

	// Handle Ctrl+C gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nstopping subscriber")
		conn.Close()
		os.Exit(0)
	}()

	// ── Consume loop ──────────────────────────────────────────────────────────
	received := 0
	for {
		ft, frame, err := dec.ReadFrame()
		if err != nil {
			log.Printf("connection closed: %v", err)
			return
		}

		switch ft {
		case protocol.FrameMessage:
			msg := frame.(protocol.MessageFrame)
			received++
			fmt.Printf("received   id=%-16s attempt=%d payload=%q\n",
				msg.MsgID, msg.Attempts, string(msg.Payload))

			// Simulate failure when -fail-rate N is set
			if *failRate > 0 && received%*failRate == 0 {
				fmt.Printf("           -> NACKing (simulated failure)\n")
				enc.WriteNACK(protocol.NACKFrame{
					MsgID:  msg.MsgID,
					Queue:  msg.Queue,
					Group:  msg.Group,
					Reason: "simulated processing failure",
				})
			} else {
				enc.WriteACK(protocol.ACKFrame{
					MsgID: msg.MsgID,
					Queue: msg.Queue,
					Group: msg.Group,
				})
			}

		case protocol.FrameHeartbeat:
			enc.WriteHeartbeat()

		case protocol.FrameError:
			e := frame.(protocol.ErrorFrame)
			log.Printf("broker error %d: %s", e.Code, e.Message)
		}
	}
}
