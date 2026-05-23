// publish is a minimal OCTAR publisher example.
//
// Before running, start the broker and declare the queue via REST:
//
//	go run ./cmd/broker
//	curl -s -X POST http://localhost:8080/auth/login \
//	     -H "Content-Type: application/json" \
//	     -d '{"username":"admin","password":"admin"}' | jq -r .token
//
//	TOKEN=<token above>
//	curl -s -X POST http://localhost:8080/queues \
//	     -H "Authorization: Bearer $TOKEN" \
//	     -H "Content-Type: application/json" \
//	     -d '{"name":"orders","namespace":"main"}'
//
// Then run:
//
//	go run ./examples/publish -count 10 -msg "order-{n}"
package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/octarhq/octar/internal/protocol"
)

func main() {
	addr := flag.String("addr", "localhost:7000", "broker TCP address")
	user := flag.String("user", "admin", "username")
	pass := flag.String("pass", "admin", "password")
	ns := flag.String("ns", "main", "namespace")
	queue := flag.String("queue", "process_user", "queue name")
	group := flag.String("group", "group-1", "group key")
	msg := flag.String("msg", "hello from octar", "message payload")
	count := flag.Int("count", 1, "number of messages to publish")
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

	// ── Publish ───────────────────────────────────────────────────────────────
	for i := 1; i <= *count; i++ {
		payload := fmt.Sprintf("%s [%d/%d]", *msg, i, *count)

		if err := enc.WritePublish(protocol.PublishFrame{
			Queue:   *queue,
			Group:   *group,
			Payload: []byte(payload),
		}); err != nil {
			log.Fatalf("write PUBLISH: %v", err)
		}

		ft, frame, err := dec.ReadFrame()
		if err != nil {
			log.Fatalf("read PUBLISH_OK: %v", err)
		}
		switch ft {
		case protocol.FramePublishOK:
			ok := frame.(protocol.PublishOKFrame)
			fmt.Printf("published  id=%-16s payload=%q\n", ok.MsgID, payload)
		case protocol.FrameError:
			e := frame.(protocol.ErrorFrame)
			log.Fatalf("broker error %d: %s", e.Code, e.Message)
		default:
			log.Fatalf("unexpected frame: 0x%02x", byte(ft))
		}
	}

	fmt.Printf("done — published %d message(s) to %s/%s\n", *count, *queue, *group)
}
