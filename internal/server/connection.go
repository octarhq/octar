package server

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/83codes/octar/internal/auth"
	authidentity "github.com/83codes/octar/internal/auth/identity"
	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
	"github.com/83codes/octar/internal/protocol"
)

type Session struct {
	Username      string
	Namespace     string
	Authenticated bool
}

type ConnHandler func(conn *Connection)

type Connection struct {
	Conn     net.Conn
	Session  *Session
	Identity *authidentity.Identity
	enc      *protocol.Encoder
	dec      *protocol.Decoder
	outCh    chan any
	stopCh   chan struct{}
	closeOnce sync.Once
	credit   *credit

	readDeadline  time.Duration
	writeDeadline time.Duration
}

func newConnection(conn net.Conn, inflightCfg config.InflightConfig, readDeadline, writeDeadline time.Duration) *Connection {
	c := &Connection{
		Conn:    conn,
		Session: &Session{},
		enc:     protocol.NewEncoder(conn),
		dec:     protocol.NewDecoder(bufio.NewReader(conn)),
		outCh:   make(chan any, 1024),
		stopCh:  make(chan struct{}),
		credit:  newCredit(inflightCfg),

		readDeadline:  readDeadline,
		writeDeadline: writeDeadline,
	}
	go c.writerLoop()
	return c
}

func NewConnection(conn net.Conn, inflightCfg config.InflightConfig) *Connection {
	return newConnection(conn, inflightCfg, 0, 0)
}

func (c *Connection) AcquireCredit() bool { return c.credit.AcquireCredit() }
func (c *Connection) ReleaseCredit()       { c.credit.ReleaseCredit() }
func (c *Connection) Inflight() int32      { return c.credit.Inflight() }

func (c *Connection) RecordDispatch(msgID string) { c.credit.RecordDispatch(msgID) }
func (c *Connection) RecordACK(msgID string)      { c.credit.RecordACK(msgID) }
func (c *Connection) RecordNACK(msgID string)     { c.credit.RecordNACK(msgID) }
func (c *Connection) RecordWriteFail()            { c.credit.RecordWriteFail() }

func (c *Connection) writerLoop() {
	defer func() {
		if r := recover(); r != nil {
			slog.Default().Error("connection writer panicked", "panic", r)
		}
	}()

	for {
		select {
		case <-c.stopCh:
			return
		case msg := <-c.outCh:
			c.writeMsg(msg)

			drained := 0
		DrainLoop:
			for {
				select {
				case m := <-c.outCh:
					c.writeMsg(m)
					drained++
					if drained >= 100 {
						break DrainLoop
					}
				default:
					break DrainLoop
				}
			}
			c.enc.Flush()
		}
	}
}

func (c *Connection) writeMsg(msg any) {
	switch m := msg.(type) {
	case protocol.MessageFrame:
		c.enc.WriteMessage(m)
	case protocol.PublishOKFrame:
		c.enc.WritePublishOK(m)
	case protocol.ErrorFrame:
		c.enc.WriteError(m)
	case protocol.ConnectErrFrame:
		c.enc.WriteConnectErr(m)
	case protocol.ConnectOKFrame:
		c.enc.WriteConnectOK(m)
	case protocol.BackpressureFrame:
		c.enc.WriteBackpressure(m)
	case struct{}:
		c.enc.WriteHeartbeat()
	}
}

func (c *Connection) Authenticate(store *db.Store, authSvc *auth.Service) bool {
	ft, frame, err := c.dec.ReadFrame()
	if err != nil || ft != protocol.FrameConnect {
		c.outCh <- protocol.ConnectErrFrame{Reason: "expected CONNECT frame"}
		return false
	}

	f := frame.(protocol.ConnectFrame)
	remoteAddr := c.RemoteAddr().String()

	var identity *authidentity.Identity

	if f.Password != "" && f.Username != "" {
		id, err := authSvc.AuthenticateTCP(context.TODO(), remoteAddr, f.Username, f.Password, f.Namespace)
		if err == nil && id != nil {
			identity = id
		}
	}

	if identity == nil && f.APIKey != "" {
		id, err := authSvc.AuthenticateTCPWithKey(context.TODO(), remoteAddr, f.APIKey, f.Namespace)
		if err == nil && id != nil {
			identity = id
		}
	}

	if identity == nil && store.CheckPassword(f.Username, f.Password) {
		identity = &authidentity.Identity{
			SubjectID:   f.Username,
			SubjectType: authidentity.SubjectUser,
			AccountID:   f.Username,
			Namespace:   f.Namespace,
			Roles:       []string{"user"},
		}
	}

	if identity == nil {
		c.outCh <- protocol.ConnectErrFrame{Reason: "invalid credentials or namespace"}
		return false
	}

	if !identity.CanAccessNamespace(f.Namespace) {
		c.outCh <- protocol.ConnectErrFrame{Reason: "no access to namespace"}
		return false
	}

	c.Identity = identity
	c.Session.Username = identity.SubjectID
	c.Session.Namespace = f.Namespace
	c.Session.Authenticated = true

	c.outCh <- protocol.ConnectOKFrame{SessionID: c.RemoteAddr().String()}
	return true
}

func (c *Connection) ReadFrame() (protocol.FrameType, any, error) {
	if c.readDeadline > 0 {
		c.Conn.SetReadDeadline(time.Now().Add(c.readDeadline))
	}
	return c.dec.ReadFrame()
}

func (c *Connection) WriteMessage(f protocol.MessageFrame) error {
	select {
	case c.outCh <- f:
		return nil
	case <-c.stopCh:
		return net.ErrClosed
	default:
		return fmt.Errorf("connection buffer full")
	}
}

func (c *Connection) Flush() error { return nil }

func (c *Connection) WritePublishOK(f protocol.PublishOKFrame) error {
	select {
	case c.outCh <- f:
	case <-c.stopCh:
	}
	return nil
}

func (c *Connection) WriteError(f protocol.ErrorFrame) error {
	select {
	case c.outCh <- f:
	case <-c.stopCh:
	}
	return nil
}

func (c *Connection) WriteBackpressure(f protocol.BackpressureFrame) error {
	select {
	case c.outCh <- f:
	case <-c.stopCh:
	default:
	}
	return nil
}

func (c *Connection) WriteHeartbeat() error {
	select {
	case c.outCh <- struct{}{}:
	case <-c.stopCh:
	}
	return nil
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.stopCh)
	})
	return c.Conn.Close()
}
