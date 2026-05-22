package broker

import (
	"sync"
	"sync/atomic"

	"github.com/83codes/octar/internal/server"
)

type subKey struct{ ns, queue, group string }

type connsSnap = []*server.Connection

type subList struct {
	mu     sync.Mutex
	conns  atomic.Pointer[connsSnap]
	cursor atomic.Uint64
}

func (l *subList) add(conn *server.Connection) {
	l.mu.Lock()
	p := l.conns.Load()
	var cur connsSnap
	if p != nil {
		cur = *p
	}
	next := make(connsSnap, len(cur)+1)
	copy(next, cur)
	next[len(cur)] = conn
	l.conns.Store(&next)
	l.mu.Unlock()
}

func (l *subList) remove(conn *server.Connection) (empty bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.conns.Load()
	if p == nil {
		return true
	}
	cur := *p
	for i, c := range cur {
		if c == conn {
			next := make(connsSnap, len(cur)-1)
			copy(next, cur[:i])
			copy(next[i:], cur[i+1:])
			l.conns.Store(&next)
			return len(next) == 0
		}
	}
	return len(cur) == 0
}

func (l *subList) next() *server.Connection {
	p := l.conns.Load()
	if p == nil {
		return nil
	}
	conns := *p
	n := uint64(len(conns))
	if n == 0 {
		return nil
	}
	return conns[(l.cursor.Add(1)-1)%n]
}

func (l *subList) hasAny() bool {
	p := l.conns.Load()
	return p != nil && len(*p) > 0
}

type subRegistry struct {
	m        sync.Map
	connMu   sync.Mutex
	connSubs map[*server.Connection][]subKey
}

func newSubRegistry() *subRegistry {
	return &subRegistry{
		connSubs: make(map[*server.Connection][]subKey),
	}
}

func (r *subRegistry) add(ns, q, group string, conn *server.Connection) {
	key := subKey{ns, q, group}
	actual, _ := r.m.LoadOrStore(key, &subList{})
	actual.(*subList).add(conn)

	r.connMu.Lock()
	r.connSubs[conn] = append(r.connSubs[conn], key)
	r.connMu.Unlock()
}

func (r *subRegistry) has(ns, q, group string) bool {
	v, ok := r.m.Load(subKey{ns, q, group})
	if !ok {
		return false
	}
	return v.(*subList).hasAny()
}

func (r *subRegistry) remove(conn *server.Connection) {
	r.connMu.Lock()
	keys := r.connSubs[conn]
	delete(r.connSubs, conn)
	r.connMu.Unlock()

	for _, key := range keys {
		v, ok := r.m.Load(key)
		if !ok {
			continue
		}
		if v.(*subList).remove(conn) {
			r.m.Delete(key)
		}
	}
}

func (r *subRegistry) next(ns, q, group string) *server.Connection {
	v, ok := r.m.Load(subKey{ns, q, group})
	if !ok {
		return nil
	}
	return v.(*subList).next()
}
