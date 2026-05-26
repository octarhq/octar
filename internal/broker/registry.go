package broker

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/octarhq/octar/internal/server"
)

// ── Glob matching ─────────────────────────────────────────────────────────────

// isGlobPattern reports whether s contains glob metacharacters.
func isGlobPattern(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

// matchGlob reports whether pattern matches name.
//
//   - '*' matches any sequence of characters, including '/' and empty string.
//   - '?' matches any single character.
//   - All other characters are matched literally.
//
// Examples:
//
//	matchGlob("*",         "tenant-123")     → true
//	matchGlob("tenant-*",  "tenant-123")     → true
//	matchGlob("tenant-*",  "other-123")      → false
//	matchGlob("*-prod",    "app-prod")       → true
//	matchGlob("region/*",  "region/us-east") → true
func matchGlob(pattern, name string) bool {
	return matchGlobRec(pattern, name)
}

func matchGlobRec(pattern, name string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Collapse consecutive stars.
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true // trailing '*' matches everything remaining
			}
			// Try matching pattern at every suffix of name.
			for i := 0; i <= len(name); i++ {
				if matchGlobRec(pattern, name[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(name) == 0 {
				return false
			}
			pattern = pattern[1:]
			name = name[1:]
		default:
			if len(name) == 0 || pattern[0] != name[0] {
				return false
			}
			pattern = pattern[1:]
			name = name[1:]
		}
	}
	return len(name) == 0
}

// ── Subscription primitives ───────────────────────────────────────────────────

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

// ── Registry ──────────────────────────────────────────────────────────────────

// subKeyEntry tracks a subscription key together with whether it is a wildcard.
// Used to clean up both maps on connection close.
type subKeyEntry struct {
	key      subKey
	wildcard bool
}

// subRegistry maintains two separate maps:
//
//   - exact:    subKey → *subList  for connections that subscribed to a specific group.
//   - wildcard: subKey → *subList  for connections that subscribed with a glob pattern
//     (e.g. "*", "tenant-*", "region/?s-*").
//
// Look-up order in next() / has():
//  1. Exact match in O(1) — the common, hot path.
//  2. Linear scan of wildcard entries — expected to be tiny (< 10) in practice.
//
// Thread safety: both sync.Maps are goroutine-safe.
// connSubs is protected by connMu.
type subRegistry struct {
	exact    sync.Map // subKey → *subList
	wildcard sync.Map // subKey → *subList  (group contains glob chars)

	connMu   sync.Mutex
	connSubs map[*server.Connection][]subKeyEntry
}

func newSubRegistry() *subRegistry {
	return &subRegistry{
		connSubs: make(map[*server.Connection][]subKeyEntry),
	}
}

// add registers conn as a subscriber for (ns, queue, group).
// If group contains glob metacharacters the subscription is stored in the
// wildcard map; otherwise it goes into the exact map.
func (r *subRegistry) add(ns, q, group string, conn *server.Connection) {
	key := subKey{ns, q, group}
	entry := subKeyEntry{key: key, wildcard: isGlobPattern(group)}

	if entry.wildcard {
		actual, _ := r.wildcard.LoadOrStore(key, &subList{})
		actual.(*subList).add(conn)
	} else {
		actual, _ := r.exact.LoadOrStore(key, &subList{})
		actual.(*subList).add(conn)
	}

	r.connMu.Lock()
	r.connSubs[conn] = append(r.connSubs[conn], entry)
	r.connMu.Unlock()
}

// has returns true if there is at least one active subscriber that would
// receive a message published to (ns, queue, group).
//
// Checks exact match first (O(1)), then wildcard patterns (O(wildcards)).
func (r *subRegistry) has(ns, q, group string) bool {
	// 1. Exact match — hot path.
	if v, ok := r.exact.Load(subKey{ns, q, group}); ok {
		if v.(*subList).hasAny() {
			return true
		}
	}
	// 2. Wildcard scan — cold path, list is small.
	found := false
	r.wildcard.Range(func(k, v any) bool {
		key := k.(subKey)
		if key.ns == ns && key.queue == q && matchGlob(key.group, group) {
			if v.(*subList).hasAny() {
				found = true
				return false // stop ranging
			}
		}
		return true
	})
	return found
}

// remove deregisters conn from all subscriptions it holds, cleaning up both
// the exact and wildcard maps.
func (r *subRegistry) remove(conn *server.Connection) {
	r.connMu.Lock()
	entries := r.connSubs[conn]
	delete(r.connSubs, conn)
	r.connMu.Unlock()

	for _, e := range entries {
		if e.wildcard {
			if v, ok := r.wildcard.Load(e.key); ok {
				if v.(*subList).remove(conn) {
					r.wildcard.Delete(e.key)
				}
			}
		} else {
			if v, ok := r.exact.Load(e.key); ok {
				if v.(*subList).remove(conn) {
					r.exact.Delete(e.key)
				}
			}
		}
	}
}

// next returns the next available connection for delivering a message published
// to (ns, queue, group), using round-robin within each matching subscriber set.
//
// Priority:
//  1. Exact match — specific group subscribers always take precedence.
//  2. First matching wildcard pattern — first-registered pattern wins.
//
// Returns nil if no subscriber is available.
func (r *subRegistry) next(ns, q, group string) *server.Connection {
	// 1. Exact match — O(1), hot path.
	if v, ok := r.exact.Load(subKey{ns, q, group}); ok {
		if conn := v.(*subList).next(); conn != nil {
			return conn
		}
	}
	// 2. Wildcard scan — O(wildcards), cold path.
	var found *server.Connection
	r.wildcard.Range(func(k, v any) bool {
		key := k.(subKey)
		if key.ns == ns && key.queue == q && matchGlob(key.group, group) {
			if conn := v.(*subList).next(); conn != nil {
				found = conn
				return false // stop ranging
			}
		}
		return true
	})
	return found
}
