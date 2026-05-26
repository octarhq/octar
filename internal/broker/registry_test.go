package broker

import (
	"fmt"
	"testing"

	"github.com/octarhq/octar/internal/server"
)

// ── matchGlob unit tests ──────────────────────────────────────────────────────

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pattern string
		name    string
		want    bool
	}{
		// Exact (no glob chars) — should behave like ==
		{"group-1", "group-1", true},
		{"group-1", "group-2", false},
		{"", "", true},

		// Star wildcard
		{"*", "anything", true},
		{"*", "tenant-123", true},
		{"*", "region/us-east", true}, // '*' crosses '/'
		{"*", "", true},               // '*' matches empty

		// Prefix glob
		{"tenant-*", "tenant-123", true},
		{"tenant-*", "tenant-", true},
		{"tenant-*", "other-123", false},
		{"tenant-*", "TENANT-123", false}, // case-sensitive

		// Suffix glob
		{"*-prod", "app-prod", true},
		{"*-prod", "app-staging", false},
		{"*-prod", "prod", false},    // '-' is literal; '*' matches anything but '-prod' != 'prod'
		{"*prod", "prod", true},      // no dash — '*' matches empty prefix

		// Middle glob
		{"region/*-east", "region/us-east", true},
		{"region/*-east", "region/eu-east", true},
		{"region/*-east", "region/us-west", false},

		// Double star (collapses to single *)
		{"**", "anything", true},
		{"tenant-**", "tenant-123", true},

		// Question mark
		{"group-?", "group-1", true},
		{"group-?", "group-12", false},
		{"group-?", "group-", false},

		// Mixed
		{"tenant-?-*", "tenant-a-data", true},
		{"tenant-?-*", "tenant-ab-data", false},

		// No match
		{"abc", "xyz", false},
		{"a*c", "abc", true},
		{"a*c", "axyzc", true},
		{"a*c", "axyz", false},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("pattern=%q name=%q", tc.pattern, tc.name), func(t *testing.T) {
			got := matchGlob(tc.pattern, tc.name)
			if got != tc.want {
				t.Errorf("matchGlob(%q, %q) = %v, want %v", tc.pattern, tc.name, got, tc.want)
			}
		})
	}
}

// ── isGlobPattern ─────────────────────────────────────────────────────────────

func TestIsGlobPattern(t *testing.T) {
	cases := []struct {
		s    string
		want bool
	}{
		{"group-1", false},
		{"tenant-123", false},
		{"*", true},
		{"tenant-*", true},
		{"group-?", true},
		{"group-[0-9]", true},
		{"", false},
	}
	for _, tc := range cases {
		if got := isGlobPattern(tc.s); got != tc.want {
			t.Errorf("isGlobPattern(%q) = %v, want %v", tc.s, got, tc.want)
		}
	}
}

// ── subRegistry tests ─────────────────────────────────────────────────────────
//
// server.Connection is a heavyweight struct that requires a real TCP connection.
// We test the registry via its public surface (add / has / next / remove) using
// nil *server.Connection pointers as stand-ins — subList stores them by pointer
// so nil is a valid (if non-functional) sentinel for identity checks.

// mkConn returns a unique *server.Connection pointer without allocating a real
// connection. We exploit the fact that *server.Connection is compared by address
// inside subList, so any unique non-nil pointer suffices.
func mkConn() *server.Connection {
	// Allocate a zero-value Connection on the heap so each call returns a
	// distinct address. We never call methods on it in registry tests.
	return new(server.Connection)
}

func TestRegistry_ExactMatch(t *testing.T) {
	r := newSubRegistry()
	conn := mkConn()

	r.add("ns", "queue", "group-1", conn)

	if !r.has("ns", "queue", "group-1") {
		t.Fatal("has() should return true for registered exact key")
	}
	if r.has("ns", "queue", "group-2") {
		t.Fatal("has() should return false for unregistered group")
	}
	if got := r.next("ns", "queue", "group-1"); got != conn {
		t.Fatalf("next() = %p, want %p", got, conn)
	}
	if got := r.next("ns", "queue", "group-2"); got != nil {
		t.Fatal("next() should return nil for unknown group")
	}
}

func TestRegistry_WildcardStar_MatchesAll(t *testing.T) {
	r := newSubRegistry()
	conn := mkConn()

	r.add("ns", "queue", "*", conn)

	groups := []string{"group-1", "tenant-abc", "region/us-east", "anything"}
	for _, g := range groups {
		if !r.has("ns", "queue", g) {
			t.Errorf("has() should return true for group %q with wildcard '*'", g)
		}
		if got := r.next("ns", "queue", g); got != conn {
			t.Errorf("next() for group %q returned %p, want %p", g, got, conn)
		}
	}
}

func TestRegistry_WildcardPrefix(t *testing.T) {
	r := newSubRegistry()
	conn := mkConn()

	r.add("ns", "orders", "tenant-*", conn)

	// Should match.
	for _, g := range []string{"tenant-123", "tenant-abc", "tenant-"} {
		if !r.has("ns", "orders", g) {
			t.Errorf("has() should match %q with 'tenant-*'", g)
		}
	}
	// Should NOT match.
	for _, g := range []string{"other-123", "TENANT-123", "group-1"} {
		if r.has("ns", "orders", g) {
			t.Errorf("has() should NOT match %q with 'tenant-*'", g)
		}
	}
}

func TestRegistry_ExactTakesPriorityOverWildcard(t *testing.T) {
	r := newSubRegistry()
	exactConn := mkConn()
	wildcardConn := mkConn()

	r.add("ns", "q", "group-1", exactConn)
	r.add("ns", "q", "*", wildcardConn)

	// next() must return exactConn, not wildcardConn.
	got := r.next("ns", "q", "group-1")
	if got != exactConn {
		t.Errorf("next() = %p (wildcard), want %p (exact)", got, exactConn)
	}

	// For a group only the wildcard covers, return wildcardConn.
	got = r.next("ns", "q", "group-2")
	if got != wildcardConn {
		t.Errorf("next() for unregistered group = %p, want wildcardConn %p", got, wildcardConn)
	}
}

func TestRegistry_RoundRobin_Exact(t *testing.T) {
	r := newSubRegistry()
	c1, c2, c3 := mkConn(), mkConn(), mkConn()

	r.add("ns", "q", "g", c1)
	r.add("ns", "q", "g", c2)
	r.add("ns", "q", "g", c3)

	seen := map[*server.Connection]int{}
	for i := 0; i < 9; i++ {
		seen[r.next("ns", "q", "g")]++
	}
	for _, c := range []*server.Connection{c1, c2, c3} {
		if seen[c] != 3 {
			t.Errorf("expected 3 dispatches to each conn, got %v", seen)
		}
	}
}

func TestRegistry_RoundRobin_Wildcard(t *testing.T) {
	r := newSubRegistry()
	c1, c2 := mkConn(), mkConn()

	// Two connections both subscribe with "*".
	r.add("ns", "q", "*", c1)
	r.add("ns", "q", "*", c2)

	seen := map[*server.Connection]int{}
	// All requests are for the same group — round-robin within the wildcard subList.
	for i := 0; i < 4; i++ {
		seen[r.next("ns", "q", "anything")]++
	}
	if seen[c1] != 2 || seen[c2] != 2 {
		t.Errorf("expected 2 dispatches each, got c1=%d c2=%d", seen[c1], seen[c2])
	}
}

func TestRegistry_Remove_Exact(t *testing.T) {
	r := newSubRegistry()
	conn := mkConn()

	r.add("ns", "q", "group-1", conn)
	r.remove(conn)

	if r.has("ns", "q", "group-1") {
		t.Fatal("has() should return false after remove")
	}
	if r.next("ns", "q", "group-1") != nil {
		t.Fatal("next() should return nil after remove")
	}
}

func TestRegistry_Remove_Wildcard(t *testing.T) {
	r := newSubRegistry()
	conn := mkConn()

	r.add("ns", "q", "*", conn)
	r.remove(conn)

	if r.has("ns", "q", "any-group") {
		t.Fatal("has() should return false after wildcard conn is removed")
	}
}

func TestRegistry_Remove_OneOfMany(t *testing.T) {
	r := newSubRegistry()
	c1, c2 := mkConn(), mkConn()

	r.add("ns", "q", "g", c1)
	r.add("ns", "q", "g", c2)
	r.remove(c1)

	// c2 should still be reachable.
	if !r.has("ns", "q", "g") {
		t.Fatal("has() should still return true with one subscriber remaining")
	}
	for i := 0; i < 4; i++ {
		got := r.next("ns", "q", "g")
		if got != c2 {
			t.Fatalf("next() after removing c1 returned %p, want c2 %p", got, c2)
		}
	}
}

func TestRegistry_MultipleWildcardPatterns(t *testing.T) {
	r := newSubRegistry()
	tenantConn := mkConn()
	regionConn := mkConn()

	r.add("ns", "q", "tenant-*", tenantConn)
	r.add("ns", "q", "region-*", regionConn)

	if !r.has("ns", "q", "tenant-42") {
		t.Error("tenant-42 should match tenant-*")
	}
	if !r.has("ns", "q", "region-eu") {
		t.Error("region-eu should match region-*")
	}
	if r.has("ns", "q", "other-group") {
		t.Error("other-group should match neither pattern")
	}
}

func TestRegistry_NamespaceIsolation(t *testing.T) {
	r := newSubRegistry()
	conn := mkConn()

	r.add("ns-a", "q", "*", conn)

	if !r.has("ns-a", "q", "group-1") {
		t.Fatal("should match in ns-a")
	}
	if r.has("ns-b", "q", "group-1") {
		t.Fatal("should NOT match in ns-b — different namespace")
	}
}

func TestRegistry_QueueIsolation(t *testing.T) {
	r := newSubRegistry()
	conn := mkConn()

	r.add("ns", "orders", "*", conn)

	if !r.has("ns", "orders", "group-1") {
		t.Fatal("should match queue=orders")
	}
	if r.has("ns", "invoices", "group-1") {
		t.Fatal("should NOT match queue=invoices — different queue")
	}
}
