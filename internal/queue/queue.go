package queue

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/83codes/octar/internal/xtime"
)

// GroupStats contains runtime statistics for a single group.
type GroupStats struct {
	Key        string `json:"key"`
	Pending    int    `json:"pending"`
	Processing int    `json:"processing"`
}

// QueueStats is a snapshot of a queue's current state.
type QueueStats struct {
	Name      string       `json:"name"`
	Namespace string       `json:"namespace"`
	Groups    []GroupStats `json:"groups"`
}

const queueShardCount = 32 // Must be power of 2

type queueShard struct {
	mu     sync.Mutex
	groups map[string]*group
}

// ── Sorted group key index ────────────────────────────────────────────────────

// groupKeyIndex is an append-only sorted index of active group keys.
//
// Why a separate structure instead of sorting on demand?
// PageGroupStats previously did: collect all N keys → sort O(N log N) → slice page.
// With this index, pagination is O(log N) binary-search to find the cursor position,
// then O(K) to read the page. The insertion cost is O(N) due to the slice shift, but
// it happens only once per new group (group creation is rare; reads are frequent).
//
// Thread safety: its own RWMutex, independent of shard locks. Never hold both.
type groupKeyIndex struct {
	mu   sync.RWMutex
	keys []string // always in lexicographic order
}

// add inserts key into the sorted index. Idempotent — safe to call concurrently
// for the same key (only one insertion will happen).
func (idx *groupKeyIndex) add(key string) {
	idx.mu.Lock()
	i := sort.SearchStrings(idx.keys, key)
	if i < len(idx.keys) && idx.keys[i] == key {
		idx.mu.Unlock()
		return // already present
	}
	// Insert at position i. O(N-i) copy — acceptable because group creation is rare.
	idx.keys = append(idx.keys, "")
	copy(idx.keys[i+1:], idx.keys[i:])
	idx.keys[i] = key
	idx.mu.Unlock()
}

// page returns at most limit keys starting after the cursor, plus the next cursor.
// O(log N) to locate cursor + O(K) to copy the page.
func (idx *groupKeyIndex) page(after string, limit int) (keys []string, nextCursor string) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	start := 0
	if after != "" {
		i := sort.SearchStrings(idx.keys, after)
		if i < len(idx.keys) && idx.keys[i] == after {
			start = i + 1 // exclusive: start after the cursor key
		} else {
			start = i // cursor not found: resume from next greater key
		}
	}

	end := start + limit
	if end > len(idx.keys) {
		end = len(idx.keys)
	}
	if start >= end {
		return nil, ""
	}

	page := make([]string, end-start)
	copy(page, idx.keys[start:end])

	if end < len(idx.keys) {
		nextCursor = idx.keys[end-1] // last key on this page becomes the cursor
	}
	return page, nextCursor
}

func (idx *groupKeyIndex) count() int {
	idx.mu.RLock()
	n := len(idx.keys)
	idx.mu.RUnlock()
	return n
}

// ── Config index ──────────────────────────────────────────────────────────────

// configIdx holds declared GroupConfigs split by kind:
//   - exact:     map lookup in O(1) — covers tenant-id, queue-name, any fixed string
//   - wildcards: a short slice of glob patterns (e.g. "tenant-*") scanned only when
//     no exact match exists. In practice there are very few wildcards.
//
// This replaces the previous []*GroupConfig slice which was O(N) on every Publish.
type configIdx struct {
	exact     map[string]*GroupConfig
	wildcards []*GroupConfig // insertion-ordered; first match wins
}

func newConfigIdx() configIdx {
	return configIdx{exact: make(map[string]*GroupConfig)}
}

// isGlobPattern reports whether key contains glob characters.
func isGlobPattern(key string) bool {
	return strings.ContainsAny(key, "*?[")
}

func (ci *configIdx) set(cfg GroupConfig) {
	c := cfg // copy so callers can reuse their struct
	if isGlobPattern(c.Key) {
		for i, w := range ci.wildcards {
			if w.Key == c.Key {
				ci.wildcards[i] = &c
				return
			}
		}
		ci.wildcards = append(ci.wildcards, &c)
	} else {
		ci.exact[c.Key] = &c
	}
}

func (ci *configIdx) get(key string) (*GroupConfig, bool) {
	if c, ok := ci.exact[key]; ok {
		return c, true
	}
	for _, c := range ci.wildcards {
		if c.Key == key {
			return c, true
		}
	}
	return nil, false
}

func (ci *configIdx) del(key string) bool {
	if _, ok := ci.exact[key]; ok {
		delete(ci.exact, key)
		return true
	}
	for i, c := range ci.wildcards {
		if c.Key == key {
			ci.wildcards = append(ci.wildcards[:i], ci.wildcards[i+1:]...)
			return true
		}
	}
	return false
}

// resolve returns the best config for key:
//   1. O(1) exact match
//   2. O(wildcards) first glob match — wildcards list is always tiny
//   3. nil (caller uses defaultGroupConfig)
func (ci *configIdx) resolve(key string) *GroupConfig {
	if c, ok := ci.exact[key]; ok {
		return c
	}
	for _, c := range ci.wildcards {
		if c.Matches(key) {
			out := *c
			out.Key = key // inherit wildcard config but stamp the real key
			return &out
		}
	}
	return nil
}

// list returns all declared configs (exact configs in sorted order, then wildcards).
func (ci *configIdx) list() []GroupConfig {
	out := make([]GroupConfig, 0, len(ci.exact)+len(ci.wildcards))
	// sort exact keys for stable output
	keys := make([]string, 0, len(ci.exact))
	for k := range ci.exact {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, *ci.exact[k])
	}
	for _, w := range ci.wildcards {
		out = append(out, *w)
	}
	return out
}

func (ci *configIdx) count() int { return len(ci.exact) + len(ci.wildcards) }

// ── Queue ─────────────────────────────────────────────────────────────────────

// Queue holds the in-memory state of a declared queue.
// It uses Lock Striping (Sharding) to ensure high concurrency across different groups.
type Queue struct {
	Name      string
	Namespace string

	configMu sync.RWMutex
	cfgIdx   configIdx // O(1) exact lookups; O(wildcards) fallback

	shards [queueShardCount]*queueShard

	// keyIdx keeps all active group keys in sorted order.
	// Maintained alongside the shards so that PageGroupStats is O(log N + K)
	// instead of O(N log N). Updated on every new group creation (rare),
	// never deleted (groups live for the lifetime of the queue).
	keyIdx groupKeyIndex
}

func NewQueue(name, namespace string) *Queue {
	q := &Queue{
		Name:      name,
		Namespace: namespace,
		cfgIdx:    newConfigIdx(),
	}
	for i := 0; i < queueShardCount; i++ {
		q.shards[i] = &queueShard{
			groups: make(map[string]*group),
		}
	}
	return q
}

// SetGroupConfig adds or replaces a group configuration (exact key or wildcard).
func (q *Queue) SetGroupConfig(cfg GroupConfig) {
	if cfg.Quantum <= 0 {
		cfg.Quantum = 1
	}

	q.configMu.Lock()
	q.cfgIdx.set(cfg)
	q.configMu.Unlock()

	// Propagate to any already-active groups in the shards.
	if !isGlobPattern(cfg.Key) {
		// Exact key: touch only the one shard that owns it.
		shardIdx := HashKey(cfg.Key) & (queueShardCount - 1)
		shard := q.shards[shardIdx]
		shard.mu.Lock()
		if g, ok := shard.groups[cfg.Key]; ok {
			g.cfg = cfg
			g.quantum = cfg.Quantum
		}
		shard.mu.Unlock()
	} else {
		// Wildcard: must walk all shards to find matching active groups.
		for i := 0; i < queueShardCount; i++ {
			shard := q.shards[i]
			shard.mu.Lock()
			for key, g := range shard.groups {
				if cfg.Matches(key) {
					updated := cfg
					updated.Key = key
					g.cfg = updated
					g.quantum = cfg.Quantum
				}
			}
			shard.mu.Unlock()
		}
	}
}

// DeleteGroupConfig removes a group configuration by key.
func (q *Queue) DeleteGroupConfig(key string) bool {
	q.configMu.Lock()
	defer q.configMu.Unlock()
	return q.cfgIdx.del(key)
}

// GetGroupConfig returns the configuration for an exact key match.
// Returns (cfg, true) if found, (GroupConfig{}, false) otherwise.
// Does NOT perform wildcard resolution — use this to inspect declared configs.
func (q *Queue) GetGroupConfig(key string) (GroupConfig, bool) {
	q.configMu.RLock()
	defer q.configMu.RUnlock()
	c, ok := q.cfgIdx.get(key)
	if !ok {
		return GroupConfig{}, false
	}
	return *c, true
}

// ListGroupConfigs returns all declared group configurations.
// For large deployments prefer PageGroupConfigs.
func (q *Queue) ListGroupConfigs() []GroupConfig {
	q.configMu.RLock()
	defer q.configMu.RUnlock()
	return q.cfgIdx.list()
}

// PageGroupConfigs returns at most limit declared group configs, starting after
// the given cursor (empty string = beginning). Returns the next cursor (empty =
// last page). Configs are returned in stable sorted order (wildcards always last).
//
// This is the scalable path for multi-tenant deployments with many configs.
func (q *Queue) PageGroupConfigs(after string, limit int) ([]GroupConfig, string) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	q.configMu.RLock()
	all := q.cfgIdx.list()
	q.configMu.RUnlock()

	start := 0
	if after != "" {
		for i, c := range all {
			if c.Key == after {
				start = i + 1
				break
			}
		}
	}

	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	page := all[start:end]

	nextCursor := ""
	if end < len(all) {
		nextCursor = all[end-1].Key
	}
	return page, nextCursor
}

// GroupCount returns the number of runtime groups currently active in this queue.
// O(1) — reads directly from the sorted key index.
func (q *Queue) GroupCount() int { return q.keyIdx.count() }

// ConfigCount returns the number of declared group configurations. O(1).
func (q *Queue) ConfigCount() int {
	q.configMu.RLock()
	defer q.configMu.RUnlock()
	return q.cfgIdx.count()
}

// PageGroupStats returns a page of runtime group statistics ordered by key.
//
// Complexity: O(log N + K) where N = total active groups, K = page size.
//
// How it works:
//  1. keyIdx.page() binary-searches the sorted index to find the cursor → O(log N)
//  2. For each key in the page, look up only its owning shard → O(K × 1)
//
// This is the fast path for per-tenant monitoring dashboards. For a single
// tenant, prefer GetGroupStats which is O(1).
func (q *Queue) PageGroupStats(after string, limit int) ([]GroupStats, string) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	// Step 1: get the page of keys from the sorted index — O(log N + K).
	keys, nextCursor := q.keyIdx.page(after, limit)
	if len(keys) == 0 {
		return nil, ""
	}

	// Step 2: for each key, read its stats from exactly one shard — O(K).
	stats := make([]GroupStats, 0, len(keys))
	for _, key := range keys {
		shardIdx := HashKey(key) & (queueShardCount - 1)
		shard := q.shards[shardIdx]
		shard.mu.Lock()
		if g, ok := shard.groups[key]; ok {
			stats = append(stats, GroupStats{
				Key:        key,
				Pending:    g.pendingCount(),
				Processing: g.processingCount(),
			})
		}
		shard.mu.Unlock()
	}

	return stats, nextCursor
}

// ExpiredLease identifies a message whose consumer lease has timed out.
// Returned by SweepExpiredLeases so the broker can write EventExpire to the WAL
// and re-activate the scheduler for the affected group.
type ExpiredLease struct {
	Namespace string
	QueueName string
	GroupKey  string
	MsgID     string
}

// SweepExpiredLeases scans all active groups for messages whose lease deadline
// (ScheduledAt) has passed — meaning the consumer did not ACK in time.
//
// For each expired lease it:
//  1. Removes the message from the group's processing map
//  2. Prepends it to the ready queue (so it is re-dispatched immediately)
//  3. Returns its identity so the broker can write EventExpire to the WAL
//
// Called by the broker's background lease sweeper once per second.
// O(active groups × avg inflight per group) — typically fast because most
// groups have zero or one inflight message at any given moment.
func (q *Queue) SweepExpiredLeases(now time.Time) []ExpiredLease {
	var expired []ExpiredLease
	for i := 0; i < queueShardCount; i++ {
		shard := q.shards[i]
		shard.mu.Lock()
		for _, g := range shard.groups {
			// Collect expired IDs first to avoid modifying the map while ranging.
			var expiredIDs []string
			for id, msg := range g.processing {
				if now.After(msg.ScheduledAt) {
					expiredIDs = append(expiredIDs, id)
				}
			}
			for _, id := range expiredIDs {
				g.returnToPending(id)
				expired = append(expired, ExpiredLease{
					Namespace: q.Namespace,
					QueueName: q.Name,
					GroupKey:  g.cfg.Key,
					MsgID:     id,
				})
			}
		}
		shard.mu.Unlock()
	}
	return expired
}

// GetGroupStats returns runtime statistics for a single group. O(1) — touches
// only the one shard that owns the group. Returns (stats, false) if the group
// has never received a message (not yet created in memory).
func (q *Queue) GetGroupStats(groupKey string) (GroupStats, bool) {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]
	shard.mu.Lock()
	g, ok := shard.groups[groupKey]
	if !ok {
		shard.mu.Unlock()
		return GroupStats{}, false
	}
	s := GroupStats{
		Key:        groupKey,
		Pending:    g.pendingCount(),
		Processing: g.processingCount(),
	}
	shard.mu.Unlock()
	return s, true
}

// PublishWithID adds a message using a caller-supplied ID instead of generating one.
// Used by the broker's onPublish handler (which pre-generates the ID for the WAL)
// and by the WAL replay path (which must restore the original IDs).
//
// Having WAL and RAM agree on the same ID is what makes ACK deduplication correct
// after a crash: a consumer that ACK'd message "abc" before the crash will not
// receive "abc" again after recovery because the ID is preserved in both places.
func (q *Queue) PublishWithID(groupKey, msgID string, payload []byte) (*Message, error) {
	now := xtime.Now()
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	g, ok := shard.groups[groupKey]
	if !ok {
		cfg := q.resolveConfig(groupKey)
		g = newGroup(cfg)
		shard.groups[groupKey] = g
	}
	// Depth limit check (B4).
	if g.cfg.MaxPending > 0 && g.pendingCount() >= g.cfg.MaxPending {
		shard.mu.Unlock()
		return nil, fmt.Errorf("group %q depth limit exceeded (%d)", groupKey, g.cfg.MaxPending)
	}
	msg := newMessageAt(q.Name, q.Namespace, groupKey, payload, now)
	msg.ID = msgID // override the generated ID with the caller-supplied one
	g.enqueue(msg)
	shard.mu.Unlock()

	if !ok {
		q.keyIdx.add(groupKey)
	}
	return msg, nil
}

// Publish adds a message to the queue for the given group.
func (q *Queue) Publish(groupKey string, payload []byte) (*Message, error) {
	now := xtime.Now()
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	g, ok := shard.groups[groupKey]
	if !ok {
		cfg := q.resolveConfig(groupKey)
		g = newGroup(cfg)
		shard.groups[groupKey] = g
	}
	// Depth limit check (B4).
	if g.cfg.MaxPending > 0 && g.pendingCount() >= g.cfg.MaxPending {
		shard.mu.Unlock()
		return nil, fmt.Errorf("group %q depth limit exceeded (%d)", groupKey, g.cfg.MaxPending)
	}
	msg := newMessageAt(q.Name, q.Namespace, groupKey, payload, now)
	g.enqueue(msg)
	shard.mu.Unlock()

	// Register new groups in the sorted index outside the shard lock — keeping
	// shard.mu as short as possible. keyIdx.add is idempotent; concurrent callers
	// for the same new key are safe (only one insertion wins).
	if !ok {
		q.keyIdx.add(groupKey)
	}

	return msg, nil
}

// ReturnToPending re-queues a dispatched message at the front of its group's pending queue.
func (q *Queue) ReturnToPending(groupKey, msgID string) {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	if g, ok := shard.groups[groupKey]; ok {
		g.returnToPending(msgID)
	}
	shard.mu.Unlock()
}

// Complete marks a dispatched message as successfully processed.
func (q *Queue) Complete(groupKey, msgID string) error {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return fmt.Errorf("group %q not found", groupKey)
	}
	if g.complete(msgID) == nil {
		return fmt.Errorf("message %q not found in processing", msgID)
	}
	return nil
}

// CompleteAndNext marks msgID as done and returns the next eligible message.
func (q *Queue) CompleteAndNext(groupKey, msgID string, now time.Time) *Message {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return nil
	}
	g.complete(msgID)
	g.promoteDelayed(now)
	return g.next(now)
}

// TryDispatchOne returns the next eligible message from the group.
func (q *Queue) TryDispatchOne(groupKey string, now time.Time) *Message {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return nil
	}
	g.promoteDelayed(now)
	return g.next(now)
}

// Fail handles a failed message: schedules retry or returns DLQ target queue name.
func (q *Queue) Fail(groupKey, msgID, errMsg string) (dlqQueue string, dlqMsg *Message, err error) {
	now := xtime.Now()
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return "", nil, fmt.Errorf("group %q not found", groupKey)
	}

	dlq := g.fail(msgID, errMsg, now)
	if dlq != nil && g.cfg.DLQ != nil {
		return g.cfg.DLQ.Queue, dlq, nil
	}
	return "", nil, nil
}

// FailAndNext handles a failed message and returns the next message to dispatch.
func (q *Queue) FailAndNext(groupKey, msgID, errMsg string, now time.Time) (dlqQueue string, dlqMsg *Message, next *Message) {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return "", nil, nil
	}

	dlq := g.fail(msgID, errMsg, now)
	if dlq != nil && g.cfg.DLQ != nil {
		dlqQueue = g.cfg.DLQ.Queue
		dlqMsg = dlq
	}

	g.promoteDelayed(now)
	next = g.next(now)
	return
}

// drainGroup drains ready work for a single group and returns any messages
// that should be dispatched immediately, plus the next delayed wakeup.
func (q *Queue) drainGroup(groupKey string, now time.Time) (*[]*Message, time.Time, bool, bool) {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return nil, time.Time{}, false, false
	}

	return g.drainRound(now)
}

// GetGroup safely retrieves the group state without holding the entire queue lock.
func (q *Queue) GetGroup(groupKey string) *group {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	g := shard.groups[groupKey]
	shard.mu.Unlock()
	return g
}

// Stats returns a point-in-time snapshot of the queue's group states.
// For large deployments, prefer PageGroupStats to avoid serialising all shards.
func (q *Queue) Stats() QueueStats {
	stats, _ := q.Snapshot()
	return stats
}

// Snapshot returns stats and group configs.
// NOTE: This acquires all 32 shard locks sequentially; it is O(active groups).
// Prefer PageGroupStats + PageGroupConfigs in high-group-count deployments.
func (q *Queue) Snapshot() (QueueStats, []GroupConfig) {
	stats := QueueStats{Name: q.Name, Namespace: q.Namespace}

	for i := 0; i < queueShardCount; i++ {
		shard := q.shards[i]
		shard.mu.Lock()
		for key, g := range shard.groups {
			stats.Groups = append(stats.Groups, GroupStats{
				Key:        key,
				Pending:    g.pendingCount(),
				Processing: g.processingCount(),
			})
		}
		shard.mu.Unlock()
	}

	cfgs := q.ListGroupConfigs()
	return stats, cfgs
}

// DrainGroup is the exported entry point for the scheduler package to drain
// a group without depending on the unexported group type.
func (q *Queue) DrainGroup(groupKey string, now time.Time) (*[]*Message, time.Time, bool, bool) {
	return q.drainGroup(groupKey, now)
}

// GetGroupToken returns a GroupToken for the given group key, or nil if the
// group does not exist. GroupToken exposes only the scheduling primitives
// (activation flag, wake timer), not the full group state.
func (q *Queue) GetGroupToken(groupKey string) *GroupToken {
	g := q.GetGroup(groupKey)
	if g == nil {
		return nil
	}
	return &GroupToken{g: g}
}

// ReplayLease moves a message from ready/urgent to processing during WAL replay,
// so that subsequent OnACK/OnNACK/OnExpire events find it in the processing map.
// Returns false if the message is not found (already completed, never published, etc).
func (q *Queue) ReplayLease(groupKey, msgID string, now time.Time) bool {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return false
	}

	// Search urgent queue first (LIFO — most recent backpressure)
	for i := g.urgentHead; i < len(g.urgent); i++ {
		if g.urgent[i] != nil && g.urgent[i].ID == msgID {
			msg := g.urgent[i]
			g.urgent[i] = nil
			msg.State = StateProcessing
			msg.ScheduledAt = now.Add(g.cfg.LeaseTimeout)
			g.processing[msg.ID] = msg
			return true
		}
	}

	// Search ready queue (FIFO — published messages that were never dispatched)
	for i := g.readyHead; i < len(g.ready); i++ {
		if g.ready[i] != nil && g.ready[i].ID == msgID {
			msg := g.ready[i]
			g.ready[i] = nil
			msg.State = StateProcessing
			msg.ScheduledAt = now.Add(g.cfg.LeaseTimeout)
			g.processing[msg.ID] = msg
			return true
		}
	}

	return false
}

// RemoveMessage removes a message from the group by ID, regardless of its
// current location (urgent, ready, or processing). Used during WAL replay to
// handle ACK events for messages published-and-acked between snapshot and crash.
// Returns true if the message was found and removed.
func (q *Queue) RemoveMessage(groupKey, msgID string) bool {
	shardIdx := HashKey(groupKey) & (queueShardCount - 1)
	shard := q.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	g, ok := shard.groups[groupKey]
	if !ok {
		return false
	}

	// Try urgent
	for i := g.urgentHead; i < len(g.urgent); i++ {
		if g.urgent[i] != nil && g.urgent[i].ID == msgID {
			g.urgent[i] = nil
			return true
		}
	}

	// Try ready
	for i := g.readyHead; i < len(g.ready); i++ {
		if g.ready[i] != nil && g.ready[i].ID == msgID {
			g.ready[i] = nil
			return true
		}
	}

	// Try processing
	if _, ok := g.processing[msgID]; ok {
		delete(g.processing, msgID)
		return true
	}

	return false
}

// GroupToken wraps the scheduling-relevant atomics of a group. It is the only
// surface the scheduler package needs from the queue package for group-level
// coordination — group message state stays fully encapsulated.
type GroupToken struct{ g *group }

func (t *GroupToken) TrySchedule() bool                { return t.g.isScheduled.CompareAndSwap(0, 1) }
func (t *GroupToken) Unschedule()                      { t.g.isScheduled.Store(0) }
func (t *GroupToken) LoadWake() int64                  { return t.g.wakeScheduledAt.Load() }
func (t *GroupToken) TrySetWake(curr, next int64) bool { return t.g.wakeScheduledAt.CompareAndSwap(curr, next) }
func (t *GroupToken) ClearWake(expected int64)         { t.g.wakeScheduledAt.CompareAndSwap(expected, 0) }

func queueKey(namespace, name string) string { return namespace + "/" + name }

// resolveConfig finds the best-matching GroupConfig for a group key.
// O(1) for exact match, O(wildcards) for glob fallback. Never O(all configs).
func (q *Queue) resolveConfig(key string) GroupConfig {
	q.configMu.RLock()
	c := q.cfgIdx.resolve(key)
	q.configMu.RUnlock()
	if c != nil {
		return *c
	}
	return defaultGroupConfig(key)
}

type QueueState struct {
	Name      string
	Namespace string
	Groups    map[string]GroupState
}

func (q *Queue) ExportState() QueueState {
	state := QueueState{
		Name:      q.Name,
		Namespace: q.Namespace,
		Groups:    make(map[string]GroupState),
	}

	for i := 0; i < queueShardCount; i++ {
		shard := q.shards[i]
		shard.mu.Lock()
		for key, g := range shard.groups {
			state.Groups[key] = g.ExportState()
		}
		shard.mu.Unlock()
	}

	return state
}

func (q *Queue) ImportState(state QueueState) {
	// Pass 1: restore all groups into their shards.
	keys := make([]string, 0, len(state.Groups))
	for _, gs := range state.Groups {
		shardIdx := HashKey(gs.Key) & (queueShardCount - 1)
		shard := q.shards[shardIdx]

		shard.mu.Lock()
		cfg := q.resolveConfig(gs.Key)
		cfg.Parallelism = gs.Parallelism
		cfg.Key = gs.Key
		g := newGroup(cfg)
		g.ImportState(gs)
		g.quantum = gs.Quantum
		shard.groups[gs.Key] = g
		shard.mu.Unlock()

		keys = append(keys, gs.Key)
	}

	// Pass 2: build the sorted key index in one shot — O(N log N) total.
	// The alternative of calling keyIdx.add() N times is O(N²) due to repeated
	// slice copies. For 100k groups that would be ~5 billion copy operations.
	sort.Strings(keys)
	q.keyIdx.mu.Lock()
	q.keyIdx.keys = keys
	q.keyIdx.mu.Unlock()
}
