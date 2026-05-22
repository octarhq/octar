package queue

// delayedHeap is a min-heap of messages ordered by ScheduledAt.
//
// Used by group to hold retry messages waiting for their backoff delay.
// heap.Push / heap.Pop are O(log n). The heap is tiny in practice (bounded by
// max_attempts × inflight), so this is effectively O(1) amortised.
//
// Tie-breaking by message ID guarantees deterministic ordering across restarts,
// which matters for exactly-once semantics in future versions.
type delayedHeap []*Message

func (h delayedHeap) Len() int { return len(h) }

func (h delayedHeap) Less(i, j int) bool {
	if h[i].ScheduledAt.Equal(h[j].ScheduledAt) {
		return h[i].ID < h[j].ID // deterministic tie-break
	}
	return h[i].ScheduledAt.Before(h[j].ScheduledAt)
}

func (h delayedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *delayedHeap) Push(x any) { *h = append(*h, x.(*Message)) }

func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // release GC reference
	*h = old[:n-1]
	return item
}
