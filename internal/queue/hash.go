package queue

// HashKey returns a 32-bit FNV-1a hash of s.
// Used for consistent, allocation-free shard selection across the queue.
// FNV-1a (XOR-then-multiply) is chosen over FNV-1 for better avalanche
// characteristics on short strings like group keys.
func HashKey(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
