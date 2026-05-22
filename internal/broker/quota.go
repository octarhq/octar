package broker

import (
	"sync/atomic"
)

type brokerQuota struct {
	inflight atomic.Int64
	max      int64
}

func newBrokerQuota(max int64) *brokerQuota {
	return &brokerQuota{max: max}
}

func (q *brokerQuota) TryAcquire() bool {
	if q.max <= 0 {
		return true
	}
	for {
		cur := q.inflight.Load()
		if cur >= q.max {
			return false
		}
		if q.inflight.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (q *brokerQuota) Release() {
	if q.max > 0 {
		q.inflight.Add(-1)
	}
}

func (q *brokerQuota) Inflight() int64 { return q.inflight.Load() }
