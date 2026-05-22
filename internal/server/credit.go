package server

import (
	"sync/atomic"

	"github.com/83codes/octar/internal/config"
)

type credit struct {
	inflight    atomic.Int32
	maxInflight int32
}

func newCredit(cfg config.InflightConfig) *credit {
	return &credit{maxInflight: cfg.MaxInflight}
}

func (c *credit) AcquireCredit() bool {
	for {
		cur := c.inflight.Load()
		if cur >= c.maxInflight {
			return false
		}
		if c.inflight.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (c *credit) ReleaseCredit() { c.inflight.Add(-1) }
func (c *credit) Inflight() int32 { return c.inflight.Load() }

func (c *credit) RecordDispatch(_ string) {}
func (c *credit) RecordACK(_ string)      {}
func (c *credit) RecordNACK(_ string)     {}
func (c *credit) RecordWriteFail()        {}
