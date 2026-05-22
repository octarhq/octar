package queue

import (
	"sync"
)

var messageBatchPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate a reasonable capacity for typical batches (e.g., 64)
		b := make([]*Message, 0, 64)
		return &b
	},
}

func getMessageBatch() *[]*Message  { return messageBatchPool.Get().(*[]*Message) }
func GetMessageBatch() *[]*Message  { return getMessageBatch() }

func putMessageBatch(b *[]*Message) {
	for i := range *b {
		(*b)[i] = nil
	}
	*b = (*b)[:0]
	messageBatchPool.Put(b)
}
func PutMessageBatch(b *[]*Message) { putMessageBatch(b) }
