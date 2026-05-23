package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/octarhq/octar/internal/storage/snapshot"
)

// readBufPool reduces allocations during WAL recovery by reusing the byte buffer
// across sequential ReadEvent calls. Each recovery goroutine gets its own buffer.
// Capacity grows to the largest field across all events; subsequent events reuse it.
var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096)
		return &b
	},
}

// ErrCorruptedRecord is returned when a WAL record fails its CRC check.
var ErrCorruptedRecord = errors.New("corrupted WAL record: CRC mismatch")

// ReadEvent decodes and CRC-validates one WAL record from reader.
// Uses streaming CRC to avoid the crcData allocation (previously a copy of the
// entire record) and a pooled buffer for field strings.
// Returns io.EOF when the segment is exhausted (caller should treat as success).
func ReadEvent(reader *bufio.Reader) (Event, error) {
	var header [21]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Event{}, err
	}

	seq := binary.BigEndian.Uint64(header[0:8])
	eventType := EventType(header[8])
	timestamp := int64(binary.BigEndian.Uint64(header[9:17]))

	// Streaming CRC — zero allocation for the data copy.
	crc := crc32.Update(0, crcTable, header[:])

	fields := make([]string, 4)
	bufp := readBufPool.Get().(*[]byte)
	buf := *bufp
	var fieldLenBuf [2]byte
	for i := range fields {
		if _, err := io.ReadFull(reader, fieldLenBuf[:]); err != nil {
			readBufPool.Put(bufp)
			return Event{}, err
		}
		fieldLen := binary.BigEndian.Uint16(fieldLenBuf[:])
		crc = crc32.Update(crc, crcTable, fieldLenBuf[:])

		if cap(buf) < int(fieldLen) {
			buf = make([]byte, fieldLen)
		}
		data := buf[:fieldLen]
		if _, err := io.ReadFull(reader, data); err != nil {
			readBufPool.Put(bufp)
			return Event{}, err
		}
		crc = crc32.Update(crc, crcTable, data)
		fields[i] = string(data)
	}
	*bufp = buf
	readBufPool.Put(bufp)

	var payloadLenBuf [4]byte
	if _, err := io.ReadFull(reader, payloadLenBuf[:]); err != nil {
		return Event{}, err
	}
	payloadLen := binary.BigEndian.Uint32(payloadLenBuf[:])
	crc = crc32.Update(crc, crcTable, payloadLenBuf[:])

	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return Event{}, err
		}
		crc = crc32.Update(crc, crcTable, payload)
	}

	var footer [4]byte
	if _, err := io.ReadFull(reader, footer[:]); err != nil {
		return Event{}, err
	}

	expectedCRC := binary.BigEndian.Uint32(footer[:])
	if expectedCRC != crc {
		return Event{}, fmt.Errorf("%w: seq=%d", ErrCorruptedRecord, seq)
	}

	return Event{
		Type:      eventType,
		Namespace: fields[0],
		Queue:     fields[1],
		Group:     fields[2],
		MsgID:     fields[3],
		Payload:   payload,
		Seq:       seq,
		Timestamp: timestamp,
	}, nil
}

// LoadSnapshot finds and loads the most recent snapshot for a queue directory.
// Returns (segmentID, walSeq, nil) on success, or (0, 0, nil) if no snapshot exists.
func (q *QueueWAL) LoadSnapshot() (uint64, uint64, error) {
	snapPath, _, err := snapshot.FindLatestSnapshot(q.dir)
	if err != nil {
		return 0, 0, err
	}
	if snapPath == "" {
		q.logger.Info("no snapshot found, starting from scratch")
		return 0, 0, nil
	}

	f, err := os.Open(snapPath)
	if err != nil {
		return 0, 0, fmt.Errorf("snapshot: open: %w", err)
	}
	defer f.Close()

	snap, err := snapshot.NewReader(f).Read()
	if err != nil {
		return 0, 0, fmt.Errorf("snapshot: read: %w", err)
	}

	q.logger.Info("snapshot loaded",
		"path", snapPath,
		"segment", snap.SegmentID,
		"wal_seq", snap.WALSeq,
	)
	return snap.SegmentID, snap.WALSeq, nil
}
