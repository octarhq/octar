// Package snapshot provides binary serialization for queue state recovery.
// 
// Snapshot format (all big-endian):
//
//   - Header (32 bytes)
//     - magic: 4 bytes "FSNP"
//     - version: 2 bytes
//     - segment_id: 8 bytes (WAL segment at time of snapshot)
//     - wal_seq: 8 bytes (last WAL seq included in snapshot)
//     - timestamp: 8 bytes (unix nanos)
//     - flags: 2 bytes (reserved)
//
//   - Groups section
//     - group_count: 4 bytes
//     - for each group:
//       - key_len: 2 bytes
//       - key: variable
//       - parallelism: 4 bytes
//       - quantum: 4 bytes
//       - pending_count: 4 bytes
//       - processing_count: 4 bytes
//       - delayed_count: 4 bytes
//       - is_scheduled: 1 byte
//       - wake_at: 8 bytes
//
//   - Messages section
//     - message_count: 4 bytes
//     - for each message:
//       - id_len: 2 bytes
//       - id: variable
//       - group_key_len: 2 bytes
//       - group_key: variable
//       - payload_len: 4 bytes
//       - payload: variable
//       - state: 1 byte
//       - attempts: 4 bytes
//       - created_at: 8 bytes
//       - scheduled_at: 8 bytes
//       - last_error_len: 2 bytes
//       - last_error: variable
package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	magic   = "FSNP"
	version = 1
)

type Snapshot struct {
	SegmentID uint64
	WALSeq    uint64
	Timestamp int64

	Groups []GroupSnapshot
}

type GroupSnapshot struct {
	Key         string
	Parallelism int32
	Quantum     int32
	Messages    []MessageSnapshot
}

type MessageSnapshot struct {
	ID          string
	Payload     []byte
	State       int8
	Attempts    int32
	CreatedAt   int64
	ScheduledAt int64
	LastError   string
}

type SnapshotWriter struct {
	w io.Writer
}

func NewWriter(w io.Writer) *SnapshotWriter {
	return &SnapshotWriter{w: w}
}

func (sw *SnapshotWriter) Write(snap *Snapshot) error {
	header := make([]byte, 32)
	copy(header[0:4], magic)
	binary.BigEndian.PutUint16(header[4:6], version)
	binary.BigEndian.PutUint64(header[6:14], snap.SegmentID)
	binary.BigEndian.PutUint64(header[14:22], snap.WALSeq)
	binary.BigEndian.PutUint64(header[22:30], uint64(snap.Timestamp))
	binary.BigEndian.PutUint16(header[30:32], 0)

	if _, err := sw.w.Write(header); err != nil {
		return fmt.Errorf("snapshot: write header: %w", err)
	}

	if err := binary.Write(sw.w, binary.BigEndian, uint32(len(snap.Groups))); err != nil {
		return fmt.Errorf("snapshot: write group count: %w", err)
	}

	for _, g := range snap.Groups {
		if err := sw.writeGroup(g); err != nil {
			return err
		}
	}

	return nil
}

func (sw *SnapshotWriter) writeGroup(g GroupSnapshot) error {
	if err := binary.Write(sw.w, binary.BigEndian, uint16(len(g.Key))); err != nil {
		return fmt.Errorf("snapshot: write group key len: %w", err)
	}
	if _, err := sw.w.Write([]byte(g.Key)); err != nil {
		return fmt.Errorf("snapshot: write group key: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, g.Parallelism); err != nil {
		return fmt.Errorf("snapshot: write parallelism: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, g.Quantum); err != nil {
		return fmt.Errorf("snapshot: write quantum: %w", err)
	}

	if err := binary.Write(sw.w, binary.BigEndian, uint32(len(g.Messages))); err != nil {
		return fmt.Errorf("snapshot: write message count: %w", err)
	}
	for _, m := range g.Messages {
		if err := sw.writeMessage(m); err != nil {
			return err
		}
	}

	return nil
}

func (sw *SnapshotWriter) writeMessage(m MessageSnapshot) error {
	if err := binary.Write(sw.w, binary.BigEndian, uint16(len(m.ID))); err != nil {
		return fmt.Errorf("snapshot: write msg id len: %w", err)
	}
	if _, err := sw.w.Write([]byte(m.ID)); err != nil {
		return fmt.Errorf("snapshot: write msg id: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, uint32(len(m.Payload))); err != nil {
		return fmt.Errorf("snapshot: write payload len: %w", err)
	}
	if _, err := sw.w.Write(m.Payload); err != nil {
		return fmt.Errorf("snapshot: write payload: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, m.State); err != nil {
		return fmt.Errorf("snapshot: write state: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, m.Attempts); err != nil {
		return fmt.Errorf("snapshot: write attempts: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, m.CreatedAt); err != nil {
		return fmt.Errorf("snapshot: write created at: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, m.ScheduledAt); err != nil {
		return fmt.Errorf("snapshot: write scheduled at: %w", err)
	}
	if err := binary.Write(sw.w, binary.BigEndian, uint16(len(m.LastError))); err != nil {
		return fmt.Errorf("snapshot: write last error len: %w", err)
	}
	if _, err := sw.w.Write([]byte(m.LastError)); err != nil {
		return fmt.Errorf("snapshot: write last error: %w", err)
	}
	return nil
}

type SnapshotReader struct {
	r io.Reader
}

func NewReader(r io.Reader) *SnapshotReader {
	return &SnapshotReader{r: r}
}

func (sr *SnapshotReader) Read() (*Snapshot, error) {
	header := make([]byte, 32)
	if _, err := io.ReadFull(sr.r, header); err != nil {
		return nil, fmt.Errorf("snapshot: read header: %w", err)
	}

	if string(header[0:4]) != magic {
		return nil, fmt.Errorf("snapshot: invalid magic")
	}
	if binary.BigEndian.Uint16(header[4:6]) != version {
		return nil, fmt.Errorf("snapshot: unsupported version")
	}

	snap := &Snapshot{
		SegmentID: binary.BigEndian.Uint64(header[6:14]),
		WALSeq:    binary.BigEndian.Uint64(header[14:22]),
		Timestamp: int64(binary.BigEndian.Uint64(header[22:30])),
	}

	var groupCount uint32
	if err := binary.Read(sr.r, binary.BigEndian, &groupCount); err != nil {
		return nil, fmt.Errorf("snapshot: read group count: %w", err)
	}

	snap.Groups = make([]GroupSnapshot, groupCount)
	for i := uint32(0); i < groupCount; i++ {
		g, err := sr.readGroup()
		if err != nil {
			return nil, err
		}
		snap.Groups[i] = *g
	}

	return snap, nil
}

func (sr *SnapshotReader) readGroup() (*GroupSnapshot, error) {
	g := &GroupSnapshot{}

	var keyLen uint16
	if err := binary.Read(sr.r, binary.BigEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("snapshot: read key len: %w", err)
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(sr.r, key); err != nil {
		return nil, fmt.Errorf("snapshot: read key: %w", err)
	}
	g.Key = string(key)

	if err := binary.Read(sr.r, binary.BigEndian, &g.Parallelism); err != nil {
		return nil, fmt.Errorf("snapshot: read parallelism: %w", err)
	}
	if err := binary.Read(sr.r, binary.BigEndian, &g.Quantum); err != nil {
		return nil, fmt.Errorf("snapshot: read quantum: %w", err)
	}

	var msgCount uint32
	if err := binary.Read(sr.r, binary.BigEndian, &msgCount); err != nil {
		return nil, fmt.Errorf("snapshot: read message count: %w", err)
	}

	g.Messages = make([]MessageSnapshot, msgCount)
	for j := uint32(0); j < msgCount; j++ {
		m, err := sr.readMessage()
		if err != nil {
			return nil, err
		}
		g.Messages[j] = *m
	}

	return g, nil
}

func (sr *SnapshotReader) readMessage() (*MessageSnapshot, error) {
	m := &MessageSnapshot{}

	var idLen uint16
	if err := binary.Read(sr.r, binary.BigEndian, &idLen); err != nil {
		return nil, fmt.Errorf("snapshot: read id len: %w", err)
	}
	id := make([]byte, idLen)
	if _, err := io.ReadFull(sr.r, id); err != nil {
		return nil, fmt.Errorf("snapshot: read id: %w", err)
	}
	m.ID = string(id)

	var payloadLen uint32
	if err := binary.Read(sr.r, binary.BigEndian, &payloadLen); err != nil {
		return nil, fmt.Errorf("snapshot: read payload len: %w", err)
	}
	m.Payload = make([]byte, payloadLen)
	if _, err := io.ReadFull(sr.r, m.Payload); err != nil {
		return nil, fmt.Errorf("snapshot: read payload: %w", err)
	}

	if err := binary.Read(sr.r, binary.BigEndian, &m.State); err != nil {
		return nil, fmt.Errorf("snapshot: read state: %w", err)
	}
	if err := binary.Read(sr.r, binary.BigEndian, &m.Attempts); err != nil {
		return nil, fmt.Errorf("snapshot: read attempts: %w", err)
	}
	if err := binary.Read(sr.r, binary.BigEndian, &m.CreatedAt); err != nil {
		return nil, fmt.Errorf("snapshot: read created at: %w", err)
	}
	if err := binary.Read(sr.r, binary.BigEndian, &m.ScheduledAt); err != nil {
		return nil, fmt.Errorf("snapshot: read scheduled at: %w", err)
	}

	var errLen uint16
	if err := binary.Read(sr.r, binary.BigEndian, &errLen); err != nil {
		return nil, fmt.Errorf("snapshot: read error len: %w", err)
	}
	errMsg := make([]byte, errLen)
	if _, err := io.ReadFull(sr.r, errMsg); err != nil {
		return nil, fmt.Errorf("snapshot: read error: %w", err)
	}
	m.LastError = string(errMsg)

	return m, nil
}

func FindLatestSnapshot(dir string) (string, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", 0, nil
	}

	var bestPath string
	var bestSeq uint64

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var seq uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.snap", &seq); err == nil {
			if seq > bestSeq {
				bestSeq = seq
				bestPath = filepath.Join(dir, e.Name())
			}
		}
	}

	return bestPath, bestSeq, nil
}