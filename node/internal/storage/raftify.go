package storage

import (
	"log"
	"sync"

	"github.com/apoydence/talaria/node/internal/storage/buffers/ringbuffer"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

type Raftifier struct {
	mu        sync.Mutex
	hardState raftpb.HardState
	confState raftpb.ConfState

	Buffer *ringbuffer.RingBuffer
}

func Raftify(b *ringbuffer.RingBuffer) *Raftifier {
	return &Raftifier{
		Buffer: b,
	}
}

func (r *Raftifier) HardState(h raftpb.HardState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hardState = h
}

func (r *Raftifier) ConfState(c raftpb.ConfState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.confState = c
}

func (r *Raftifier) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return r.hardState, r.confState, nil
}

func (r *Raftifier) WriteTo(data *raftpb.Entry) (uint64, error) {
	return r.Buffer.WriteTo(data)
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (r *Raftifier) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if r.Buffer.LastIndex() == ringbuffer.NoData {
		return nil, raft.ErrUnavailable
	}

	var totalBytes uint64
	var entries []raftpb.Entry
	for i := lo; i < hi; i++ {
		entry, seq, err := r.Buffer.ReadAt(i)
		if err != nil {
			log.Panic("Error reading from buffer (lo=%d, hi=%d): %s", err, lo, hi)
		}

		if seq != i {
			return nil, raft.ErrCompacted
		}

		totalBytes += uint64(len(entry.Data))
		if totalBytes > maxSize {
			return entries, nil
		}

		entries = append(entries, *entry)
	}

	return entries, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (r *Raftifier) Term(i uint64) (uint64, error) {
	entry, seq, err := r.Buffer.ReadAt(i)
	if err != nil {
		log.Printf("Error reading from buffer for Term() (i=%d): %s", i, err)
		return 0, raft.ErrUnavailable
	}

	if seq > i {
		return 0, raft.ErrCompacted
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (r *Raftifier) LastIndex() (uint64, error) {
	return r.Buffer.LastIndex(), nil
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (r *Raftifier) FirstIndex() (uint64, error) {
	for {
		lastIdx := r.Buffer.LastIndex()
		_, seq, _ := r.Buffer.ReadAt(lastIdx + 1 - uint64(r.Buffer.Size))

		if seq < lastIdx {
			return seq, nil
		}
	}

	return 0, nil
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (r *Raftifier) Snapshot() (raftpb.Snapshot, error) {
	log.Panic("Not implemented")
	return raftpb.Snapshot{}, nil
}
