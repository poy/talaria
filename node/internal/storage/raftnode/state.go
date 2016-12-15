package raftnode

import (
	"io"
	"log"
	"sync"
	"sync/atomic"

	"github.com/apoydence/talaria/node/internal/storage/buffers/ringbuffer"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
)

type State struct {
	mu           sync.Mutex
	hardState    raftpb.HardState
	confState    raftpb.ConfState
	lastSnapshot *raftpb.Snapshot

	writeCount uint64

	Buffer *ringbuffer.RingBuffer
}

func NewState(b *ringbuffer.RingBuffer) *State {
	return &State{
		Buffer:       b,
		lastSnapshot: &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Term: 1}},
	}
}

func (s *State) HardState(h raftpb.HardState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = h
}

func (s *State) ConfState(c raftpb.ConfState) {
	s.mu.Lock()
	s.confState = c
	s.mu.Unlock()

	s.createSnapshot()
}

func (s *State) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return s.hardState, s.confState, nil
}

func (s *State) Write(data raftpb.Entry) (uint64, error) {
	defer func() {
		if atomic.AddUint64(&s.writeCount, 1)%uint64(s.Buffer.Size) == 0 {
			s.createSnapshot()
		}
	}()

	return s.Buffer.Write(data)
}

func (s *State) ReadAt(index uint64) (raftpb.Entry, uint64, error) {
	return s.Buffer.ReadAt(index)
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *State) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if s.Buffer.LastIndex() == ringbuffer.NoData {
		return nil, raft.ErrUnavailable
	}

	var totalBytes uint64
	var entries []raftpb.Entry
	for i := lo; i < hi; i++ {
		entry, seq, err := s.Buffer.ReadAt(i)
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

		entries = append(entries, entry)
	}

	return entries, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (s *State) Term(i uint64) (uint64, error) {
	entry, seq, err := s.Buffer.ReadAt(i)
	if err != nil {
		log.Printf("Error reading from buffer for Term() (i=%d): %s", i, err)
		return 0, nil
	}

	if seq > i {
		return 0, raft.ErrCompacted
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (s *State) LastIndex() (uint64, error) {
	idx := s.Buffer.LastIndex()
	if idx == ringbuffer.NoData {
		return 0, nil
	}

	return idx, nil
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (s *State) FirstIndex() (uint64, error) {
	idx := s.Buffer.LastIndex()
	if idx == ringbuffer.NoData || idx < uint64(s.Buffer.Size) {
		return 1, nil
	}

	return idx - uint64(s.Buffer.Size) + 2, nil
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (s *State) Snapshot() (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.lastSnapshot, nil
}

func (s *State) createSnapshot() {
	log.Printf("Creating snapshot...")
	defer log.Printf("Done creating snapshot.")

	var snapshotData intra.SnapshotData
	var term uint64
	for i := 0; i < s.Buffer.Size; i++ {
		entry, seq, err := s.Buffer.ReadAt(uint64(i))

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Panic(err)
		}

		if i == 0 {
			term = entry.Term
		}

		snapshotData.Entries = append(snapshotData.Entries, &intra.SnapshotEntry{
			Seq:   seq,
			Entry: &entry,
		})
	}

	data, err := proto.Marshal(&snapshotData)
	if err != nil {
		log.Panic(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSnapshot = &raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			ConfState: s.confState,
			Term:      term,
		},
	}
}
