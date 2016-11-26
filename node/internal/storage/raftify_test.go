//go:generate hel
package storage_test

import (
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage"
	"github.com/apoydence/talaria/node/internal/storage/buffers/ringbuffer"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

type TRI struct {
	*testing.T
	ringBuffer  *ringbuffer.RingBuffer
	raftStorage *storage.Raftifier
	hardState   raftpb.HardState
	confState   raftpb.ConfState
}

func TestRaftifyInitialState(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRI {
		buffer := ringbuffer.New(5)
		return TRI{
			T:           t,
			ringBuffer:  buffer,
			raftStorage: storage.Raftify(buffer),
		}
	})

	o.Group("when state has not been set", func() {
		o.Spec("it returns the last known state", func(t TRI) {
			h, c, e := t.raftStorage.InitialState()
			Expect(t, e == nil).To(BeTrue())
			Expect(t, h).To(Equal(raftpb.HardState{}))
			Expect(t, c).To(Equal(raftpb.ConfState{}))
		})
	})

	o.Group("when state has been set", func() {
		o.BeforeEach(func(t TRI) TRI {
			h := raftpb.HardState{Term: 99}
			c := raftpb.ConfState{Nodes: []uint64{99}}

			t.raftStorage.HardState(h)
			t.raftStorage.ConfState(c)

			t.hardState = h
			t.confState = c
			return t
		})

		o.Spec("it returns the last known state", func(t TRI) {
			h, c, e := t.raftStorage.InitialState()
			Expect(t, e == nil).To(BeTrue())
			Expect(t, h).To(Equal(t.hardState))
			Expect(t, c).To(Equal(t.confState))
		})
	})

}

type TRE struct {
	*testing.T
	ringBuffer  *ringbuffer.RingBuffer
	raftStorage *storage.Raftifier
	entries     []raftpb.Entry
}

func TestRaftifyEntries(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRE {
		b := ringbuffer.New(5)
		return TRE{
			T:           t,
			ringBuffer:  b,
			raftStorage: storage.Raftify(b),
		}
	})

	o.Group("when no data has been added", func() {
		o.Spec("it returns ErrUnavailable", func(t TRE) {
			_, err := t.raftStorage.Entries(0, 5, 10)
			Expect(t, err).To(Equal(raft.ErrUnavailable))
		})
	})

	o.Group("when data has been added", func() {
		o.BeforeEach(func(t TRE) TRE {
			for i := 0; i < 3; i++ {
				entry := raftpb.Entry{
					Term:  uint64(i),
					Data:  []byte("some-data"),
					Index: uint64(i),
				}
				t.entries = append(t.entries, entry)
				t.raftStorage.WriteTo(&entry)
			}
			return t
		})

		o.Spec("it returns the added entries", func(t TRE) {
			es, err := t.raftStorage.Entries(1, 3, 100)
			Expect(t, err == nil).To(BeTrue())
			Expect(t, es).To(Equal(t.entries[1:3]))
		})

		o.Spec("it truncates the data to maxSize", func(t TRE) {
			es, err := t.raftStorage.Entries(1, 3, 10)
			Expect(t, err == nil).To(BeTrue())
			Expect(t, es).To(Equal(t.entries[1:2]))
		})
	})

	o.Group("when the ring buffer has lapped", func() {
		o.BeforeEach(func(t TRE) TRE {
			for i := 0; i < 8; i++ {
				entry := raftpb.Entry{
					Term:  uint64(i),
					Index: uint64(i),
				}
				t.entries = append(t.entries, entry)
				t.raftStorage.WriteTo(&entry)
			}
			return t
		})

		o.Spec("it returns ErrCompacted", func(t TRE) {
			_, err := t.raftStorage.Entries(1, 3, 10)
			Expect(t, err).To(Equal(raft.ErrCompacted))
		})
	})

}

func TestRaftifyTerm(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRE {
		b := ringbuffer.New(5)
		raftStorage := storage.Raftify(b)
		var entries []raftpb.Entry

		for i := 0; i < 8; i++ {
			entry := raftpb.Entry{
				Term:  uint64(i),
				Index: uint64(i),
			}
			entries = append(entries, entry)
			raftStorage.WriteTo(&entry)
		}

		return TRE{
			T:           t,
			ringBuffer:  b,
			raftStorage: raftStorage,
		}
	})

	o.Spec("returns the Term of the given index", func(t TRE) {
		term, err := t.raftStorage.Term(5)
		Expect(t, err == nil).To(BeTrue())
		Expect(t, term).To(Equal(uint64(5)))
	})

	o.Group("when index is too low", func() {
		o.Spec("it returns ErrCompacted", func(t TRE) {
			_, err := t.raftStorage.Term(0)
			Expect(t, err).To(Equal(raft.ErrCompacted))
		})
	})

	o.Group("when index is too high", func() {
		o.Spec("it returns ErrCompacted", func(t TRE) {
			_, err := t.raftStorage.Term(100)
			Expect(t, err).To(Equal(raft.ErrUnavailable))
		})
	})
}

func TestRaftifyLastIndex(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRE {
		b := ringbuffer.New(5)
		raftStorage := storage.Raftify(b)
		var entries []raftpb.Entry

		for i := 0; i < 8; i++ {
			entry := raftpb.Entry{
				Term:  uint64(i),
				Index: uint64(i),
			}
			entries = append(entries, entry)
			raftStorage.WriteTo(&entry)
		}

		return TRE{
			T:           t,
			ringBuffer:  b,
			raftStorage: raftStorage,
		}
	})

	o.Spec("it returns the last index", func(t TRE) {
		index, err := t.raftStorage.LastIndex()
		Expect(t, err == nil).To(BeTrue())
		Expect(t, index).To(Equal(uint64(7)))
	})
}

func TestRaftifyFirstIndex(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRE {
		b := ringbuffer.New(5)
		raftStorage := storage.Raftify(b)
		var entries []raftpb.Entry

		for i := 0; i < 8; i++ {
			entry := raftpb.Entry{
				Term:  uint64(i),
				Index: uint64(i),
			}
			entries = append(entries, entry)
			raftStorage.WriteTo(&entry)
		}

		return TRE{
			T:           t,
			ringBuffer:  b,
			raftStorage: raftStorage,
		}
	})

	o.Spec("it returns the first index", func(t TRE) {
		index, err := t.raftStorage.FirstIndex()
		Expect(t, err == nil).To(BeTrue())

		// This is the last index that hasn't been overwritten
		Expect(t, index).To(Equal(uint64(3)))
	})
}
