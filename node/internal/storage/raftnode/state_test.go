//go:generate hel
package raftnode_test

import (
	"fmt"
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage/buffers/ringbuffer"
	"github.com/apoydence/talaria/node/internal/storage/raftnode"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
)

type TRI struct {
	*testing.T
	ringBuffer  *ringbuffer.RingBuffer
	raftStorage *raftnode.State
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
			raftStorage: raftnode.NewState(buffer),
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
	raftStorage *raftnode.State
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
			raftStorage: raftnode.NewState(b),
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
				t.raftStorage.Write(entry)
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
				t.raftStorage.Write(entry)
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
		raftStorage := raftnode.NewState(b)
		var entries []raftpb.Entry

		for i := 0; i < 8; i++ {
			entry := raftpb.Entry{
				Term:  uint64(i),
				Index: uint64(i),
			}
			entries = append(entries, entry)
			raftStorage.Write(entry)
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
		o.Spec("it returns term 0", func(t TRE) {
			term, err := t.raftStorage.Term(100)
			Expect(t, err == nil).To(BeTrue())
			Expect(t, term).To(Equal(uint64(0)))
		})
	})
}

func TestRaftifyLastIndex(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRE {
		b := ringbuffer.New(5)
		raftStorage := raftnode.NewState(b)

		return TRE{
			T:           t,
			ringBuffer:  b,
			raftStorage: raftStorage,
		}
	})

	o.Spec("it returns 0", func(t TRE) {
		index, err := t.raftStorage.LastIndex()
		Expect(t, err == nil).To(BeTrue())
		Expect(t, index).To(Equal(uint64(0)))
	})

	o.Group("data has been added", func() {
		o.BeforeEach(func(t TRE) TRE {
			var entries []raftpb.Entry

			for i := 0; i < 8; i++ {
				entry := raftpb.Entry{
					Term:  uint64(i),
					Index: uint64(i),
				}
				entries = append(entries, entry)
				t.raftStorage.Write(entry)
			}
			return t
		})

		o.Spec("it returns the last index", func(t TRE) {
			index, err := t.raftStorage.LastIndex()
			Expect(t, err == nil).To(BeTrue())
			Expect(t, index).To(Equal(uint64(7)))
		})
	})
}

func TestRaftifyFirstIndex(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRE {
		b := ringbuffer.New(5)
		raftStorage := raftnode.NewState(b)

		return TRE{
			T:           t,
			ringBuffer:  b,
			raftStorage: raftStorage,
		}
	})

	o.Spec("it returns the 1 for the index", func(t TRE) {
		index, err := t.raftStorage.FirstIndex()
		Expect(t, err == nil).To(BeTrue())
		Expect(t, index).To(Equal(uint64(1)))
	})

	o.Group("data has been added", func() {
		o.BeforeEach(func(t TRE) TRE {
			var entries []raftpb.Entry

			entry := raftpb.Entry{
				Term:  uint64(0),
				Index: uint64(0),
			}
			entries = append(entries, entry)
			t.raftStorage.Write(entry)
			return t
		})

		o.Spec("it returns the first index", func(t TRE) {
			index, err := t.raftStorage.FirstIndex()
			Expect(t, err == nil).To(BeTrue())
			Expect(t, index).To(Equal(uint64(1)))
		})
	})

	o.Group("buffer has wrapped", func() {
		o.BeforeEach(func(t TRE) TRE {
			var entries []raftpb.Entry

			for i := 0; i < 8; i++ {
				entry := raftpb.Entry{
					Term:  uint64(i),
					Index: uint64(i),
				}
				entries = append(entries, entry)
				t.raftStorage.Write(entry)
			}
			return t
		})

		o.Spec("it returns the first index", func(t TRE) {
			index, err := t.raftStorage.FirstIndex()
			Expect(t, err == nil).To(BeTrue())

			// This is the last index that hasn't been overwritten plus 1
			Expect(t, index).To(Equal(uint64(4)))
		})
	})
}

type TRS struct {
	*testing.T
	ringBuffer  *ringbuffer.RingBuffer
	raftStorage *raftnode.State
}

func TestRaftifySnapshot(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TRS {
		b := ringbuffer.New(5)
		raftStorage := raftnode.NewState(b)

		h := raftpb.HardState{Term: 99}
		raftStorage.HardState(h)

		return TRS{
			T:           t,
			ringBuffer:  b,
			raftStorage: raftStorage,
		}
	})

	o.Spec("it returns empty Snapshot", func(t TRS) {
		_, err := t.raftStorage.Snapshot()
		Expect(t, err == nil).To(BeTrue())
	})

	o.Group("when the ring buffer has rounded a few times", func() {
		o.BeforeEach(func(t TRS) TRS {
			for i := 0; i < 11; i++ {
				t.raftStorage.Write(raftpb.Entry{
					Term: uint64(i),
					Data: []byte(fmt.Sprintf("some-data-%d", i)),
				})
			}
			return t
		})

		o.Spec("it returns the last full set of data", func(t TRS) {
			snap, err := t.raftStorage.Snapshot()
			Expect(t, err == nil).To(BeTrue())

			Expect(t, snap.Metadata.Term).To(Equal(uint64(5)))

			snapData := new(intra.SnapshotData)
			err = proto.Unmarshal(snap.Data, snapData)
			Expect(t, err == nil).To(BeTrue())

			Expect(t, snapData.Entries).To(HaveLen(5))
			for i := 5; i < 10; i++ {
				Expect(t, snapData.Entries[i-5]).To(Equal(&intra.SnapshotEntry{
					Seq: uint64(i),
					Entry: &raftpb.Entry{
						Term: uint64(i),
						Data: []byte(fmt.Sprintf("some-data-%d", i)),
					}}))
			}
		})

		o.Group("when ConfState is invoked", func() {
			o.Spec("it returns a Snapshot with the ConfState", func(t TRS) {
				cs := raftpb.ConfState{
					Nodes: []uint64{1, 2, 3},
				}
				t.raftStorage.ConfState(cs)

				snap, err := t.raftStorage.Snapshot()
				Expect(t, err == nil).To(BeTrue())
				Expect(t, snap.Metadata.ConfState).To(Equal(cs))
			})
		})
	})
}
