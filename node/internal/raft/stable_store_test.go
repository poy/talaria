package raft_test

import (
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/raft"
	rafthashi "github.com/hashicorp/raft"
)

type TSS struct {
	*testing.T
	store rafthashi.StableStore
}

func TestStableStore(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TSS {
		return TSS{
			T:     t,
			store: raft.NewStableStore(),
		}
	})

	o.Group("Set() & Get()", func() {
		o.Spec("it returns the set value via the key", func(t TSS) {
			err := t.store.Set([]byte("some-key"), []byte("some-value"))
			Expect(t, err == nil).To(BeTrue())

			value, err := t.store.Get([]byte("some-key"))
			Expect(t, err == nil).To(BeTrue())
			Expect(t, value).To(Equal([]byte("some-value")))
		})

		o.Spec("it returns an empty slice if the key is not found", func(t TSS) {
			value, err := t.store.Get([]byte("some-key"))
			Expect(t, err == nil).To(BeTrue())
			Expect(t, value).To(HaveLen(0))
		})
	})

	o.Group("SetUint64() & GetUint64()", func() {
		o.Spec("it returns the set value via the key", func(t TSS) {
			err := t.store.SetUint64([]byte("some-key"), 99)
			Expect(t, err == nil).To(BeTrue())

			value, err := t.store.GetUint64([]byte("some-key"))
			Expect(t, err == nil).To(BeTrue())
			Expect(t, value).To(Equal(uint64(99)))
		})

		o.Spec("it returns a 0 if the key is not found", func(t TSS) {
			value, err := t.store.GetUint64([]byte("some-key"))
			Expect(t, err == nil).To(BeTrue())
			Expect(t, value).To(Equal(uint64(0)))
		})
	})
}
