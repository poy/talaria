package router_test

import (
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/storage/intraserver/router"
	"github.com/coreos/etcd/raft/raftpb"
)

type TR struct {
	*testing.T
	r *router.Router
}

func TestRouterRoute(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		return TR{
			T: t,
			r: router.New(),
		}
	})

	o.Spec("it routes data to the correct receivers", func(t TR) {
		rx := t.r.Receiver("A")
		expectedMsg1 := raftpb.Message{To: 99}
		expectedMsg2 := raftpb.Message{To: 101}
		expectedMsg3 := raftpb.Message{To: 103}
		t.r.Route("A", []raftpb.Message{expectedMsg1})
		t.r.Route("B", []raftpb.Message{expectedMsg2})
		t.r.Route("A", []raftpb.Message{expectedMsg3})

		msg, err := rx()
		Expect(t, err == nil).To(BeTrue())
		Expect(t, msg).To(Equal(expectedMsg1))

		msg, err = rx()
		Expect(t, err == nil).To(BeTrue())
		Expect(t, msg).To(Equal(expectedMsg3))
	})
}
