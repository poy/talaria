//go:generate hel

package iofetcher_test

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/talaria/node/internal/raft/iofetcher"
	"github.com/apoydence/talaria/pb/stored"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TIF struct {
	*testing.T
	fetcher         *iofetcher.IOFetcher
	mockRaftCluster *mockRaftCluster
	createName      chan string
	createPeers     chan []string
}

func TestIOFetcher(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TIF {
		mockRaftCluster := newMockRaftCluster()
		createName := make(chan string, 100)
		createPeers := make(chan []string, 100)

		return TIF{
			T: t,
			fetcher: iofetcher.New(iofetcher.RaftClusterCreator(func(name string, peers []string) (iofetcher.RaftCluster, error) {
				createName <- name
				createPeers <- peers
				return mockRaftCluster, nil
			})),
			mockRaftCluster: mockRaftCluster,
			createName:      createName,
			createPeers:     createPeers,
		}
	})

	o.Group("Create", func() {
		o.Spec("it returns a BufferAlreadyCreated error for a redundant create", func(t TIF) {
			err := t.fetcher.Create("some-name", []string{"A", "B", "C"})
			Expect(t, err == nil).To(BeTrue())
			Expect(t, t.createName).To(
				Chain(Receive(), Equal("some-name")),
			)
			Expect(t, t.createPeers).To(
				Chain(Receive(), Equal([]string{"A", "B", "C"})),
			)

			err = t.fetcher.Create("some-name", []string{"A", "B", "C"})
			Expect(t, err).To(Equal(iofetcher.BufferAlreadyCreated))
		})
	})

	o.Group("ReadOnly", func() {
		o.Group("when the buffer is not created", func() {
			o.Spec("it returns a BufferNotCreated error", func(t TIF) {
				err := t.fetcher.ReadOnly("some-name")
				Expect(t, err).To(Equal(iofetcher.BufferNotCreated))
			})
		})

		o.Group("when buffer is created", func() {
			o.BeforeEach(func(t TIF) TIF {
				err := t.fetcher.Create("some-name", []string{"A", "B", "C"})
				Expect(t, err == nil).To(BeTrue())
				return t
			})

			o.Spec("it writes a ReadOnly message to the buffer", func(t TIF) {
				close(t.mockRaftCluster.WriteOutput.Ret0)
				t.fetcher.ReadOnly("some-name")

				var data stored.Data
				Expect(t, t.mockRaftCluster.WriteInput.Data).To(ViaPolling(
					Chain(Receive(), Fetch(&data)),
				))

				Expect(t, data.Type).To(Equal(stored.Data_ReadOnly))
			})
		})
	})

	o.Group("FetchWriter", func() {
		o.Group("when the buffer is not created", func() {
			o.Spec("it returns a BufferNotCreated error", func(t TIF) {
				_, err := t.fetcher.FetchWriter("some-name")
				Expect(t, err).To(Equal(iofetcher.BufferNotCreated))
			})
		})

		o.Group("when buffer is created", func() {
			o.BeforeEach(func(t TIF) TIF {
				err := t.fetcher.Create("some-name", []string{"A", "B", "C"})
				Expect(t, err == nil).To(BeTrue())
				return t
			})

			o.Spec("it returns a writer", func(t TIF) {
				writer, err := t.fetcher.FetchWriter("some-name")
				Expect(t, err == nil).To(BeTrue())
				Expect(t, writer != nil).To(BeTrue())
			})

			o.Spec("it resturns the same writer for the same name", func(t TIF) {
				writerA, err := t.fetcher.FetchWriter("some-name")
				Expect(t, err == nil).To(BeTrue())

				writerB, err := t.fetcher.FetchWriter("some-name")
				Expect(t, err == nil).To(BeTrue())

				Expect(t, writerA).To(Equal(writerB))
			})
		})
	})

	o.Group("FetchReader", func() {
		o.Group("when the buffer is not created", func() {
			o.Spec("it returns a BufferNotCreated error", func(t TIF) {
				_, err := t.fetcher.FetchReader("some-name")
				Expect(t, err).To(Equal(iofetcher.BufferNotCreated))
			})
		})

		o.Group("when buffer is created", func() {
			o.BeforeEach(func(t TIF) TIF {
				err := t.fetcher.Create("some-name", []string{"A", "B", "C"})
				Expect(t, err == nil).To(BeTrue())
				return t
			})

			o.Spec("it returns a reader", func(t TIF) {
				reader, err := t.fetcher.FetchReader("some-name")
				Expect(t, err == nil).To(BeTrue())
				Expect(t, reader != nil).To(BeTrue())
			})

			o.Spec("it resturns the same reader for the same name", func(t TIF) {
				readerA, err := t.fetcher.FetchReader("some-name")
				Expect(t, err == nil).To(BeTrue())

				readerB, err := t.fetcher.FetchReader("some-name")
				Expect(t, err == nil).To(BeTrue())

				Expect(t, readerA).To(Equal(readerB))
			})
		})
	})

	o.Group("Leader", func() {
		o.Group("when the buffer is not created", func() {
			o.Spec("it returns a BufferNotCreated error", func(t TIF) {
				_, err := t.fetcher.Leader("some-name")
				Expect(t, err).To(Equal(iofetcher.BufferNotCreated))
			})
		})

		o.Group("when buffer is created", func() {
			o.BeforeEach(func(t TIF) TIF {
				err := t.fetcher.Create("some-name", []string{"A", "B", "C"})
				Expect(t, err == nil).To(BeTrue())

				t.mockRaftCluster.LeaderOutput.Ret0 <- "some-leader"
				return t
			})

			o.Spec("it returns the leader", func(t TIF) {
				leader, err := t.fetcher.Leader("some-name")
				Expect(t, err == nil).To(BeTrue())
				Expect(t, leader).To(Equal("some-leader"))
			})
		})
	})

	o.Group("SetExpectedPeers", func() {
		o.Group("when the buffer is not created", func() {
			o.Spec("it returns a BufferNotCreated error", func(t TIF) {
				err := t.fetcher.SetExpectedPeers("some-name", []string{"A", "B", "C"})
				Expect(t, err).To(Equal(iofetcher.BufferNotCreated))
			})
		})

		o.Group("when buffer is created", func() {
			o.BeforeEach(func(t TIF) TIF {
				err := t.fetcher.Create("some-name", []string{"A", "B", "C"})
				Expect(t, err == nil).To(BeTrue())
				return t
			})

			o.Spec("it sets the expected peers", func(t TIF) {
				err := t.fetcher.SetExpectedPeers("some-name", []string{"A", "B", "C"})
				Expect(t, err == nil).To(BeTrue())
				Expect(t, t.mockRaftCluster.SetExpectedPeersInput.Peers).To(
					Chain(Receive(), Equal([]string{"A", "B", "C"})),
				)
			})
		})
	})

	o.Group("Status", func() {
		o.BeforeEach(func(t TIF) TIF {
			err := t.fetcher.Create("some-name-A", []string{"A", "B", "C"})
			Expect(t, err == nil).To(BeTrue())
			t.mockRaftCluster.ExpectedPeersOutput.Ret0 <- []string{"A", "B", "C"}

			err = t.fetcher.Create("some-name-B", []string{"A", "B", "C"})
			Expect(t, err == nil).To(BeTrue())
			t.mockRaftCluster.ExpectedPeersOutput.Ret0 <- []string{"A", "B", "C"}

			err = t.fetcher.Create("some-name-C", []string{"A", "B", "C"})
			Expect(t, err == nil).To(BeTrue())
			t.mockRaftCluster.ExpectedPeersOutput.Ret0 <- []string{"A", "B", "C"}
			return t
		})

		o.Spec("it returns a map with all the cluster names and their peers", func(t TIF) {
			status := t.fetcher.Status()

			Expect(t, status).To(HaveLen(3))
			Expect(t, status["some-name-A"]).To(Equal([]string{"A", "B", "C"}))
			Expect(t, status["some-name-B"]).To(Equal([]string{"A", "B", "C"}))
			Expect(t, status["some-name-C"]).To(Equal([]string{"A", "B", "C"}))
		})

	})

}
