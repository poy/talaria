package orchestrator_test

import (
	"github.com/apoydence/talaria/orchestrator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var (
		mockKvStore     *mockKvStore
		mockPartManager *mockPartitionManager
		key             string
		clientAddr      string
		orch            *orchestrator.Orchestrator
	)

	BeforeEach(func() {
		key = "some-key"
		clientAddr = "some-addr"
		mockKvStore = newMockKvStore()
		mockPartManager = newMockPartitionManager()
		orch = orchestrator.New(clientAddr, mockPartManager, mockKvStore)
	})

	Context("FetchLeader", func() {
		It("Returns an already elected leader", func() {
			expectedLeader := "some-leader"
			mockKvStore.fetchLeaderTx <- expectedLeader
			mockKvStore.fetchLeaderOk <- true

			leaderUri, local := orch.FetchLeader(key)

			Expect(leaderUri).To(Equal(expectedLeader))
			Expect(local).To(BeFalse())
			Expect(mockKvStore.fetchLeaderRx).To(Receive(Equal(key)))
		})

		It("Starts an election for a new partition and waits for results", func() {
			expectedLeader := "some-leader"
			results := make(chan string, 0)
			mockKvStore.fetchLeaderTx <- ""
			mockKvStore.fetchLeaderOk <- false

			go func() {
				leader, _ := orch.FetchLeader(key)
				results <- leader
			}()

			Eventually(mockKvStore.listenNameCh).Should(Receive(Equal(key)))
			Eventually(mockKvStore.leaderCallbackCh).Should(Receive())
			Eventually(mockKvStore.announceCh).Should(Receive(Equal(key)))
			mockKvStore.fetchLeaderTx <- expectedLeader

			Eventually(results).Should(Receive(Equal(expectedLeader)))
		})
	})

	Context("Participate in election", func() {
		It("Listens for election announcements and adds channel on victory", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- key
			mockKvStore.acquireTx <- true
			Eventually(mockKvStore.acquireRx).Should(Receive(Equal(key)))
			Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
		})

		It("Listens for election announcements and does not add channel on loss", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- key
			mockKvStore.acquireTx <- false
			Eventually(mockKvStore.acquireRx).Should(Receive(Equal(key)))
			Consistently(mockPartManager.addCh).ShouldNot(Receive())
		})
	})

})
