package orchestrator_test

import (
	"fmt"

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
		orch = orchestrator.New(clientAddr, 2, mockPartManager, mockKvStore)
	})

	Context("FetchLeader", func() {
		It("returns an already elected leader", func() {
			expectedLeader := "some-leader"
			mockKvStore.fetchLeaderTx <- expectedLeader
			mockKvStore.fetchLeaderOk <- true

			leaderUri, local := orch.FetchLeader(key)

			Expect(leaderUri).To(Equal(expectedLeader))
			Expect(local).To(BeFalse())
			Expect(mockKvStore.fetchLeaderRx).To(Receive(Equal(key)))
		})

		It("starts an election for a new partition and waits for results", func() {
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
			Eventually(mockKvStore.announceCh).Should(Receive(Equal(key + "~0")))
			mockKvStore.fetchLeaderTx <- expectedLeader

			Eventually(results).Should(Receive(Equal(expectedLeader)))
		})
	})

	Context("Participate in election", func() {
		It("listens for election announcements and adds to manager on victory", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())

			for i := 0; i < 3; i++ {
				mockKvStore.announceLeaderTx <- fmt.Sprintf("%s~%d", key, i)
				mockKvStore.acquireTx <- true
				mockPartManager.partCh <- true

				Eventually(mockKvStore.acquireRx).Should(Receive(Equal(key)))

				By("Asking the participation")
				Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
				Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(i)))

				By("Adding to the manager")
				Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
				Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(i)))

				if i < 2 {
					By("Starting an election for the next index")
					Eventually(mockKvStore.announceCh).Should(Receive(Equal(fmt.Sprintf("%s~%d", key, i+1))))
				} else {
					By("Not starting an election for the last index + 1")
					Consistently(mockKvStore.announceCh).ShouldNot(Receive())
				}
			}
		})

		It("does not participate in an election if told not to", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- key + "~1"
			mockPartManager.partCh <- false

			By("Asking the participation")
			Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
			Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(1)))

			Consistently(mockKvStore.acquireRx).ShouldNot(Receive())
		})

		It("listens for election announcements and does not add to manager if lossed", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- key + "~1"
			mockKvStore.acquireTx <- false
			mockPartManager.partCh <- true
			Eventually(mockKvStore.acquireRx).Should(Receive(Equal(key)))

			By("Asking the participation")
			Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
			Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(1)))

			By("Not adding to the manager")
			Consistently(mockPartManager.addCh).ShouldNot(Receive())
		})
	})

})
