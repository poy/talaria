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
		orch = orchestrator.New(clientAddr, 2, mockKvStore)
	})

	Describe("FetchLeader", func() {
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

			Eventually(mockKvStore.listenNameCh).Should(Receive(Equal(key + "~0")))
			Eventually(mockKvStore.leaderCallbackCh).Should(Receive())
			Eventually(mockKvStore.announceCh).Should(Receive(Equal(key + "~0")))
			mockKvStore.fetchLeaderTx <- expectedLeader

			Eventually(results).Should(Receive(Equal(expectedLeader)))
		})
	})

	Describe("ListenForReplicas", func() {
		It("Calls callback on updates", func() {
			expectedName := "some-name"
			var expectedReplica uint = 4
			expectedAddr := "http://some.uri"

			rxNameCh := make(chan string, 100)
			rxReplicaCh := make(chan uint, 100)
			rxAddrCh := make(chan string, 100)

			orch.ListenForReplicas(expectedName, func(name string, replica uint, addr string) {
				rxNameCh <- name
				rxReplicaCh <- replica
				rxAddrCh <- addr
			})

			By("waiting to get the callback")
			var callback func(string, string)
			Eventually(mockKvStore.listenNameCh).Should(Receive(Equal(expectedName)))
			Eventually(mockKvStore.leaderCallbackCh).Should(Receive(&callback))

			By("invoking the callback")
			callback(fmt.Sprintf("%s~%d", expectedName, expectedReplica), expectedAddr)
			Eventually(rxNameCh).Should(Receive(Equal(expectedName)))
			Eventually(rxReplicaCh).Should(Receive(Equal(expectedReplica)))
			Eventually(rxAddrCh).Should(Receive(Equal(expectedAddr)))
		})
	})

	Describe("ParticipateInElection", func() {
		BeforeEach(func() {
			orch.ParticipateInElection(mockPartManager)
		})

		It("listens for election announcements and adds to manager on victory", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())

			for i := 0; i < 3; i++ {
				mockKvStore.announceLeaderTx <- fmt.Sprintf("%s~%d", key, i)
				mockKvStore.acquireTx <- true
				mockPartManager.partCh <- true

				Eventually(mockKvStore.acquireRx).Should(Receive(Equal(fmt.Sprintf("%s~%d", key, i))))

				By("asking the participation")
				Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
				Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(i)))

				By("adding to the manager")
				Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
				Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(i)))

				if i < 2 {
					By("starting an election for the next index")
					Eventually(mockKvStore.announceCh).Should(Receive(Equal(fmt.Sprintf("%s~%d", key, i+1))))
				} else {
					By("not starting an election for the last index + 1")
					Consistently(mockKvStore.announceCh).ShouldNot(Receive())
				}
			}
		})

		It("does not participate in an election if told not to", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- key + "~1"
			mockPartManager.partCh <- false

			By("asking the participation")
			Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
			Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(1)))

			Consistently(mockKvStore.acquireRx).ShouldNot(Receive())
		})

		It("listens for election announcements and does not add to manager if lossed", func() {
			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- key + "~1"
			mockKvStore.acquireTx <- false
			mockPartManager.partCh <- true
			Eventually(mockKvStore.acquireRx).Should(Receive(Equal(fmt.Sprintf("%s~%d", key, 1))))

			By("asking the participation")
			Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
			Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(1)))

			By("not adding to the manager")
			Consistently(mockPartManager.addCh).ShouldNot(Receive())
		})
	})

})
