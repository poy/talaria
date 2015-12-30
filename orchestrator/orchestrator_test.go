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

	Describe("FetchLeader()", func() {
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

	Describe("ParticipateInElection()", func() {

		var (
			expectedReplica int
		)

		JustBeforeEach(func() {
			close(mockPartManager.addResultCh)
			close(mockPartManager.addResultOkCh)
		})

		Context("wins election", func() {

			JustBeforeEach(func() {
				mockKvStore.announceLeaderTx <- fmt.Sprintf("%s~%d", key, expectedReplica)
				mockKvStore.acquireTx <- true
				mockPartManager.partCh <- true
			})

			Context("regardless of leader", func() {

				BeforeEach(func() {
					expectedReplica = 1
				})

				It("listens for election announcements and adds to manager", func() {
					orch.ParticipateInElection(mockPartManager)

					Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
				})

				It("acquires a lock from the kv-store", func() {
					orch.ParticipateInElection(mockPartManager)

					Eventually(mockKvStore.acquireRx).Should(Receive(Equal(fmt.Sprintf("%s~%d", key, expectedReplica))))
				})

				It("deletes the announcement", func() {
					orch.ParticipateInElection(mockPartManager)

					Eventually(mockKvStore.deleteAnnounceCh).Should(Receive(Equal("some-key~1")))
				})

				It("adds to the partition manager", func() {
					orch.ParticipateInElection(mockPartManager)

					Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
					Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(expectedReplica)))
				})

				Context("told to announce replica", func() {

					var (
						expectedAnnounce uint
					)

					BeforeEach(func() {
						expectedAnnounce = 99

						mockPartManager.addResultCh <- expectedAnnounce
						mockPartManager.addResultOkCh <- true
					})

					It("announces replica", func() {
						orch.ParticipateInElection(mockPartManager)

						Eventually(mockKvStore.announceCh).Should(Receive(Equal("some-key~99")))
					})
				})
			})

			Context("not the leader", func() {

				It("does not start elections for replicas", func() {
					orch.ParticipateInElection(mockPartManager)

					Consistently(mockKvStore.announceCh).Should(HaveLen(0))
				})
			})

			Context("is the leader", func() {
				var (
					expectedName string
					expectedAddr string
				)

				BeforeEach(func() {
					expectedName = "some-key"
					expectedReplica = 0
					expectedAddr = "some-addr"
				})

				It("start an election for each replica", func() {
					orch.ParticipateInElection(mockPartManager)

					Eventually(mockKvStore.announceCh).Should(HaveLen(2))
					Consistently(mockKvStore.announceCh).Should(HaveLen(2))

					Expect(mockKvStore.announceCh).To(Receive(Equal("some-key~1")))
					Expect(mockKvStore.announceCh).To(Receive(Equal("some-key~2")))
				})
			})
		})

		It("does not participate in an election if told not to", func() {
			orch.ParticipateInElection(mockPartManager)

			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- key + "~1"
			mockPartManager.partCh <- false

			By("asking the participation")
			Eventually(mockPartManager.addCh).Should(Receive(Equal(key)))
			Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(1)))

			Consistently(mockKvStore.acquireRx).ShouldNot(Receive())
		})

		Context("loses election", func() {

			It("listens for election announcements and does not add to manager", func() {
				orch.ParticipateInElection(mockPartManager)

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

})
