package orchestrator_test

import (
	"fmt"

	"github.com/apoydence/talaria/orchestrator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var (
		expectedKey string

		mockKvStore     *mockKvStore
		mockPartManager *mockPartitionManager
		clientAddr      string
		orch            *orchestrator.Orchestrator
	)

	BeforeEach(func() {
		expectedKey = "some-key"

		clientAddr = "some-addr"
		mockKvStore = newMockKvStore()
		mockPartManager = newMockPartitionManager()
		orch = orchestrator.New(clientAddr, 2, mockKvStore)
	})

	Describe("FetchLeader()", func() {

		var (
			expectedLeader string
		)

		BeforeEach(func() {
			expectedLeader = "some-leader"
		})

		Context("leader already elected", func() {

			BeforeEach(func() {
				mockKvStore.fetchLeaderTx <- expectedLeader
				mockKvStore.fetchLeaderOk <- true
			})

			It("returns the elected leader when told to create", func() {
				leaderUri, local, err := orch.FetchLeader(expectedKey, true)

				Expect(err).ToNot(HaveOccurred())
				Expect(leaderUri).To(Equal(expectedLeader))
				Expect(local).To(BeFalse())
				Expect(mockKvStore.fetchLeaderRx).To(Receive(Equal(expectedKey + "~0")))
			})

			It("returns the elected leader when told not to create", func() {
				leaderUri, local, err := orch.FetchLeader(expectedKey, false)

				Expect(err).ToNot(HaveOccurred())
				Expect(leaderUri).To(Equal(expectedLeader))
				Expect(local).To(BeFalse())
				Expect(mockKvStore.fetchLeaderRx).To(Receive(Equal(expectedKey + "~0")))
			})
		})

		Context("leader not yet elected", func() {

			BeforeEach(func() {
				mockKvStore.fetchLeaderTx <- ""
				mockKvStore.fetchLeaderOk <- false
				mockKvStore.fetchLeaderTx <- expectedLeader
			})

			It("returns the expected leader", func() {
				leader, _, _ := orch.FetchLeader(expectedKey, true)
				Expect(leader).To(Equal(expectedLeader))
			})

			It("starts listening for results for leader", func() {
				orch.FetchLeader(expectedKey, true)

				Eventually(mockKvStore.listenNameCh).Should(Receive(Equal(expectedKey + "~0")))
				Eventually(mockKvStore.leaderCallbackCh).Should(Receive())
			})

			It("announces an election for the leader", func() {
				orch.FetchLeader(expectedKey, true)

				Eventually(mockKvStore.announceCh).Should(Receive(Equal(expectedKey + "~0")))
			})

			It("returns an error when told not to create", func() {
				_, _, err := orch.FetchLeader(expectedKey, false)

				Expect(err).To(HaveOccurred())
				Eventually(mockKvStore.listenNameCh).ShouldNot(Receive())
				Eventually(mockKvStore.leaderCallbackCh).ShouldNot(Receive())
			})
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
				mockKvStore.announceLeaderTx <- fmt.Sprintf("%s~%d", expectedKey, expectedReplica)
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

					Eventually(mockKvStore.acquireRx).Should(Receive(Equal(fmt.Sprintf("%s~%d", expectedKey, expectedReplica))))
				})

				It("deletes the announcement", func() {
					orch.ParticipateInElection(mockPartManager)

					Eventually(mockKvStore.deleteAnnounceCh).Should(Receive(Equal(expectedKey + "~1")))
				})

				It("adds to the partition manager", func() {
					orch.ParticipateInElection(mockPartManager)

					Eventually(mockPartManager.addCh).Should(Receive(Equal(expectedKey)))
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

						Eventually(mockKvStore.announceCh).Should(Receive(Equal(expectedKey + "~99")))
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

					Expect(mockKvStore.announceCh).To(Receive(Equal(expectedName + "~1")))
					Expect(mockKvStore.announceCh).To(Receive(Equal(expectedName + "~2")))
				})
			})
		})

		It("does not participate in an election if told not to", func() {
			orch.ParticipateInElection(mockPartManager)

			Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
			mockKvStore.announceLeaderTx <- expectedKey + "~1"
			mockPartManager.partCh <- false

			By("asking the participation")
			Eventually(mockPartManager.addCh).Should(Receive(Equal(expectedKey)))
			Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(1)))

			Consistently(mockKvStore.acquireRx).ShouldNot(Receive())
		})

		Context("loses election", func() {

			It("listens for election announcements and does not add to manager", func() {
				orch.ParticipateInElection(mockPartManager)

				Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
				mockKvStore.announceLeaderTx <- expectedKey + "~1"
				mockKvStore.acquireTx <- false
				mockPartManager.partCh <- true
				Eventually(mockKvStore.acquireRx).Should(Receive(Equal(fmt.Sprintf("%s~%d", expectedKey, 1))))

				By("asking the participation")
				Eventually(mockPartManager.addCh).Should(Receive(Equal(expectedKey)))
				Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(1)))

				By("not adding to the manager")
				Consistently(mockPartManager.addCh).ShouldNot(Receive())
			})
		})
	})

})
