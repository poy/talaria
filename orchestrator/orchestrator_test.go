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

		mockKvStore         *mockKvStore
		mockPartManager     *mockPartitionManager
		mockLeaderValidator *mockLeaderValidator
		mockIndexProvider   *mockIndexProvider

		clientAddr string
		orch       *orchestrator.Orchestrator
	)

	BeforeEach(func() {
		expectedKey = "some-key"
		clientAddr = "some-addr"

		mockKvStore = newMockKvStore()
		mockPartManager = newMockPartitionManager()
		mockLeaderValidator = newMockLeaderValidator()
		mockIndexProvider = newMockIndexProvider()

		orch = orchestrator.New(clientAddr, 2, mockKvStore, mockPartManager, mockLeaderValidator, mockIndexProvider)
	})

	Describe("FetchLeader()", func() {

		var (
			expectedLeader string
		)

		BeforeEach(func() {
			expectedLeader = clientAddr
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
				Expect(local).To(BeTrue())
				Expect(mockKvStore.fetchLeaderRx).To(Receive(Equal(expectedKey + "~0")))
			})

			It("returns the elected leader when told not to create", func() {
				leaderUri, local, err := orch.FetchLeader(expectedKey, false)

				Expect(err).ToNot(HaveOccurred())
				Expect(leaderUri).To(Equal(expectedLeader))
				Expect(local).To(BeTrue())
				Expect(mockKvStore.fetchLeaderRx).To(Receive(Equal(expectedKey + "~0")))
			})
		})

		Context("leader not yet elected", func() {

			BeforeEach(func() {
				mockKvStore.fetchLeaderTx <- ""
				mockKvStore.fetchLeaderOk <- false
				mockKvStore.fetchLeaderTx <- expectedLeader
			})

			Context("partition manager told to add leader", func() {

				BeforeEach(func() {
					mockPartManager.partCh <- false
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

			Context("partition manager not told to add leader yet", func() {

				BeforeEach(func() {
					mockPartManager.partCh <- true
				})

				It("waits for the leader to be validated", func() {
					result := make(chan bool)
					go func() {
						defer GinkgoRecover()
						_, ok, err := orch.FetchLeader(expectedKey, true)
						Expect(err).ToNot(HaveOccurred())
						result <- ok
					}()

					Consistently(result).Should(HaveLen(0))
					mockPartManager.partCh <- false
					Eventually(result).Should(Receive(BeTrue()))
				})
			})
		})
	})

	Describe("ParticipateInElections()", func() {

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
					orch.ParticipateInElections()

					Eventually(mockKvStore.announcementCallbackCh).Should(Receive())
				})

				It("acquires a lock from the kv-store", func() {
					orch.ParticipateInElections()

					Eventually(mockKvStore.acquireRx).Should(Receive(Equal(fmt.Sprintf("%s~%d", expectedKey, expectedReplica))))
				})

				It("adds to the partition manager", func() {
					orch.ParticipateInElections()

					Eventually(mockPartManager.addCh).Should(Receive(Equal(expectedKey)))
					Eventually(mockPartManager.indexCh).Should(Receive(BeEquivalentTo(expectedReplica)))
				})

				Context("not the leader", func() {

					It("does not start elections for replicas", func() {
						orch.ParticipateInElections()

						Consistently(mockKvStore.announceCh).Should(HaveLen(0))
					})

					It("does not validate leader", func() {
						orch.ParticipateInElections()

						Consistently(mockIndexProvider.nameCh).ShouldNot(Receive())
					})
				})

				Context("is the leader", func() {
					var (
						expectedName  string
						expectedAddr  string
						expectedIndex uint64

						expectedCallback func(string, bool)
					)

					var fetchCallback = func() {
						Eventually(mockLeaderValidator.callbackCh).Should(Receive(&expectedCallback))
					}

					BeforeEach(func() {
						expectedName = "some-key"
						expectedReplica = 0
						expectedAddr = "some-addr"
						expectedIndex = 101

						mockIndexProvider.indexCh <- expectedIndex
						mockIndexProvider.okCh <- true
					})

					JustBeforeEach(func() {
						orch.ParticipateInElections()
						fetchCallback()
					})

					Context("validation doesn't finish", func() {

						It("validates leadership before adding to the partition manager", func() {
							Eventually(mockLeaderValidator.nameCh).Should(Receive(Equal(expectedName)))
							Eventually(mockLeaderValidator.indexCh).Should(Receive(Equal(expectedIndex)))
							Eventually(mockIndexProvider.nameCh).Should(Receive(Equal(expectedName)))

							Consistently(mockPartManager.addCh).Should(HaveLen(1))
						})
					})

					Context("validation does finish", func() {

						Context("validation succeeds", func() {

							JustBeforeEach(func() {
								expectedCallback(expectedName, true)
							})

							It("start an election for each replica", func() {
								Eventually(mockKvStore.announceCh).Should(HaveLen(2))
								Consistently(mockKvStore.announceCh).Should(HaveLen(2))

								Expect(mockKvStore.announceCh).To(Receive(Equal(expectedName + "~1")))
								Expect(mockKvStore.announceCh).To(Receive(Equal(expectedName + "~2")))
							})

							It("adds the replica to the partiton manager", func() {
								Eventually(mockPartManager.addCh).Should(HaveLen(2))
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
									Eventually(mockKvStore.announceCh).Should(Receive(Equal(expectedKey + "~99")))
								})

								It("releases the lock for the replica", func() {
									Eventually(mockKvStore.releaseCh).Should(Receive(Equal(expectedKey + "~99")))
								})
							})
						})

						Context("validation fails", func() {

							JustBeforeEach(func() {
								expectedCallback(expectedName, false)
							})

							It("does not add the replica", func() {
								Consistently(mockPartManager.addCh).Should(HaveLen(1))
							})

							It("releases the key for the replica", func() {
								Eventually(mockKvStore.releaseCh).Should(Receive(Equal(expectedName + "~0")))
							})
						})
					})
				})

			})
		})

		It("does not participate in an election if told not to", func() {
			orch.ParticipateInElections()

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
				orch.ParticipateInElections()

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
