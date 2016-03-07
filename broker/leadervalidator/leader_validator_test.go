package leadervalidator_test

import (
	"sync"

	"github.com/apoydence/talaria/broker/leadervalidator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LeaderValidator", func() {
	var (
		expectedName  string
		expectedIndex uint64

		mockReplicaFetcher   *mockReplicaFetcher
		mockValidator        *mockValidator
		mockValidatorFetcher *mockValidatorFetcher

		leaderValidator *leadervalidator.LeaderValidator

		callbackNameCh  chan string
		callbackValidCh chan bool

		callbackWg sync.WaitGroup
	)

	var expectedCallback = func(name string, valid bool) {
		defer callbackWg.Done()
		callbackNameCh <- name
		callbackValidCh <- valid
	}

	BeforeEach(func() {
		callbackWg.Add(1)
		expectedName = "some-name"
		expectedIndex = 101

		callbackNameCh = make(chan string, 100)
		callbackValidCh = make(chan bool, 100)

		mockReplicaFetcher = newMockReplicaFetcher()
		mockValidator = newMockValidator()
		mockValidatorFetcher = newMockValidatorFetcher()

		mockValidatorFetcher.validatorCh <- mockValidator

		leaderValidator = leadervalidator.New(mockReplicaFetcher, mockValidatorFetcher)
	})

	AfterEach(func() {
		callbackWg.Wait()
	})

	Context("2 replicas", func() {

		var (
			expectedReplicaURLs []string
		)

		BeforeEach(func() {
			expectedReplicaURLs = []string{"some-url-1", "some-url-2"}

			mockReplicaFetcher.replicaCh <- expectedReplicaURLs

			for range expectedReplicaURLs {
				mockValidatorFetcher.validatorCh <- mockValidator
			}
		})

		Context("don't care about the validator results", func() {

			BeforeEach(func() {
				for range expectedReplicaURLs {
					mockValidator.retCh <- true
				}
			})

			It("uses the replica URLs to fetch validators", func() {
				leaderValidator.Validate(expectedName, expectedIndex, expectedCallback)

				Eventually(mockValidatorFetcher.urlCh).Should(Receive(Equal(expectedReplicaURLs[0])))
				Eventually(mockValidatorFetcher.urlCh).Should(Receive(Equal(expectedReplicaURLs[1])))
			})

			It("uses the expected name for the validators", func() {
				leaderValidator.Validate(expectedName, expectedIndex, expectedCallback)

				Eventually(mockValidator.nameCh).Should(Receive(Equal(expectedName)))
				Eventually(mockValidator.nameCh).Should(Receive(Equal(expectedName)))
			})

			It("uses the expected index for the validators", func() {
				leaderValidator.Validate(expectedName, expectedIndex, expectedCallback)

				Eventually(mockValidator.indexCh).Should(Receive(Equal(expectedIndex)))
				Eventually(mockValidator.indexCh).Should(Receive(Equal(expectedIndex)))
			})
		})

		Context("replicas all report leader is valid", func() {

			BeforeEach(func() {
				for range expectedReplicaURLs {
					mockValidator.retCh <- true
				}
			})

			It("reports that it is ready", func() {
				leaderValidator.Validate(expectedName, expectedIndex, expectedCallback)

				Eventually(callbackNameCh).Should(Receive(Equal(expectedName)))
				Eventually(callbackValidCh).Should(Receive(BeTrue()))
			})

			Context("nil validator (blacklisted)", func() {
				BeforeEach(func() {
					By("replacing one of the validators with a nil")
					<-mockValidatorFetcher.validatorCh
					<-mockValidatorFetcher.validatorCh
					mockValidatorFetcher.validatorCh <- nil
				})

				It("reports that it is ready", func() {
					leaderValidator.Validate(expectedName, expectedIndex, expectedCallback)

					Eventually(callbackNameCh).Should(Receive(Equal(expectedName)))
					Eventually(callbackValidCh).Should(Receive(BeTrue()))
				})

			})
		})

		Context("replica reports leader is not valid", func() {
			BeforeEach(func() {
				for range expectedReplicaURLs {
					mockValidator.retCh <- false
				}
			})

			It("reports that it is ready", func() {
				leaderValidator.Validate(expectedName, expectedIndex, expectedCallback)

				Eventually(callbackNameCh).Should(Receive(Equal(expectedName)))
				Eventually(callbackValidCh).Should(Receive(BeFalse()))
			})
		})

	})
})
