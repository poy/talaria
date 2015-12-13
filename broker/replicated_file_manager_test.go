package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileManager", func() {
	var (
		mockWriterFactory       *mockWriterFactory
		mockInnerBrokerProvider *mockInnerBrokerProvider
		mockReplicaListener     *mockReplicaListener
		manager                 *broker.ReplicatedFileManager
		expectedName            string
		expectedReplica         uint
	)

	BeforeEach(func() {
		mockWriterFactory = newMockWriterFactory()
		mockInnerBrokerProvider = newMockInnerBrokerProvider()
		mockReplicaListener = newMockReplicaListener()
		manager = broker.NewReplicatedFileManager(mockWriterFactory, mockInnerBrokerProvider, mockReplicaListener)

		expectedName = "some-name"
		expectedReplica = 7
	})

	Describe("Add()", func() {
		It("fetches a new writer on Add()", func() {
			mockWriterFactory.resultCh <- nil

			manager.Add(expectedName, expectedReplica)
			Expect(mockWriterFactory.nameCh).To(Receive(Equal(expectedName)))
		})
	})

	Describe("Participate", func() {
		It("returns true if it is not affiliated with the partition", func(done Done) {
			defer close(done)
			mockWriterFactory.resultCh <- nil
			manager.Add("some-name-1", 2)
			Expect(manager.Participate("some-name-2", 3)).To(BeTrue())
			Expect(manager.Participate("some-name-1", 3)).To(BeFalse())
		})

		It("returns true if the replica would be an upgrade", func(done Done) {
			defer close(done)
			mockWriterFactory.resultCh <- nil
			manager.Add("some-name-1", 2)

			By("giving a replia index less than the current")
			Expect(manager.Participate("some-name-1", 1)).To(BeTrue())

			By("giving a replia index more than the current")
			Expect(manager.Participate("some-name-1", 4)).To(BeFalse())
		})
	})
})
