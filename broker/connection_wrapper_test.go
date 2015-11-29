package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConnectionWrapper", func() {
	var (
		connWrapper    *broker.ConnectionWrapper
		mockConnection *mockConnection
		expectedFileId uint64
		expectedData   []byte
	)

	BeforeEach(func() {
		expectedFileId = 99
		expectedData = []byte("some-data")
		mockConnection = newMockConnection()
		connWrapper = broker.NewConnectionWrapper(expectedFileId, mockConnection)
	})

	Describe("Write()", func() {
		It("writes to the connection", func(done Done) {
			defer close(done)

			n, err := connWrapper.Write(expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(expectedData)))

			Expect(mockConnection.fileIdCh).To(Receive(Equal(expectedFileId)))
			Expect(mockConnection.dataCh).To(Receive(Equal(expectedData)))
		})
	})

	Describe("InitWriteIndex()", func() {
		It("inits the write index", func(done Done) {
			defer close(done)

			index, err := connWrapper.InitWriteIndex(101, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(index).To(BeEquivalentTo(101))
			Expect(mockConnection.fileIdCh).To(Receive(Equal(expectedFileId)))
			Expect(mockConnection.indexCh).To(Receive(BeEquivalentTo(101)))
			Expect(mockConnection.dataCh).To(Receive(Equal(expectedData)))
		})
	})

})
