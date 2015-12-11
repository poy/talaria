package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader", func() {
	var (
		mockDirectConnection *mockDirectConnection
		reader               *broker.Reader
		fileId               uint64
	)

	BeforeEach(func() {
		fileId = 99
		mockDirectConnection = newMockDirectConnection()
		reader = broker.NewReader(fileId, mockDirectConnection)
	})

	It("reads from the given connection", func() {
		close(mockDirectConnection.errCh)
		expectedData := []byte("some-data")
		var expectedIndex int64 = 101
		mockDirectConnection.resultCh <- expectedData
		mockDirectConnection.indexCh <- expectedIndex

		data, index, err := reader.ReadFromFile()
		Expect(err).ToNot(HaveOccurred())
		Expect(data).To(Equal(expectedData))
		Expect(index).To(Equal(expectedIndex))
		Expect(mockDirectConnection.fileIdCh).To(Receive(Equal(fileId)))
	})

})
