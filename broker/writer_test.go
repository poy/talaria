package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Writer", func() {
	var (
		expectedFileId      uint64
		expectedData        []byte
		mockWriteConnection *mockWriteConnection
		writer              *broker.Writer
	)

	BeforeEach(func() {
		expectedFileId = 99
		expectedData = []byte("some-data")
		mockWriteConnection = newMockWriteConnection()

		writer = broker.NewWriter(expectedFileId, mockWriteConnection)
	})

	Describe("WriteToFile()", func() {
		Context("without errors", func() {

			var (
				expectedIndex int64
			)

			BeforeEach(func() {
				expectedIndex = 101
				mockWriteConnection.indexCh <- expectedIndex
				close(mockWriteConnection.errCh)
			})

			It("uses the expected fileId", func() {
				writer.WriteToFile(expectedData)

				Expect(mockWriteConnection.fileIdCh).To(Receive(Equal(expectedFileId)))
			})

			It("writes the expected data", func() {
				writer.WriteToFile(expectedData)

				Expect(mockWriteConnection.dataCh).To(Receive(Equal(expectedData)))
			})

			It("does not return an error", func() {
				_, err := writer.WriteToFile(expectedData)

				Expect(err).ToNot(HaveOccurred())
			})

			It("returns the expected index", func() {
				index, _ := writer.WriteToFile(expectedData)

				Expect(index).To(Equal(expectedIndex))
			})
		})

		Context("with errors", func() {
			var (
				expectedError *broker.ConnectionError
			)

			BeforeEach(func() {
				expectedError = new(broker.ConnectionError)
				close(mockWriteConnection.indexCh)
				mockWriteConnection.errCh <- expectedError
			})

			It("returns the expected error", func() {
				_, err := writer.WriteToFile(expectedData)

				Expect(err).To(Equal(expectedError))
			})

		})
	})
})
