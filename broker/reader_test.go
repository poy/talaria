package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader", func() {
	var (
		mockReadConnection *mockReadConnection
		reader             *broker.Reader
		fileId             uint64
	)

	BeforeEach(func() {
		fileId = 99
		mockReadConnection = newMockReadConnection()
		reader = broker.NewReader(fileId, mockReadConnection)
	})

	Describe("ReadFromFile()", func() {

		var (
			expectedData  []byte
			expectedIndex int64
		)

		BeforeEach(func() {
			close(mockReadConnection.errCh)
			expectedData = []byte("some-data")
			expectedIndex = 101

			mockReadConnection.resultCh <- expectedData
			mockReadConnection.indexCh <- expectedIndex
		})

		Context("without errors", func() {
			It("reads from the given connection", func() {
				data, index, err := reader.ReadFromFile()

				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
				Expect(index).To(Equal(expectedIndex))
				Expect(mockReadConnection.fileIdCh).To(Receive(Equal(fileId)))
			})
		})
	})

	Describe("SeekIndex()", func() {

		var (
			expectedIndex uint64
		)

		BeforeEach(func() {
			close(mockReadConnection.seekErrCh)
		})

		Context("without errors", func() {

			It("seeks for the given fileId", func() {
				Expect(reader.SeekIndex(expectedIndex)).To(Succeed())

				Expect(mockReadConnection.seekIndexCh).To(Receive(Equal(expectedIndex)))
				Expect(mockReadConnection.fileIdCh).To(Receive(Equal(fileId)))
			})
		})
	})

})
