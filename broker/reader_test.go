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

	Describe("ReadFromFile()", func() {

		var (
			expectedData  []byte
			expectedIndex int64
		)

		BeforeEach(func() {
			close(mockDirectConnection.errCh)
			expectedData = []byte("some-data")
			expectedIndex = 101

			mockDirectConnection.resultCh <- expectedData
			mockDirectConnection.indexCh <- expectedIndex
		})

		Context("without errors", func() {
			It("reads from the given connection", func() {
				data, index, err := reader.ReadFromFile()

				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
				Expect(index).To(Equal(expectedIndex))
				Expect(mockDirectConnection.fileIdCh).To(Receive(Equal(fileId)))
			})
		})
	})

	Describe("SeekIndex()", func() {

		var (
			expectedIndex uint64
		)

		BeforeEach(func() {
			close(mockDirectConnection.seekErrCh)
		})

		Context("without errors", func() {

			It("seeks for the given fileId", func() {
				Expect(reader.SeekIndex(expectedIndex)).To(Succeed())

				Expect(mockDirectConnection.seekIndexCh).To(Receive(Equal(expectedIndex)))
				Expect(mockDirectConnection.fileIdCh).To(Receive(Equal(fileId)))
			})
		})
	})

})
