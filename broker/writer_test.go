package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Writer", func() {
	var (
		expectedFileId      uint64
		expectedFileName    string
		expectedData        []byte
		mockWriteConnection *mockWriteConnection
		mockFetcher         *mockWriteConnectionFetcher
		writer              *broker.Writer
	)

	var populateFetcherMock = func() {
		mockFetcher.fileIdCh <- expectedFileId
		mockFetcher.writeConnectionCh <- mockWriteConnection
	}

	BeforeEach(func() {
		expectedFileName = "some-name"
		expectedFileId = 99
		expectedData = []byte("some-data")
		mockWriteConnection = newMockWriteConnection()
		mockFetcher = newMockWriteConnectionFetcher()

		writer = broker.NewWriter(expectedFileName, mockFetcher)
	})

	Describe("WriteToFile()", func() {
		Context("without errors", func() {

			var (
				expectedIndex int64
			)

			var populateWriteConnMock = func() {
				mockWriteConnection.indexCh <- expectedIndex
			}

			BeforeEach(func() {
				expectedIndex = 101
				close(mockWriteConnection.errCh)
				close(mockFetcher.errCh)

				populateWriteConnMock()
				populateFetcherMock()
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

			It("asks the fetcher for the expected file name", func() {
				writer.WriteToFile(expectedData)

				Expect(mockFetcher.fileNameCh).To(Receive(Equal(expectedFileName)))
			})

			Context("writes more than once", func() {
				BeforeEach(func() {
					populateFetcherMock()
					populateWriteConnMock()

					writer.WriteToFile(expectedData)
				})

				It("does not fetch a connection twice", func(done Done) {
					defer close(done)
					writer.WriteToFile(expectedData)

					Expect(mockFetcher.writeConnectionCh).To(HaveLen(1))
				})
			})
		})

		Context("with errors", func() {
			var (
				expectedError *broker.ConnectionError
			)

			Context("non-websocket error", func() {

				BeforeEach(func() {
					expectedError = new(broker.ConnectionError)
					close(mockWriteConnection.indexCh)
					close(mockFetcher.errCh)
					populateFetcherMock()
					mockWriteConnection.errCh <- expectedError
				})

				It("returns the expected error", func() {
					_, err := writer.WriteToFile(expectedData)

					Expect(err).To(Equal(expectedError))
				})
			})

			Context("websocket connection error", func() {

				BeforeEach(func(done Done) {
					defer close(done)
					expectedError = &broker.ConnectionError{
						WebsocketError: true,
					}

					mockWriteConnection.errCh <- expectedError
					populateFetcherMock()
					populateFetcherMock()

					close(mockWriteConnection.indexCh)
					close(mockFetcher.errCh)
					close(mockWriteConnection.errCh)

					writer.WriteToFile(expectedData)
				})

				It("acquires a new connection", func(done Done) {
					defer close(done)
					writer.WriteToFile(expectedData)

					Expect(mockFetcher.writeConnectionCh).To(HaveLen(0))
				})
			})

		})
	})
})
