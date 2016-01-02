package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader", func() {
	var (
		mockFetcher        *mockReadConnectionFetcher
		mockReadConnection *mockReadConnection

		reader *broker.Reader

		fileName string
		fileId   uint64
	)

	var populateFetcherMock = func() {
		mockFetcher.fileIdCh <- fileId
		mockFetcher.readConnectionCh <- mockReadConnection
	}

	BeforeEach(func() {
		fileName = "some-name"
		fileId = 99

		mockFetcher = newMockReadConnectionFetcher()
		mockReadConnection = newMockReadConnection()
		reader = broker.NewReader(fileName, mockFetcher)
	})

	Describe("ReadFromFile()", func() {

		var (
			expectedData  []byte
			expectedIndex int64
		)

		var populateReadConnectionMock = func() {
			mockReadConnection.resultCh <- expectedData
			mockReadConnection.indexCh <- expectedIndex
		}

		BeforeEach(func() {
			close(mockFetcher.errCh)
			expectedData = []byte("some-data")
			expectedIndex = 101

			populateReadConnectionMock()
			populateFetcherMock()
		})

		Context("without errors", func() {

			BeforeEach(func() {
				close(mockReadConnection.errCh)
			})

			It("reads from the given connection", func() {
				data, index, err := reader.ReadFromFile()

				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
				Expect(index).To(Equal(expectedIndex))
				Expect(mockReadConnection.fileIdCh).To(Receive(Equal(fileId)))
			})

			It("asks the fetcher for the expected file name", func() {
				reader.ReadFromFile()

				Expect(mockFetcher.fileNameCh).To(Receive(Equal(fileName)))
			})

			Context("reads more than once", func() {
				BeforeEach(func() {
					populateReadConnectionMock()
					populateFetcherMock()
					reader.ReadFromFile()
				})

				It("does not fetch a connection twice", func() {
					reader.ReadFromFile()

					Expect(mockFetcher.readConnectionCh).To(HaveLen(1))
				})
			})
		})

		Context("with websocket connection error", func() {

			var (
				expectedError *broker.ConnectionError
			)

			BeforeEach(func() {
				expectedError = &broker.ConnectionError{
					WebsocketError: true,
				}

				mockReadConnection.errCh <- expectedError

				populateReadConnectionMock()
				populateFetcherMock()
				populateReadConnectionMock()
				populateFetcherMock()

				close(mockReadConnection.errCh)

				reader.ReadFromFile()
			})

			It("acquires a new connection", func(done Done) {
				defer close(done)
				reader.ReadFromFile()

				Expect(mockFetcher.readConnectionCh).To(HaveLen(1))
			})
		})
	})

	Describe("SeekIndex()", func() {

		var (
			expectedIndex uint64
		)

		BeforeEach(func() {
			close(mockFetcher.errCh)
			populateFetcherMock()
		})

		Context("without errors", func() {

			BeforeEach(func() {
				close(mockReadConnection.seekErrCh)
			})

			It("seeks for the given fileId", func(done Done) {
				defer close(done)
				Expect(reader.SeekIndex(expectedIndex)).To(Succeed())

				Expect(mockReadConnection.seekIndexCh).To(Receive(Equal(expectedIndex)))
				Expect(mockReadConnection.fileIdCh).To(Receive(Equal(fileId)))
			})
		})

		Context("with websocket connection error", func() {

			var (
				expectedError *broker.ConnectionError
			)

			BeforeEach(func(done Done) {
				defer close(done)
				expectedError = &broker.ConnectionError{
					WebsocketError: true,
				}

				mockReadConnection.seekErrCh <- expectedError
				close(mockReadConnection.seekErrCh)

				populateFetcherMock()
				populateFetcherMock()

				close(mockReadConnection.errCh)

				reader.SeekIndex(expectedIndex)
			})

			It("acquires a new connection", func(done Done) {
				defer close(done)
				reader.SeekIndex(expectedIndex)

				Expect(mockFetcher.readConnectionCh).To(HaveLen(1))
			})
		})
	})

})
