package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileManager", func() {
	var (
		mockDirectConn    *mockDirectConnection
		mockIoProvider    *mockFileProvider
		mockReaderFetcher *mockReaderFetcher
		manager           *broker.ReplicatedFileManager
		mockWriter        *mockWriter
		reader            *broker.Reader
		expectedName      string
	)

	BeforeEach(func() {
		mockDirectConn = newMockDirectConnection()
		mockWriter = newMockWriter()
		mockIoProvider = newMockFileProvider()
		mockReaderFetcher = newMockReaderFetcher()
		reader = broker.NewReader(99, mockDirectConn)

		manager = broker.NewReplicatedFileManager(mockIoProvider, mockReaderFetcher)

		expectedName = "some-name"
	})

	Describe("Add()", func() {

		var (
			expectedReplica uint
		)

		Context("is the leader", func() {
			BeforeEach(func() {
				expectedReplica = 0
				mockIoProvider.writerCh <- mockWriter
			})

			It("does not copy data to a writer", func(done Done) {
				defer close(done)
				manager.Add(expectedName, expectedReplica)

				By("not requesting a writer")
				Expect(mockIoProvider.writerCh).To(HaveLen(1))
			})
		})

		Context("not the leader", func() {
			BeforeEach(func() {
				mockIoProvider.writerCh <- mockWriter
				mockReaderFetcher.readerCh <- reader
				close(mockReaderFetcher.errCh)
				expectedReplica = 7
			})

			var writeToDirectConn = func(data []byte, index int64, err *broker.ConnectionError) {
				mockDirectConn.resultCh <- data
				mockDirectConn.indexCh <- index
				mockDirectConn.errCh <- err
			}

			AfterEach(func() {
				By("closing the read loop with an error")
				writeToDirectConn(nil, 0, broker.NewConnectionError("asdf", "", false))
			})

			Context("reading from the start of the file", func() {
				It("fetches a new Writer on Add()", func() {
					manager.Add(expectedName, expectedReplica)
					Expect(mockIoProvider.writerNameCh).To(Receive(Equal(expectedName)))
				})

				It("fetches a new Reader on Add()", func() {
					manager.Add(expectedName, expectedReplica)
					Expect(mockReaderFetcher.nameCh).To(Receive(Equal(expectedName)))
				})

				Context("multiple data points ready to read", func() {

					var (
						expectedData []byte
					)

					BeforeEach(func() {
						expectedData = []byte("some-data")
						writeToDirectConn(expectedData, 0, nil)
						writeToDirectConn(expectedData, 1, nil)
					})

					It("copies from the reader to the writer", func() {
						manager.Add(expectedName, expectedReplica)

						Eventually(mockWriter.dataCh).Should(Receive(Equal(expectedData)))
						Eventually(mockWriter.dataCh).Should(Receive(Equal(expectedData)))
					})

					It("does not initialze the write index for multiple reads", func() {
						manager.Add(expectedName, expectedReplica)

						Consistently(mockIoProvider.indexCh).ShouldNot(Receive())
					})
				})
			})

			Context("non-matching indexes", func() {
				var (
					expectedData  []byte
					expectedIndex int64
				)

				BeforeEach(func() {
					expectedData = []byte("some-data")
					expectedIndex = 101
					writeToDirectConn(expectedData, expectedIndex, nil)
				})

				Context("reading non-zero start", func() {

					BeforeEach(func() {
						mockIoProvider.initIndexResultCh <- expectedIndex
					})

					It("initializes the write index", func() {
						manager.Add(expectedName, expectedReplica)

						Eventually(mockIoProvider.indexCh).Should(Receive(Equal(expectedIndex)))
						Eventually(mockWriter.dataCh).Should(Receive(Equal(expectedData)))
						Expect(mockDirectConn.seekIndexCh).To(HaveLen(0))
					})
				})

				Context("writing to a file that already has data", func() {

					var (
						expectedSeekIndex int64
					)

					BeforeEach(func() {
						close(mockDirectConn.seekErrCh)
						expectedSeekIndex = 202
						mockIoProvider.initIndexResultCh <- expectedSeekIndex
					})

					It("seeks the reader to the correct index", func() {
						manager.Add(expectedName, expectedReplica)

						Eventually(mockDirectConn.seekIndexCh).Should(Receive(BeEquivalentTo(expectedSeekIndex)))
					})
				})
			})

		})

	})

	Describe("Participate()", func() {
		BeforeEach(func() {
			mockIoProvider.writerCh <- mockWriter
			mockReaderFetcher.readerCh <- reader
			close(mockReaderFetcher.errCh)
		})

		Context("not leader", func() {
			It("returns true if it is not affiliated with the partition", func(done Done) {
				defer close(done)
				manager.Add("some-name-1", 2)
				Expect(manager.Participate("some-name-2", 3)).To(BeTrue())
				Expect(manager.Participate("some-name-1", 3)).To(BeFalse())
			})

			It("returns true if the replica would be an upgrade", func(done Done) {
				defer close(done)
				manager.Add("some-name-1", 2)

				By("giving a replia index less than the current")
				Expect(manager.Participate("some-name-1", 1)).To(BeTrue())

				By("giving a replia index more than the current")
				Expect(manager.Participate("some-name-1", 4)).To(BeFalse())
			})
		})

		Context("is the leader", func() {
			It("returns false", func(done Done) {
				defer close(done)

				manager.Add("some-name-1", 0)

				Expect(manager.Participate("some-name-1", 1)).To(BeFalse())
			})
		})
	})
})
