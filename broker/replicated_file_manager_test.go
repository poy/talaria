package broker_test

import (
	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileManager", func() {
	var (
		mockReadConnectionFetcher *mockReadConnectionFetcher
		mockReadConn              *mockReadConnection
		mockIoProvider            *mockFileProvider
		mockReaderFetcher         *mockReaderFetcher
		manager                   *broker.ReplicatedFileManager
		mockWriter                *mockWriter
		reader                    *broker.Reader

		expectedName   string
		expectedFileId uint64
	)

	BeforeEach(func() {
		mockReadConnectionFetcher = newMockReadConnectionFetcher()
		mockReadConn = newMockReadConnection()
		mockWriter = newMockWriter()
		mockIoProvider = newMockFileProvider()
		mockReaderFetcher = newMockReaderFetcher()
		reader = broker.NewReader("some-name", mockReadConnectionFetcher)

		manager = broker.NewReplicatedFileManager(mockIoProvider, mockReaderFetcher)

		expectedName = "some-name"
		expectedFileId = 99

		mockReadConnectionFetcher.readConnectionCh <- mockReadConn
		mockReadConnectionFetcher.fileIdCh <- expectedFileId
		close(mockReadConnectionFetcher.errCh)
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

			var writeToReadConn = func(data []byte, index int64, err *broker.ConnectionError) {
				mockReadConn.resultCh <- data
				mockReadConn.indexCh <- index
				mockReadConn.errCh <- err
			}

			AfterEach(func() {
				By("closing the read loop with an error")
				writeToReadConn(nil, 0, broker.NewConnectionError("asdf", "", false))
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
						writeToReadConn(expectedData, 0, nil)
						writeToReadConn(expectedData, 1, nil)
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
					writeToReadConn(expectedData, expectedIndex, nil)
				})

				Context("reading non-zero start", func() {

					BeforeEach(func() {
						mockIoProvider.initIndexResultCh <- expectedIndex
					})

					It("initializes the write index", func() {
						manager.Add(expectedName, expectedReplica)

						Eventually(mockIoProvider.indexCh).Should(Receive(Equal(expectedIndex)))
						Eventually(mockWriter.dataCh).Should(Receive(Equal(expectedData)))
						Expect(mockReadConn.seekIndexCh).To(HaveLen(0))
					})
				})

				Context("writing to a file that already has data", func() {

					var (
						expectedSeekIndex int64
					)

					BeforeEach(func() {
						close(mockReadConn.seekErrCh)
						expectedSeekIndex = 202
						mockIoProvider.initIndexResultCh <- expectedSeekIndex
					})

					It("seeks the reader to the correct index", func() {
						manager.Add(expectedName, expectedReplica)

						Eventually(mockReadConn.seekIndexCh).Should(Receive(BeEquivalentTo(expectedSeekIndex)))
					})
				})
			})

			Describe("upgrades", func() {
				Context("is upgrade", func() {

					BeforeEach(func() {
						expectedReplica = 1
						mockIoProvider.writerCh <- mockWriter
						mockReaderFetcher.readerCh <- reader

						manager.Add(expectedName, expectedReplica+1)
					})

					It("returns the replica that needs to be announced", func(done Done) {
						defer close(done)
						replica, ok := manager.Add(expectedName, expectedReplica)

						Expect(ok).To(BeTrue())
						Expect(replica).To(BeEquivalentTo(expectedReplica + 1))
					})

					It("does not start reading from the leader again", func(done Done) {
						defer close(done)
						manager.Add(expectedName, expectedReplica)

						Expect(mockReaderFetcher.readerCh).To(HaveLen(1))
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

			Context("upgrade", func() {

				var (
					currentIndex uint
				)

				BeforeEach(func() {
					currentIndex = 2
				})

				It("returns true if the replica index is less than current", func(done Done) {
					defer close(done)
					manager.Add("some-name-1", currentIndex)

					Expect(manager.Participate("some-name-1", currentIndex-1)).To(BeTrue())
				})

				It("returns false if the replica index is more than current", func(done Done) {
					defer close(done)
					manager.Add("some-name-1", 2)

					Expect(manager.Participate("some-name-1", currentIndex+1)).To(BeFalse())
				})
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
