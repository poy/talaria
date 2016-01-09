package broker_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FileController", func() {
	var (
		expectedFileId   uint64
		expectedFileName string

		tmpDir           string
		mockOrchestrator *mockOrchestrator
		mockFileProvider *mockFileProvider
		fileController   *broker.FileController
	)

	var populateLocalOrch = func() {
		mockOrchestrator.uriCh <- "http://uri.local"
		mockOrchestrator.localCh <- true
		mockOrchestrator.errCh <- nil
	}

	BeforeEach(func() {
		expectedFileId = 3
		expectedFileName = "some-name"

		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		mockOrchestrator = newMockOrchestrator()
		mockFileProvider = newMockFileProvider()
		fileController = broker.NewFileController(mockFileProvider, mockOrchestrator)
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("FetchFile()", func() {
		var populateProvider = func() {
			close(mockFileProvider.writerCh)
			close(mockFileProvider.readerCh)
		}

		BeforeEach(func() {
			populateProvider()
		})

		Context("create file", func() {

			Context("local file already added", func() {

				BeforeEach(func() {
					populateLocalOrch()
					populateLocalOrch()

					fileController.FetchFile(expectedFileId, expectedFileName, true)
				})

				It("succeeds when given the same name for the ID", func() {
					Expect(fileController.FetchFile(expectedFileId, expectedFileName, true)).To(Succeed())
				})

				It("succeeds when given the same name for a different ID", func() {
					Expect(fileController.FetchFile(expectedFileId+1, expectedFileName, true)).To(Succeed())
				})

				It("fails when given a different name for the same ID", func() {
					Expect(fileController.FetchFile(expectedFileId, expectedFileName+"-other", true)).ToNot(Succeed())
				})

				It("asks the orchestrator for the correct file", func() {
					Eventually(mockOrchestrator.nameCh).Should(Receive(Equal(expectedFileName)))
				})

				It("instructs the orchestrator to create the leader", func() {
					Eventually(mockOrchestrator.createCh).Should(Receive(BeTrue()))
				})
			})

			Context("remote file already added", func() {

				var (
					expectedUri string
				)

				BeforeEach(func() {
					expectedUri = "http://uri.b"

					mockOrchestrator.uriCh <- expectedUri
					mockOrchestrator.localCh <- false
					mockOrchestrator.errCh <- nil
				})

				It("returns an error for a non-local file", func() {
					err := fileController.FetchFile(4, "some-name-2", true)

					Expect(err).To(HaveOccurred())
					Expect(err.Uri).To(Equal(expectedUri))
				})

				It("returns an error for a non-local file when only fetching", func() {
					err := fileController.FetchFile(4, "some-name-2", false)

					Expect(err).To(HaveOccurred())
					Expect(err.Uri).To(Equal(expectedUri))
				})
			})
		})

		Context("don't create file", func() {

			Context("local file already created", func() {

				BeforeEach(func() {
					populateLocalOrch()
					populateLocalOrch()

					fileController.FetchFile(expectedFileId, expectedFileName, true)
				})

				It("fetches file", func() {
					Expect(fileController.FetchFile(expectedFileId, expectedFileName, false)).To(Succeed())
				})

				It("fetches file for different fileId", func() {
					Expect(fileController.FetchFile(expectedFileId+1, expectedFileName, false)).To(Succeed())
				})
			})

			Context("local file not created", func() {

				BeforeEach(func() {
					mockOrchestrator.uriCh <- ""
					mockOrchestrator.localCh <- true
					mockOrchestrator.errCh <- fmt.Errorf("file doesn't exist")
				})

				It("fails to fetch a non-created file", func() {
					Expect(fileController.FetchFile(expectedFileId, expectedFileName, false)).ToNot(Succeed())
				})

				It("instructs the orchestrator to create the leader", func() {
					fileController.FetchFile(expectedFileId, expectedFileName, false)

					Eventually(mockOrchestrator.createCh).Should(Receive(BeFalse()))
				})
			})
		})

		Context("orchestrator gives an error", func() {

			var (
				expectedErr error
			)

			BeforeEach(func() {
				expectedErr = fmt.Errorf("some-error")

				mockOrchestrator.uriCh <- ""
				mockOrchestrator.localCh <- true
				mockOrchestrator.errCh <- expectedErr
			})

			It("returns the expected error", func() {
				Expect(fileController.FetchFile(expectedFileId, expectedFileName, true)).ToNot(Succeed())
			})
		})
	})

	Describe("WriteToFile()", func() {
		It("returns an error for an unknown file ID", func() {
			_, err := fileController.WriteToFile(0, []byte("some-data"))
			Expect(err).To(HaveOccurred())
			Expect(mockFileProvider.writerNameCh).ToNot(Receive())
		})

		It("writes to the correct writer", func() {
			populateLocalOrch()
			populateLocalOrch()
			mockFileProvider.writerCh <- createFile(tmpDir, "some-name-1")
			mockFileProvider.writerCh <- createFile(tmpDir, "some-name-2")
			mockFileProvider.readerCh <- nil
			mockFileProvider.readerCh <- nil
			expectedData := []byte("some-data")

			var fileId1 uint64 = 3
			ffErr := fileController.FetchFile(fileId1, "some-name-1", true)
			Expect(ffErr).To(BeNil())
			Expect(mockFileProvider.writerNameCh).To(Receive(Equal("some-name-1")))

			var fileId2 uint64 = 4
			ffErr = fileController.FetchFile(fileId2, "some-name-2", true)
			Expect(ffErr).To(BeNil())
			Expect(mockFileProvider.writerNameCh).To(Receive(Equal("some-name-2")))

			By("writing to the same file twice")
			offset, err := fileController.WriteToFile(fileId1, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(len(expectedData)))
			Expect(readEntireFile(tmpDir, "some-name-1")).To(Equal(expectedData))

			offset, err = fileController.WriteToFile(fileId1, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(2 * len(expectedData)))
			Expect(readEntireFile(tmpDir, "some-name-1")).To(Equal(append(expectedData, expectedData...)))

			By("writing to a different file")
			offset, err = fileController.WriteToFile(fileId2, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(len(expectedData)))
			Expect(readEntireFile(tmpDir, "some-name-2")).To(Equal(expectedData))
		})
	})

	Describe("ReadFromFile()", func() {

		var (
			mockReader *mockReader
			wg         sync.WaitGroup
			callback   func([]byte, int64, error)
			dataCh     chan []byte
			sendCh     chan []byte
			offsetCh   chan int64
			errCh      chan error
		)

		var createCallback = func(dataCh chan []byte, offsetCh chan int64, errCh chan error) func([]byte, int64, error) {
			return func(data []byte, offset int64, err error) {
				dataCh <- data
				offsetCh <- offset
				errCh <- err
			}
		}

		var setupMockReader = func() {
			sendCh = make(chan []byte, 100)
			close(mockReader.readErrs)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for buffer := range mockReader.buffers {
					var data []byte
					Expect(sendCh).To(Receive(&data))
					copy(buffer, data)
					mockReader.lengths <- len(data)
				}
			}()
		}

		BeforeEach(func() {
			mockReader = newMockReader()
			populateLocalOrch()

			dataCh = make(chan []byte, 100)
			offsetCh = make(chan int64, 100)
			errCh = make(chan error, 100)
			callback = createCallback(dataCh, offsetCh, errCh)
			setupMockReader()
		})

		AfterEach(func() {
			close(mockReader.buffers)
			wg.Wait()
		})

		It("returns an error for an unknown file ID", func() {
			fileController.ReadFromFile(0, callback)
			Eventually(errCh).Should(Receive(HaveOccurred()))
			Expect(mockFileProvider.writerNameCh).ToNot(Receive())
		})

		It("reads from the correct file", func() {
			mockFileProvider.writerCh <- nil
			mockFileProvider.readerCh <- mockReader
			mockReader.nextIndexes <- 101
			mockReader.nextIndexes <- 102
			expectedData := []byte("some-data")

			sendCh <- expectedData

			var fileId1 uint64 = 3
			ffErr := fileController.FetchFile(fileId1, "some-name-1", true)
			Expect(ffErr).To(BeNil())
			Expect(mockFileProvider.writerNameCh).To(Receive(Equal("some-name-1")))

			By("reading from the first offset")
			fileController.ReadFromFile(fileId1, callback)
			Consistently(errCh).ShouldNot(Receive(HaveOccurred()))
			Eventually(dataCh).Should(Receive(Equal(expectedData)))
			Eventually(offsetCh).Should(Receive(BeEquivalentTo(100)))

			By("reading from the next offset")
			sendCh <- expectedData

			fileController.ReadFromFile(fileId1, callback)
			Eventually(errCh).ShouldNot(Receive(HaveOccurred()))
			Eventually(dataCh).Should(Receive(Equal(expectedData)))
			Eventually(offsetCh).Should(Receive(BeEquivalentTo(101)))
		})

		It("returns an error if the reader returns an error and negative n", func(done Done) {
			defer close(done)
			mockReader := newMockReader()
			mockReader.lengths <- -10
			mockReader.readErrs <- fmt.Errorf("some-error")
			mockFileProvider.readerCh <- mockReader
			mockFileProvider.writerCh <- nil
			mockFileProvider.indexCh <- 101

			var fileId uint64 = 3
			ffErr := fileController.FetchFile(fileId, "some-name-1", true)
			Expect(ffErr).To(BeNil())

			fileController.ReadFromFile(fileId, callback)
			Eventually(errCh).ShouldNot(Receive(HaveOccurred()))
		})
	})

	Describe("SeekIndex()", func() {
		var (
			errCh         chan error
			expectedIndex uint64
		)

		BeforeEach(func() {
			expectedIndex = 101
			errCh = make(chan error, 100)
		})

		var callback = func(err error) {
			errCh <- err
		}

		Context("without existing file", func() {
			It("returns an error for an unknown file ID", func() {
				fileController.SeekIndex(0, expectedIndex, callback)
				Expect(errCh).To(Receive(HaveOccurred()))
				Expect(mockFileProvider.writerNameCh).ToNot(Receive())
			})
		})

		Context("with existing file", func() {
			var (
				mockReader     *mockReader
				expectedFileId uint64
			)

			BeforeEach(func() {
				mockReader = newMockReader()
				mockFileProvider.writerCh <- nil
				mockFileProvider.readerCh <- mockReader
				expectedFileId = 3
				close(mockReader.seekErrs)

				populateLocalOrch()
				Expect(fileController.FetchFile(expectedFileId, "some-name", true)).To(Succeed())
			})

			It("seeks within the reader", func() {
				fileController.SeekIndex(expectedFileId, expectedIndex, callback)
				Eventually(errCh).Should(Receive(BeNil()))
				Expect(mockReader.seekIndexes).To(Receive(Equal(expectedIndex)))
			})
		})
	})
})

func createFile(dir, name string) *os.File {
	file, err := os.Create(path.Join(dir, name))
	Expect(err).ToNot(HaveOccurred())
	return file
}

func readEntireFile(dir, name string) []byte {
	file, err := os.Open(path.Join(dir, name))
	Expect(err).ToNot(HaveOccurred())
	data, err := ioutil.ReadAll(file)
	Expect(err).ToNot(HaveOccurred())
	return data
}

func writeToFile(file *os.File, data []byte, offset int64, whence int) {
	_, err := file.Write(data)
	Expect(err).ToNot(HaveOccurred())
	err = file.Sync()
	Expect(err).ToNot(HaveOccurred())
	_, err = file.Seek(offset, whence)
	Expect(err).ToNot(HaveOccurred())
}
