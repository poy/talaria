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
		tmpDir           string
		mockOrchestrator *mockOrchestrator
		mockFileProvider *mockFileProvider
		fileController   *broker.FileController
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		mockOrchestrator = newMockOrchestrator()
		mockFileProvider = newMockFileProvider()
		fileController = broker.NewFileController(false, mockFileProvider, mockOrchestrator)
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("FetchFile()", func() {
		Context("Don't skip orch", func() {
			It("gives the correct file ID", func(done Done) {
				defer close(done)
				mockFileProvider.writerCh <- nil
				mockFileProvider.writerCh <- nil
				mockFileProvider.readerCh <- nil
				mockFileProvider.readerCh <- nil

				By("giving the same for the same file name")
				populateLocalOrch(mockOrchestrator)
				err := fileController.FetchFile(3, "some-name-1")
				Expect(err).To(BeNil())

				By("giving a different ID for a different name")
				populateLocalOrch(mockOrchestrator)
				err = fileController.FetchFile(4, "some-name-2")
				Expect(err).To(BeNil())
			})

			It("asks the orchestrator for the leader", func(done Done) {
				defer close(done)

				populateProvider := func() {
					mockFileProvider.writerCh <- nil
					mockFileProvider.writerCh <- nil
					mockFileProvider.readerCh <- nil
					mockFileProvider.readerCh <- nil
				}

				By("not returning an error for a local file")
				populateProvider()
				populateLocalOrch(mockOrchestrator)
				err := fileController.FetchFile(3, "some-name-1")
				Expect(err).To(BeNil())
				Eventually(mockOrchestrator.nameCh).Should(Receive(Equal("some-name-1")))

				By("returning an error for a non-local file")
				populateProvider()
				expectedUri := "http://uri.b"
				mockOrchestrator.uriCh <- expectedUri
				mockOrchestrator.localCh <- false
				err = fileController.FetchFile(4, "some-name-2")
				Expect(err).To(HaveOccurred())
				Expect(err.Uri).To(Equal(expectedUri))
				Eventually(mockOrchestrator.nameCh).Should(Receive(Equal("some-name-2")))
			}, 2)

			It("returns an error if the same file ID is used more than once", func() {
				mockFileProvider.writerCh <- nil
				mockFileProvider.readerCh <- nil

				populateLocalOrch(mockOrchestrator)
				err := fileController.FetchFile(3, "some-name-1")
				Expect(err).To(BeNil())

				By("not returning an error for the same name")
				err = fileController.FetchFile(3, "some-name-1")
				Expect(err).To(BeNil())

				By("returning an error for a different name")
				err = fileController.FetchFile(3, "some-name-2")
				Expect(err).To(HaveOccurred())
			})
		})

		Context("Skip Orch", func() {
			BeforeEach(func() {
				fileController = broker.NewFileController(true, mockFileProvider, mockOrchestrator)
			})

			It("gives the correct file ID", func(done Done) {
				defer close(done)
				mockFileProvider.writerCh <- nil
				mockFileProvider.writerCh <- nil
				mockFileProvider.readerCh <- nil
				mockFileProvider.readerCh <- nil

				By("giving the same for the same file name")
				By("not populating the orch")
				err := fileController.FetchFile(3, "some-name-1")
				Expect(err).To(BeNil())

				By("giving a different ID for a different name")
				err = fileController.FetchFile(4, "some-name-2")
				Expect(err).To(BeNil())
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
			populateLocalOrch(mockOrchestrator)
			populateLocalOrch(mockOrchestrator)
			mockFileProvider.writerCh <- createFile(tmpDir, "some-name-1")
			mockFileProvider.writerCh <- createFile(tmpDir, "some-name-2")
			mockFileProvider.readerCh <- nil
			mockFileProvider.readerCh <- nil
			expectedData := []byte("some-data")

			var fileId1 uint64 = 3
			ffErr := fileController.FetchFile(fileId1, "some-name-1")
			Expect(ffErr).To(BeNil())
			Expect(mockFileProvider.writerNameCh).To(Receive(Equal("some-name-1")))

			var fileId2 uint64 = 4
			ffErr = fileController.FetchFile(fileId2, "some-name-2")
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
			populateLocalOrch(mockOrchestrator)

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
			ffErr := fileController.FetchFile(fileId1, "some-name-1")
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
			ffErr := fileController.FetchFile(fileId, "some-name-1")
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

				populateLocalOrch(mockOrchestrator)
				Expect(fileController.FetchFile(expectedFileId, "some-name")).To(Succeed())
			})

			It("seeks within the reader", func() {
				fileController.SeekIndex(expectedFileId, expectedIndex, callback)
				Eventually(errCh).Should(Receive(BeNil()))
				Expect(mockReader.seekIndexes).To(Receive(Equal(expectedIndex)))
			})
		})
	})
})

func populateLocalOrch(orch *mockOrchestrator) {
	orch.uriCh <- "http://uri.local"
	orch.localCh <- true
}

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
