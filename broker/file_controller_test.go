package broker_test

import (
	"io/ioutil"
	"os"
	"path"

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
		fileController = broker.NewFileController(mockFileProvider, mockOrchestrator)
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Context("FetchFile", func() {
		It("gives the correct file ID", func(done Done) {
			defer close(done)
			mockFileProvider.writerCh <- nil
			mockFileProvider.writerCh <- nil
			mockFileProvider.readerCh <- nil
			mockFileProvider.readerCh <- nil

			By("Giving the same for the same file name")
			populateLocalOrch(mockOrchestrator)
			err := fileController.FetchFile(3, "some-name-1")
			Expect(err).To(BeNil())

			By("Giving a different ID for a different name")
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

			By("Not returning an error for a local file")
			populateProvider()
			populateLocalOrch(mockOrchestrator)
			err := fileController.FetchFile(3, "some-name-1")
			Expect(err).To(BeNil())
			Eventually(mockOrchestrator.nameCh).Should(Receive(Equal("some-name-1")))

			By("Returning an error for a non-local file")
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

	Context("WriteToFile", func() {
		It("seturns an error for an unknown file ID", func() {
			_, err := fileController.WriteToFile(0, []byte("some-data"))
			Expect(err).To(HaveOccurred())
			Expect(mockFileProvider.writerNameCh).ToNot(Receive())
		})

		It("Writes to the correct writer", func() {
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

			By("Writing to the same file twice")
			offset, err := fileController.WriteToFile(fileId1, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(len(expectedData)))
			Expect(readEntireFile(tmpDir, "some-name-1")).To(Equal(expectedData))

			offset, err = fileController.WriteToFile(fileId1, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(2 * len(expectedData)))
			Expect(readEntireFile(tmpDir, "some-name-1")).To(Equal(append(expectedData, expectedData...)))

			By("Writing to a different file")
			offset, err = fileController.WriteToFile(fileId2, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(len(expectedData)))
			Expect(readEntireFile(tmpDir, "some-name-2")).To(Equal(expectedData))
		})
	})

	Context("ReadFromFile", func() {
		It("returns an error for an unknown file ID", func() {
			_, err := fileController.ReadFromFile(0)
			Expect(err).To(HaveOccurred())
			Expect(mockFileProvider.writerNameCh).ToNot(Receive())
		})

		It("Reads from the correct file", func() {
			populateLocalOrch(mockOrchestrator)
			mockFileProvider.writerCh <- nil
			file := createFile(tmpDir, "some-name-1")
			mockFileProvider.readerCh <- file
			expectedData := []byte("some-data")

			writeToFile(file, expectedData, 0, 0)

			var fileId1 uint64 = 3
			ffErr := fileController.FetchFile(fileId1, "some-name-1")
			Expect(ffErr).To(BeNil())
			Expect(mockFileProvider.writerNameCh).To(Receive(Equal("some-name-1")))

			By("Reading from the first offset")
			data, err := fileController.ReadFromFile(fileId1)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal(expectedData))

			By("Reading from the next offset")
			writeToFile(file, expectedData, int64(-len(expectedData)), 2)

			data, err = fileController.ReadFromFile(fileId1)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal(expectedData))
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
