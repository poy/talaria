package broker_test

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("FileController", func() {
	var (
		tmpDir           string
		mockFileProvider *mockFileProvider
		fileController   *broker.FileController
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		mockFileProvider = newMockFileProvider()
		fileController = broker.NewFileController(mockFileProvider)
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
			fileIdA, err := fileController.FetchFile("some-name-1")
			Expect(err).ToNot(HaveOccurred())

			fileIdB, err := fileController.FetchFile("some-name-1")
			Expect(err).ToNot(HaveOccurred())
			Expect(fileIdA).To(Equal(fileIdB))

			By("Giving a different ID for a different name")
			fileIdC, err := fileController.FetchFile("some-name-2")
			Expect(err).ToNot(HaveOccurred())
			Expect(fileIdC).ToNot(Equal(fileIdA))
		})
	})

	Context("WriteToFile", func() {
		It("Returns an error for an unknown file ID", func() {
			_, err := fileController.WriteToFile(0, []byte("some-data"))
			Expect(err).To(HaveOccurred())
			Expect(mockFileProvider.writerNameCh).ToNot(Receive())
		})

		It("Writes to the correct writer", func() {
			mockFileProvider.writerCh <- createFile(tmpDir, "some-name-1")
			mockFileProvider.writerCh <- createFile(tmpDir, "some-name-2")
			mockFileProvider.readerCh <- nil
			mockFileProvider.readerCh <- nil
			expectedData := []byte("some-data")

			fileId1, err := fileController.FetchFile("some-name-1")
			Expect(err).ToNot(HaveOccurred())
			Expect(mockFileProvider.writerNameCh).To(Receive(Equal("some-name-1")))

			fileId2, err := fileController.FetchFile("some-name-2")
			Expect(err).ToNot(HaveOccurred())
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
		It("Returns an error for an unknown file ID", func() {
			_, _, err := fileController.ReadFromFile(0, 99)
			Expect(err).To(HaveOccurred())
			Expect(mockFileProvider.writerNameCh).ToNot(Receive())
		})

		It("Reads from the correct file", func() {
			mockFileProvider.writerCh <- nil
			file := createFile(tmpDir, "some-name-1")
			mockFileProvider.readerCh <- file
			expectedData := []byte("some-data")

			writeToFile(file, expectedData, 0, 0)

			fileId1, err := fileController.FetchFile("some-name-1")
			Expect(err).ToNot(HaveOccurred())
			Expect(mockFileProvider.writerNameCh).To(Receive(Equal("some-name-1")))

			By("Reading from the first offset")
			data, offset, err := fileController.ReadFromFile(fileId1, broker.FirstOffset)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(len(expectedData)))
			Expect(data).To(Equal(expectedData))

			By("Reading from the first offset again")
			data, offset, err = fileController.ReadFromFile(fileId1, broker.FirstOffset)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(len(expectedData)))
			Expect(data).To(Equal(expectedData))

			By("Reading from the next offset")
			writeToFile(file, expectedData, int64(-len(expectedData)), 2)

			data, offset, err = fileController.ReadFromFile(fileId1, broker.NextOffset)
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeEquivalentTo(2 * len(expectedData)))
			Expect(data).To(Equal(expectedData))
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
