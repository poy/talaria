package files_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/apoydence/talaria/files"
	"github.com/apoydence/talaria/pb/filemeta"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SegmentedFileWriter", func() {
	var (
		segmentedFile *files.SegmentedFileWriter
		tmpDir        string
		expectedData  []byte
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())
		segmentedFile = files.NewSegmentedFileWriter(tmpDir, 10, 10)

		for i := 0; i < 110; i++ {
			expectedData = append(expectedData, byte(i))
		}
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("Write()", func() {
		Describe("Splits Files", func() {
			It("writes data to a file named 0", func(done Done) {
				defer close(done)
				expectedData := []byte("some-data")
				n, err := segmentedFile.Write(expectedData)
				Expect(n).To(Equal(len(expectedData)))
				Expect(err).ToNot(HaveOccurred())

				file, err := os.Open(path.Join(tmpDir, "0"))
				Expect(err).ToNot(HaveOccurred())

				_, err = file.Seek(8, 0)
				Expect(err).ToNot(HaveOccurred())
				chunkedReader := files.NewChunkedFileReader(file)
				buffer := make([]byte, 1024)
				n, err = chunkedReader.Read(buffer)
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(len(expectedData)))

				Expect(buffer[:n]).To(Equal(expectedData))
			})

			It("writes data from multiple go routines", func(done Done) {
				defer close(done)
				expectedData := []byte("some-data")
				count := 5
				var wg sync.WaitGroup
				wg.Add(count)

				segmentedFile2 := files.NewSegmentedFileWriter(tmpDir, uint64(200*count*(len(expectedData))+8), 100)

				for i := 0; i < count; i++ {
					go func() {
						defer wg.Done()
						for j := 0; j < 100; j++ {
							n, err := segmentedFile2.Write(expectedData)
							Expect(n).To(Equal(len(expectedData)))
							Expect(err).ToNot(HaveOccurred())
							time.Sleep(time.Millisecond)
						}
					}()
				}

				wg.Wait()

				file, err := os.Open(path.Join(tmpDir, "0"))
				Expect(err).ToNot(HaveOccurred())
				verifyData(expectedData, file)
			})

			It("writes meta data", func(done Done) {
				defer close(done)

				for i := 0; i < 15; i += 5 {
					n, err := segmentedFile.Write(expectedData[i : i+5])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(5))
				}

				validateMeta := func(data []byte, expectedIndex, expectedCount uint64) {
					Expect(binary.LittleEndian.Uint64(data[:8])).To(Equal(uint64(21)))
					meta := new(filemeta.FileMeta)
					Expect(meta.Unmarshal(data[21:])).To(Succeed())
					Expect(meta.GetStartingIndex()).To(Equal(expectedIndex))
					Expect(meta.GetCount()).To(Equal(expectedCount))
				}

				for i := 0; i < 2; i++ {
					data, err := readFromFile(path.Join(tmpDir, fmt.Sprintf("%d", i)))
					Expect(err).ToNot(HaveOccurred())
					validateMeta(data, uint64(i), 1)
				}
			})

			It("writes data to a series of files after enough data has been written", func(done Done) {
				defer close(done)

				for i := 0; i < 15; i += 5 {
					n, err := segmentedFile.Write(expectedData[i : i+5])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(5))
				}

				for i := 0; i < 3; i++ {
					file, err := os.Open(path.Join(tmpDir, fmt.Sprintf("%d", i)))
					Expect(err).ToNot(HaveOccurred())

					verifyData(expectedData[i*5:i*5+5], file)
				}
			})

			It("reports the updated LastIndex()", func() {
				for i := 0; i < 15; i += 5 {
					segmentedFile.Write(expectedData[i : i+5])
				}

				Expect(segmentedFile.LastIndex()).To(BeEquivalentTo(3))
			})
		})

		Context("Deletes files", func() {
			It("keeps the number of files at the given value", func(done Done) {
				defer close(done)

				By("writing 10 bytes at a time (2 data + 8 length)")
				for i := 0; i < 22; i += 2 {
					n, err := segmentedFile.Write(expectedData[i : i+2])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(2))
				}

				_, err := readFromFile(path.Join(tmpDir, fmt.Sprintf("%d", 0)))
				Expect(err).To(HaveOccurred())

				for i := 1; i < 11; i++ {
					_, err = readFromFile(path.Join(tmpDir, fmt.Sprintf("%d", i)))
					Expect(err).ToNot(HaveOccurred())
				}
			})
		})

	})

	Describe("InitWriteIndex()", func() {
		var validateMeta = func(data []byte, expectedIndex, expectedCount, length uint64) {
			Expect(binary.LittleEndian.Uint64(data[:8])).To(Equal(uint64(length)))
			meta := new(filemeta.FileMeta)
			Expect(meta).ToNot(BeNil())
			Expect(meta.Unmarshal(data[length:])).To(Succeed())
			Expect(meta.GetStartingIndex()).To(Equal(expectedIndex))
			Expect(meta.GetCount()).To(Equal(expectedCount))
		}

		JustBeforeEach(func() {
			for i := 0; i < 15; i += 5 {
				n, err := segmentedFile.Write(expectedData[i : i+5])
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(5))
			}
		})

		Context("InitWriteIndex() not used", func() {
			It("writes meta data", func(done Done) {
				defer close(done)

				for i := 0; i < 2; i++ {
					data, err := readFromFile(path.Join(tmpDir, fmt.Sprintf("%d", i)))
					Expect(err).ToNot(HaveOccurred())
					validateMeta(data, uint64(i), 1, 21)
				}
			})
		})

		Context("InitWriteIndex() used", func() {

			var (
				expectedIndex uint64
			)

			BeforeEach(func() {
				expectedIndex = 1000
				segmentedFile.InitWriteIndex(int64(expectedIndex), expectedData[5:10])
			})

			It("writes the init data", func(done Done) {
				defer close(done)

				By("skipping the first file because it is just meta")
				file, err := os.Open(path.Join(tmpDir, "1"))
				Expect(err).ToNot(HaveOccurred())
				_, err = file.Seek(8, 0)
				Expect(err).ToNot(HaveOccurred())

				chunkedReader := files.NewChunkedFileReader(file)
				buffer := make([]byte, 1024)
				n, err := chunkedReader.Read(buffer)
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(5))
				Expect(buffer[:n]).To(Equal(expectedData[5:10]))
			})

			It("writes the expected meta data to the first file", func(done Done) {
				defer close(done)

				data, err := readFromFile(path.Join(tmpDir, "0"))
				Expect(err).ToNot(HaveOccurred())
				validateMeta(data, expectedIndex, 0, 8)
			})

			It("writes the expected data to the second file", func(done Done) {
				defer close(done)

				data, err := readFromFile(path.Join(tmpDir, "1"))
				Expect(err).ToNot(HaveOccurred())
				validateMeta(data, expectedIndex, 1, 21)
			})

			It("writes the expected data to the third file", func(done Done) {
				defer close(done)

				data, err := readFromFile(path.Join(tmpDir, "2"))
				Expect(err).ToNot(HaveOccurred())
				validateMeta(data, expectedIndex+1, 1, 21)
			})

			It("reports LastIndex() as adjusted", func() {
				Expect(segmentedFile.LastIndex()).To(Equal(expectedIndex + 4))
			})
		})
	})
})

func verifyData(expectedData []byte, file *os.File) {
	_, err := file.Seek(8, 0)
	Expect(err).ToNot(HaveOccurred())
	chunkedReader := files.NewChunkedFileReader(file)

	buffer := make([]byte, 1024)
	n, err := chunkedReader.Read(buffer)

	Expect(err).ToNot(HaveOccurred())
	Expect(n).To(Equal(len(expectedData)))
	Expect(buffer[:n]).To(Equal(expectedData))
}

func readFromFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(file)
}
