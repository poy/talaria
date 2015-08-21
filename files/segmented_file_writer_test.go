package files_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/apoydence/talaria/files"
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

	Context("Splits Files", func() {
		It("writes data to a file named 0", func(done Done) {
			defer close(done)
			expectedData := []byte("some-data")
			n, err := segmentedFile.Write(expectedData)
			Expect(n).To(Equal(len(expectedData)))
			Expect(err).ToNot(HaveOccurred())

			file, err := os.Open(path.Join(tmpDir, "0"))
			Expect(err).ToNot(HaveOccurred())

			data, err := ioutil.ReadAll(file)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal(expectedData))
		})

		It("writes data from multiple go routines", func(done Done) {
			defer close(done)
			expectedData := []byte("some-data")
			count := 5
			wg := sync.WaitGroup{}
			wg.Add(count)

			segmentedFile = files.NewSegmentedFileWriter(tmpDir, uint64(200*count*len(expectedData)), 10)

			for i := 0; i < count; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < 100; j++ {
						n, err := segmentedFile.Write(expectedData)
						Expect(n).To(Equal(len(expectedData)))
						Expect(err).ToNot(HaveOccurred())
						time.Sleep(time.Millisecond)
					}
				}()
			}

			wg.Wait()

			file, err := os.Open(path.Join(tmpDir, "0"))
			Expect(err).ToNot(HaveOccurred())

			data, err := ioutil.ReadAll(file)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(count * 100 * len(expectedData)))
		})

		It("writes data to a series of files after enough data has been written", func(done Done) {
			defer close(done)

			for i := 0; i < 100; i += 5 {
				n, err := segmentedFile.Write(expectedData[i : i+5])
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(5))
			}

			for i := 0; i < 10; i++ {
				data, err := readFromFile(path.Join(tmpDir, fmt.Sprintf("%d", i)))
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData[i*10 : (i+1)*10]))
			}
		})
	})

	Context("Deletes files", func() {
		It("keeps the number of files at the given value", func(done Done) {
			defer close(done)

			for i := 0; i < 110; i += 5 {
				n, err := segmentedFile.Write(expectedData[i : i+5])
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(5))
			}

			_, err := readFromFile(path.Join(tmpDir, fmt.Sprintf("%d", 0)))
			Expect(err).To(HaveOccurred())

			for i := 1; i < 11; i++ {
				data, err := readFromFile(path.Join(tmpDir, fmt.Sprintf("%d", i)))
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData[i*10 : (i+1)*10]))
			}
		})
	})

})

func readFromFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(file)
}
