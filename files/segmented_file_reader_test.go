package files_test

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/apoydence/talaria/files"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SegmentedFileReader", func() {
	var (
		tmpDir              string
		expectedData        []byte
		segmentedFileReader *files.SegmentedFileReader
		segmentedFileWriter *files.SegmentedFileWriter
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		segmentedFileWriter = files.NewSegmentedFileWriter(tmpDir, 10, 10)
		segmentedFileReader = files.NewSegmentedFileReader(tmpDir, time.Millisecond)

		for i := 0; i < 100; i++ {
			expectedData = append(expectedData, byte(i))
		}
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("Reads data from a file", func(done Done) {
		defer close(done)

		n, err := segmentedFileWriter.Write(expectedData[:5])
		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(5))

		buffer := make([]byte, 1024)
		n, err = segmentedFileReader.Read(buffer)
		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(5))
		Expect(buffer[:5]).To(Equal(expectedData[:5]))
	})

	It("Reads from a segmented file", func(done Done) {
		defer close(done)

		for i := 0; i < 100; i += 5 {
			n, err := segmentedFileWriter.Write(expectedData[i : i+5])
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(5))
		}

		for i := 0; i < 10; i++ {
			buffer := make([]byte, 1024)
			n, err := segmentedFileReader.Read(buffer)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(10))
			Expect(buffer[:10]).To(Equal(expectedData[i*10 : i*10+10]))
		}
	})

	It("Finds the correct file to read from", func(done Done) {
		defer close(done)

		// Segment 0 will be deleted
		for i := 0; i < 110; i += 5 {
			n, err := segmentedFileWriter.Write(expectedData[i : i+5])
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(5))
		}

		buffer := make([]byte, 1024)
		n, err := segmentedFileReader.Read(buffer)
		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(10))
		Expect(buffer[:10]).To(Equal(expectedData[10:20]))
	})

	It("Polls a file", func(done Done) {
		defer close(done)

		go func() {
			for i := 0; i < 5; i++ {
				n, err := segmentedFileWriter.Write(expectedData[i : i+1])
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(1))
			}
		}()

		buffer := make([]byte, 1024)
		var results []byte
		for len(results) < 5 {
			n, err := segmentedFileReader.Read(buffer)
			Expect(err).ToNot(HaveOccurred())
			results = append(results, buffer[:n]...)
		}
		Expect(results).To(Equal(expectedData[:5]))
	})

})