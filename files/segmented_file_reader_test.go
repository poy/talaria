package files_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/apoydence/talaria/files"
	"github.com/apoydence/talaria/logging"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SegmentedFileReader", func() {
	var (
		tmpDir       string
		expectedData []byte

		segmentedFileReader *files.SegmentedFileReader
		segmentedFileWriter *files.SegmentedFileWriter
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		segmentedFileWriter = files.NewSegmentedFileWriter(tmpDir, 10, 10)

		for i := 0; i < 100; i++ {
			expectedData = append(expectedData, byte(i))
		}
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("Read()", func() {

		Context("Millisecond Poll Time", func() {
			BeforeEach(func() {
				segmentedFileReader = files.NewSegmentedFileReader(tmpDir, time.Millisecond)
			})

			It("reads data from a file", func(done Done) {
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

			It("reads from a segmented file", func(done Done) {
				defer close(done)

				for i := 0; i < 20; i += 2 {
					n, err := segmentedFileWriter.Write(expectedData[i : i+2])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(2))
				}

				for i := 0; i < 20; i += 2 {
					buffer := make([]byte, 1024)
					n, err := segmentedFileReader.Read(buffer)
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(2))
					Expect(buffer[:n]).To(Equal(expectedData[i : i+2]))
				}
			})

			It("finds the correct file to read from", func(done Done) {
				defer close(done)

				By("segment 0 will be deleted")
				for i := 0; i < 22; i += 2 {
					n, err := segmentedFileWriter.Write(expectedData[i : i+2])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(2))
				}

				buffer := make([]byte, 1024)
				n, err := segmentedFileReader.Read(buffer)
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(2))
				Expect(buffer[:n]).To(Equal(expectedData[2:4]))
			})

			It("polls a file", func(done Done) {
				defer close(done)

				segmentedFileWriter2 := files.NewSegmentedFileWriter(tmpDir, 100, 10)

				go func() {
					for i := 0; i < 5; i++ {
						time.Sleep(100 * time.Millisecond)
						n, err := segmentedFileWriter2.Write(expectedData[i : i+1])
						Expect(err).ToNot(HaveOccurred())
						Expect(n).To(Equal(1))
						By(fmt.Sprintf("Writing %d times", i))
					}
				}()

				buffer := make([]byte, 1024)
				var results []byte

				for len(results) < 5 {
					By(fmt.Sprintf("Reading %d times", len(results)))
					n, err := segmentedFileReader.Read(buffer)
					Expect(err).ToNot(HaveOccurred())
					results = append(results, buffer[:n]...)
				}
				Expect(results).To(Equal(expectedData[:5]))
			}, 5)
		})

		Context("0 Poll Time", func() {
			BeforeEach(func() {
				segmentedFileReader = files.NewSegmentedFileReader(tmpDir, 0)
			})

			It("returns an EOF when polling time is set to 0", func(done Done) {
				defer close(done)

				n, err := segmentedFileWriter.Write(expectedData[:5])
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(5))

				buffer := make([]byte, 1024)
				n, err = segmentedFileReader.Read(buffer)
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(5))
				Expect(buffer[:5]).To(Equal(expectedData[:5]))

				n, err = segmentedFileReader.Read(buffer)
				Expect(err).To(MatchError(io.EOF))
			})
		})

		Describe("SeekIndex()", func() {
			BeforeEach(func() {
				logging.SetLevel(logging.DEBUG)
				segmentedFileWriter = files.NewSegmentedFileWriter(tmpDir, 20, 10)
				segmentedFileReader = files.NewSegmentedFileReader(tmpDir, time.Millisecond)
			})

			It("seeks to each point", func(done Done) {
				defer close(done)
				count := 5

				for i := 0; i < count; i++ {
					By(fmt.Sprintf("writing index %d to the file", i))
					n, err := segmentedFileWriter.Write(expectedData[i*2 : i*2+2])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(2))
				}

				buffer := make([]byte, 1024)
				for i := 0; i < count; i++ {
					By(fmt.Sprintf("seeking to index %d", i))
					err := segmentedFileReader.SeekIndex(uint64(i))
					Expect(err).ToNot(HaveOccurred())

					By(fmt.Sprintf("reading from the file at index %d", i))
					n, err := segmentedFileReader.Read(buffer)
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(2))
					fmt.Println(buffer[:n])
					Expect(buffer[:n]).To(Equal(expectedData[i*2 : i*2+2]))
				}

			})

			It("returns an error when seeking beyond the file", func(done Done) {
				defer close(done)
				count := 5

				for i := 0; i < count; i++ {
					By(fmt.Sprintf("writing index %d to the file", i))
					fmt.Println(expectedData[i*2 : i*2+2])
					n, err := segmentedFileWriter.Write(expectedData[i*2 : i*2+2])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(2))
				}

				err := segmentedFileReader.SeekIndex(uint64(count + 1))
				Expect(err).To(HaveOccurred())
			})
		})

	})

})
