package files_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/apoydence/talaria/files"
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

		PDescribe("SeekIndex()", func() {
			BeforeEach(func() {
				segmentedFileReader = files.NewSegmentedFileReader(tmpDir, time.Millisecond)
			})

			It("starts reading from a specific point", func(done Done) {
				defer close(done)

				for i := 0; i < 100; i++ {
					n, err := segmentedFileWriter.Write(expectedData[i : i+1])
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(1))
				}

				By("reading from the file")
				buffer := make([]byte, 1024)
				_, err := segmentedFileReader.Read(buffer)
				Expect(err).ToNot(HaveOccurred())

				By("seeking near the beginning of the file")
				err = segmentedFileReader.SeekIndex(2)
				Expect(err).ToNot(HaveOccurred())

				By("reading from the file again")
				n, err := segmentedFileReader.Read(buffer)
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(3))
				Expect(buffer[:3]).To(Equal(expectedData[2:5]))
			})
		})

	})

})
