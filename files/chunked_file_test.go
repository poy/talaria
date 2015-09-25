package files_test

import (
	"bytes"
	"io"

	"github.com/apoydence/talaria/files"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ChunkedFile Reader/Writer", func() {
	var (
		wrappedReader bytes.Buffer
		reader        *files.ChunkedFileReader
		writer        *files.ChunkedFileWriter
	)

	BeforeEach(func() {
		reader = files.NewChunkedFileReader(&wrappedReader)
		writer = files.NewChunkedFileWriter(&wrappedReader)
	})

	It("Writes data with a length and reads one chunk of a file at a time", func() {
		expectedData1 := []byte("data")
		expectedData2 := []byte("some-other-data")
		writeData(expectedData1, writer)
		writeData(expectedData2, writer)

		buffer1 := make([]byte, len(expectedData1))
		buffer2 := make([]byte, len(expectedData2))

		n, err := reader.Read(buffer1)
		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(len(expectedData1)))
		Expect(buffer1[:n]).To(Equal(expectedData1))

		n, err = reader.Read(buffer2)
		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(len(expectedData2)))
		Expect(buffer2[:n]).To(Equal(expectedData2))
	})

})

func writeData(data []byte, writer io.Writer) {
	n, err := writer.Write(data)
	Expect(err).ToNot(HaveOccurred())
	Expect(n).To(Equal(len(data)))
}
