package datapipeline_test

import (
	"bytes"

	"encoding/binary"
	"github.com/apoydence/talaria/datapipeline"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MetaWriter", func() {
	var (
		metaWriter *datapipeline.MetaWriter
		mockWriter *writeWrapper
	)

	BeforeEach(func() {
		mockWriter = &writeWrapper{
			buffer: &bytes.Buffer{},
		}
		metaWriter = datapipeline.NewMetaWriter(mockWriter)
	})

	It("Should write the length of the data", func() {
		expectedData := []byte{1, 2, 3, 4, 5}
		expectedDataBuffer := &bytes.Buffer{}
		binary.Write(expectedDataBuffer, binary.LittleEndian, uint32(5))
		binary.Write(expectedDataBuffer, binary.LittleEndian, expectedData)
		n, err := metaWriter.Write(expectedData)

		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(4 + len(expectedData)))
		Expect(mockWriter.buffer.Bytes()).To(Equal(expectedDataBuffer.Bytes()))
		Expect(mockWriter.count).To(Equal(1))
	})
})

type writeWrapper struct {
	buffer *bytes.Buffer
	count  int
}

func (w *writeWrapper) Write(data []byte) (int, error) {
	w.count++
	return w.buffer.Write(data)
}
