package datapipeline_test

import (
	"encoding/binary"
	"github.com/apoydence/talaria/datapipeline"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FileWriter", func() {
	var (
		fileWriter  *datapipeline.FileWriter
		mockFileGen mockFileGenerator
		fileBuffers [][]byte
		closeChan   chan int
	)

	BeforeEach(func() {
		fileBuffers = make([][]byte, 0)
		fileIndex := 0
		closeChan = make(chan int, 5)

		mockFileGen = mockFileGenerator(func() datapipeline.WriteSeekCloser {
			buffer := make([]byte, 1024)
			fileBuffers = append(fileBuffers, buffer)
			m := newMockFile(buffer, fileIndex, closeChan)
			fileIndex++
			return m
		})
		fileWriter = datapipeline.NewFileWriter(mockFileGen, 1024)
	})

	It("should write the count in the first 4 bytes", func() {
		count := 4
		data := make([]byte, 508)
		for i := 0; i < len(data); i++ {
			data[i] = byte(i)
		}

		for i := 0; i < count; i++ {
			n, err := fileWriter.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))
		}

		Expect(fileBuffers).To(HaveLen(2))
		buf := fileBuffers[0]
		length := binary.LittleEndian.Uint32(buf)
		Expect(length).To(BeEquivalentTo(2))
		Expect(buf[4 : len(data)+4]).To(BeEquivalentTo(data))
	})

	It("should close a file once it's done with it", func() {
		count := 4
		data := make([]byte, 508)
		for i := 0; i < len(data); i++ {
			data[i] = byte(i)
		}

		for i := 0; i < count; i++ {
			n, err := fileWriter.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))
		}

		Expect(closeChan).To(Receive())
	})
})
