package datapipeline_test

import (
	"github.com/apoydence/talaria/datapipeline"
	"io"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FileConsumer", func() {

	var (
		reader       *os.File
		fileConsumer *datapipeline.FileConsumer
	)

	BeforeEach(func() {
		var err error
		if reader, err = ioutil.TempFile("", "fct"); err != nil {
			panic(err)
		}
		fileConsumer = datapipeline.NewFileConsumer(reader.Name(), 0)
	})

	AfterEach(func() {
		reader.Close()
		os.Remove(reader.Name())
	})

	Context("Start from beginning", func() {
		It("It reads the written data with a large enough buffer", func() {
			data := []byte{0, 0, 0, 0, 4, 0, 0, 0, 4, 3, 2, 1, 2, 0, 0, 0, 6, 5, 1, 0, 0, 0, 7}
			n, err := reader.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))

			buf := make([]byte, 1024)

			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(4))
			Expect(buf[:n]).To(ConsistOf([]byte{4, 3, 2, 1}))

			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(2))
			Expect(buf[:n]).To(ConsistOf([]byte{6, 5}))

			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(1))
			Expect(buf[:n]).To(ConsistOf([]byte{7}))
		})

		It("It reads the written data with a buffer that's too small", func() {
			data := []byte{0, 0, 0, 0, 6, 0, 0, 0, 6, 5, 4, 3, 2, 1, 2, 0, 0, 0, 8, 7}
			n, err := reader.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))

			buf := make([]byte, 4)

			n, err = fileConsumer.Read(buf)
			Expect(err).To(Equal(datapipeline.MoreData))
			Expect(n).To(Equal(4))
			Expect(buf[:n]).To(ConsistOf([]byte{6, 5, 4, 3}))

			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(2))
			Expect(buf[:n]).To(ConsistOf([]byte{2, 1}))

			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(2))
			Expect(buf[:n]).To(ConsistOf([]byte{8, 7}))
		})

		It("Follows new data", func(done Done) {
			defer close(done)
			data := []byte{0, 0, 0, 0, 4, 0, 0, 0, 4, 3, 2, 1}
			n, err := reader.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))

			buf := make([]byte, 1024)
			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(4))
			Expect(buf[:n]).To(ConsistOf([]byte{4, 3, 2, 1}))

			go func() {
				defer GinkgoRecover()
				data = []byte{3, 0, 0, 0, 5, 6, 7}
				n, err = reader.Write(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(len(data)))
			}()

			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(3))
			Expect(buf[:n]).To(ConsistOf([]byte{5, 6, 7}))
		}, 2)
	})

	Context("SeekBlock", func() {
		It("It reads the written data", func(done Done) {
			defer close(done)
			data := []byte{0, 0, 0, 0, 4, 0, 0, 0, 4, 3, 2, 1, 2, 0, 0, 0, 6, 5, 1, 0, 0, 0, 7}
			n, err := reader.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))
			Expect(err).ToNot(HaveOccurred())
			fileConsumer = datapipeline.NewFileConsumer(reader.Name(), 2)

			buf := make([]byte, 1024)
			n, err = fileConsumer.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(1))
			Expect(buf[:n]).To(ConsistOf([]byte{7}))
		})

		It("Moves to end when you seek to LatestEntry", func(done Done) {
			defer close(done)
			data := []byte{0, 0, 0, 0, 4, 0, 0, 0, 4, 3, 2, 1, 2, 0, 0, 0, 6, 5, 1, 0, 0, 0, 7}
			n, err := reader.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))
			Expect(err).ToNot(HaveOccurred())
			fileConsumer = datapipeline.NewFileConsumer(reader.Name(), datapipeline.LatestEntry)

			go func() {
				data := []byte{3, 0, 0, 0, 8, 9, 10}
				n, err := reader.Write(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(len(data)))
			}()

			buf := make([]byte, 1024)
			Expect(err).ToNot(HaveOccurred())
			n, err = fileConsumer.Read(buf)
			Expect(n).To(Equal(3))
			Expect(buf[:n]).To(ConsistOf([]byte{8, 9, 10}))
		}, 5)
	})

	Context("EOF", func() {
		It("Should return an EOF if the file has been closed and the last data is read", func(done Done) {
			defer close(done)
			data := []byte{3, 0, 0, 0, 4, 0, 0, 0, 4, 3, 2, 1, 2, 0, 0, 0, 6, 5, 1, 0, 0, 0, 7}
			n, err := reader.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(data)))

			fileConsumer = datapipeline.NewFileConsumer(reader.Name(), datapipeline.LatestEntry)
			buf := make([]byte, 1024)
			Expect(err).ToNot(HaveOccurred())
			_, err = fileConsumer.Read(buf)
			Expect(err).To(Equal(io.EOF))
		})
	})
})
