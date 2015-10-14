package broker_test

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FileProvider", func() {
	var (
		tmpDir       string
		fileProvider *broker.FileProvider
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		fileProvider = broker.NewFileProvider(tmpDir, 10, 3, 100*time.Millisecond)
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("ProvideWriter", func() {
		It("provides the same writer for each unique name", func() {
			writer1 := fileProvider.ProvideWriter("some-name-1")
			Expect(writer1).ToNot(BeNil())
			writer2 := fileProvider.ProvideWriter("some-name-1")
			Expect(writer2).ToNot(BeNil())
			writer3 := fileProvider.ProvideWriter("some-name-2")
			Expect(writer3).ToNot(BeNil())

			By("writing to the file to ensure they differ")
			writer1.Write([]byte("some-data"))

			Expect(writer1).To(Equal(writer2))
			Expect(writer1).ToNot(Equal(writer3))
		})

		It("provides the correct pre-writer", func(done Done) {
			defer close(done)

			mockWriter := newMockWriter()
			expectedPreData := []byte("some-pre-data")
			expectedData := []byte("some-data")

			writer := fileProvider.ProvideWriter("some-name-1")
			writer.Write(expectedPreData)
			writer.UpdateWriter(mockWriter)
			writer.Write(expectedData)

			Eventually(mockWriter.dataCh).Should(Receive(Equal(expectedPreData)))
			Eventually(mockWriter.dataCh).Should(Receive(Equal(expectedData)))
		}, 2)
	})

	Describe("ProvideReader", func() {
		It("provides a unique reader each time", func() {
			writer := fileProvider.ProvideWriter("some-name-1")
			Expect(writer).ToNot(BeNil())
			writer.Write([]byte("some-data"))

			reader1 := fileProvider.ProvideReader("some-name-1")
			reader2 := fileProvider.ProvideReader("some-name-1")
			reader1.Read(make([]byte, 1))

			Expect(reader1).ToNot(Equal(reader2))
		})
	})
})
