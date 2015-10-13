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

			writer1.Write([]byte("some-data"))

			Expect(writer1).To(Equal(writer2))
			Expect(writer1).ToNot(Equal(writer3))
		})
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
