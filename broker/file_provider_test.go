package broker_test

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("FileProvider", func() {
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

	Context("ProvideWriter", func() {
		It("Provides the same writer for each unique name", func() {
			writer1 := fileProvider.ProvideWriter("some-name-1")
			writer2 := fileProvider.ProvideWriter("some-name-1")
			writer3 := fileProvider.ProvideWriter("some-name-2")

			writer1.Write([]byte("some-data"))

			Expect(writer1).To(Equal(writer2))
			Expect(writer1).ToNot(Equal(writer3))
		})
	})

	Context("ProvideReader", func() {
		It("Provides a unique reader each time", func() {
			writer := fileProvider.ProvideWriter("some-name-1")
			writer.Write([]byte("some-data"))

			reader1 := fileProvider.ProvideReader("some-name-1")
			reader2 := fileProvider.ProvideReader("some-name-1")
			reader1.Read(make([]byte, 1))

			Expect(reader1).ToNot(Equal(reader2))
		})
	})
})
