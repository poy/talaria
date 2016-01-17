package broker_test

import (
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/apoydence/talaria/broker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FileProvider", func() {
	var (
		expectedName1 string
		expectedName2 string

		tmpDir       string
		fileProvider *broker.FileProvider
	)

	BeforeEach(func() {
		expectedName1 = "some-name-1"
		expectedName2 = "some-name-2"

		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		fileProvider = broker.NewFileProvider(tmpDir, 10, 3, 100*time.Millisecond)
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("ProvideWriter()", func() {
		It("provides the same writer for each unique name", func() {
			writer1 := fileProvider.ProvideWriter(expectedName1)
			Expect(writer1).ToNot(BeNil())
			writer2 := fileProvider.ProvideWriter(expectedName1)
			Expect(writer2).ToNot(BeNil())
			writer3 := fileProvider.ProvideWriter(expectedName2)
			Expect(writer3).ToNot(BeNil())

			By("writing to the file to ensure they differ")
			writer1.Write([]byte("some-data"))

			Expect(writer1).To(Equal(writer2))
			Expect(writer1).ToNot(Equal(writer3))
		})
	})

	Describe("ProvideReader()", func() {
		It("provides a unique reader each time", func() {
			writer := fileProvider.ProvideWriter(expectedName1)
			Expect(writer).ToNot(BeNil())
			writer.Write([]byte("some-data"))

			reader1 := fileProvider.ProvideReader(expectedName1)
			reader2 := fileProvider.ProvideReader(expectedName1)
			reader1.Read(make([]byte, 1))

			Expect(reader1).ToNot(Equal(reader2))
		})
	})

	Describe("ProvdeLastIndex()", func() {
		Context("directory is not present", func() {
			It("returns false for OK", func() {
				_, ok := fileProvider.ProvideLastIndex(expectedName1)
				Expect(ok).To(BeFalse())
			})
		})

		Context("directory is present", func() {

			BeforeEach(func() {
				Expect(os.Mkdir(path.Join(tmpDir, expectedName1), 0777)).To(Succeed())
			})

			It("returns true for OK and the index", func() {
				index, ok := fileProvider.ProvideLastIndex(expectedName1)
				Expect(ok).To(BeTrue())
				Expect(index).To(BeEquivalentTo(0))
			})
		})

	})
})
