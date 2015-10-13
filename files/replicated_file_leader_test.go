package files_test

import (
	"io/ioutil"
	"os"

	"github.com/apoydence/talaria/files"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileLeader", func() {

	var (
		tmpFile        *os.File
		clientWriter   *mockWriter
		mockWriter     *mockWriter
		replicatedFile *files.ReplicatedFileLeader
	)

	BeforeEach(func() {
		var err error
		tmpFile, err = ioutil.TempFile("", "replicated")
		Expect(err).ToNot(HaveOccurred())
		mockWriter = newMockWriter()
		clientWriter = newMockWriter()
		replicatedFile = files.NewReplicatedFileLeader(mockWriter, tmpFile)
	})

	AfterEach(func() {
		Expect(os.Remove(tmpFile.Name())).To(Succeed())
	})

	It("writes the data to the listener and waits for a success before writing to the wrapped writer", func(done Done) {
		defer close(done)
		clientWriter.dataChan = make(chan []byte)
		replicatedFile.UpdateWriter(clientWriter)

		expectedData := []byte("some-data")
		go replicatedFile.Write(expectedData)

		By("waiting for the listeners to respond")
		Consistently(mockWriter.dataChan).ShouldNot(Receive())

		Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedData)))
		Eventually(mockWriter.dataChan).Should(Receive(Equal(expectedData)))
	}, 5)

	It("writes any data available before accepting writes", func(done Done) {
		defer close(done)

		By("writing pre-data")
		expectedPreData := []byte("some-pre-data")
		tmpFile.Write(expectedPreData)
		tmpFile.Sync()

		replicatedFile.UpdateWriter(clientWriter)

		expectedData := []byte("some-data")
		go replicatedFile.Write(expectedData)

		By("reading pre-data")
		Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedPreData)))

		Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedData)))
		Eventually(mockWriter.dataChan).Should(Receive(Equal(expectedData)))
	}, 5)
})
