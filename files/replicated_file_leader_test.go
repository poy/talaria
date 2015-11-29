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
		tmpDir         string
		segWriter      *files.SegmentedFileWriter
		segReader      *files.SegmentedFileReader
		clientWriter   *mockWriter
		mockWriter     *mockWriter
		replicatedFile *files.ReplicatedFileLeader
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("/tmp", "seg")
		Expect(err).ToNot(HaveOccurred())

		segWriter = files.NewSegmentedFileWriter(tmpDir, 10, 10)
		segReader = files.NewSegmentedFileReader(tmpDir, 0)

		Expect(err).ToNot(HaveOccurred())
		mockWriter = newMockWriter()
		clientWriter = newMockWriter()
		replicatedFile = files.NewReplicatedFileLeader(mockWriter, segReader)
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("UpdateWriter()", func() {
		var (
			expectedData    []byte
			expectedPreData []byte
		)

		BeforeEach(func() {
			expectedPreData = []byte("some-pre-data")
			expectedData = []byte("some-data")
		})

		It("writes the data to the listener and waits for a success before writing to the wrapped writer", func(done Done) {
			defer close(done)
			clientWriter.dataChan = make(chan []byte)
			replicatedFile.UpdateWriter(clientWriter)

			go replicatedFile.Write(expectedData)

			By("waiting for the listeners to respond")
			Consistently(mockWriter.dataChan).ShouldNot(Receive())

			Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedData)))
			Eventually(mockWriter.dataChan).Should(Receive(Equal(expectedData)))
		}, 5)

		It("Inits the new writer that doesn't have any data", func(done Done) {
			defer close(done)
			clientWriter.retIndexCh <- 101
			segWriter.InitWriteIndex(101, expectedPreData)

			replicatedFile.UpdateWriter(clientWriter)
			Eventually(clientWriter.indexCh).Should(Receive(BeEquivalentTo(101)))
			Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedPreData)))
		}, 5)

		It("Inits the new writer that already has data", func(done Done) {
			defer close(done)
			notExpectedData := []byte("some-data-1")
			clientWriter.retIndexCh <- 102
			segWriter.InitWriteIndex(101, expectedPreData)
			segWriter.Write(notExpectedData)
			segWriter.Write(expectedData)

			replicatedFile.UpdateWriter(clientWriter)
			Eventually(clientWriter.dataChan).Should(HaveLen(2))
			Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedData)))
		}, 5)

		It("writes any data available before accepting writes", func(done Done) {
			defer close(done)
			clientWriter.retIndexCh <- 0

			By("writing pre-data")
			segWriter.Write(expectedPreData)

			replicatedFile.UpdateWriter(clientWriter)

			expectedData := []byte("some-data")
			go replicatedFile.Write(expectedData)

			By("reading pre-data")
			Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedPreData)))

			Eventually(clientWriter.dataChan).Should(Receive(Equal(expectedData)))
			Eventually(mockWriter.dataChan).Should(Receive(Equal(expectedData)))
		}, 5)
	})

	Describe("InitWriteIndex()", func() {
		It("inits the writer", func(done Done) {
			defer close(done)
			clientWriter.dataChan = make(chan []byte)
			replicatedFile.UpdateWriter(clientWriter)

			expectedData := []byte("some-data")
			go replicatedFile.InitWriteIndex(101, expectedData)

			Eventually(mockWriter.dataChan).Should(Receive(Equal(expectedData)))
			Eventually(mockWriter.indexCh).Should(Receive(BeEquivalentTo(101)))
		}, 5)
	})
})
