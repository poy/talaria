package files_test

import (
	"io/ioutil"
	"os"

	"github.com/apoydence/talaria/files"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileClient", func() {
	var (
		tmpFile              *os.File
		mockWriterLeader     *mockWriter
		mockWriterClient     *mockWriter
		replicatedFileClient *files.ReplicatedFileClient
		mockHttpStarter      *mockHttpStarter
		replicatedFileLeader *files.ReplicatedFileLeader
		url                  string
	)

	BeforeEach(func() {
		var err error
		tmpFile, err = ioutil.TempFile("", "replicated")
		Expect(err).ToNot(HaveOccurred())
		mockWriterClient = newMockWriter()
		mockWriterLeader = newMockWriter()

		mockHttpStarter = newMockHttpStarter()
		replicatedFileLeader = files.NewReplicatedFileLeader(mockWriterLeader, tmpFile, mockHttpStarter)
		url = "ws" + mockHttpStarter.server.URL[4:]
		replicatedFileClient = files.NewReplicatedFileClient(url, mockWriterClient)
	})

	AfterEach(func() {
		Expect(os.Remove(tmpFile.Name())).To(Succeed())
	})

	It("writes data as receieved", func() {
		expectedData := []byte("some-data")
		replicatedFileLeader.Write(expectedData)
		Eventually(mockWriterClient.dataChan).Should(Receive(Equal(expectedData)))
	})
})
