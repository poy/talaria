package files_test

import (
	"github.com/apoydence/talaria/files"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileClient", func() {
	var (
		mockWriterLeader     *mockWriter
		mockWriterClient     *mockWriter
		replicatedFileClient *files.ReplicatedFileClient
		mockHttpStarter      *mockHttpStarter
		replicatedFileLeader *files.ReplicatedFileLeader
		url                  string
	)

	BeforeEach(func() {
		mockWriterClient = newMockWriter()
		mockWriterLeader = newMockWriter()

		mockHttpStarter = newMockHttpStarter()
		replicatedFileLeader = files.NewReplicatedFileLeader(mockWriterLeader, mockHttpStarter)
		url = "ws" + mockHttpStarter.server.URL[4:]
		replicatedFileClient = files.NewReplicatedFileClient(url, mockWriterClient)
	})

	It("writes data as receieved", func() {
		expectedData := []byte("some-data")
		replicatedFileLeader.Write(expectedData)
		Eventually(mockWriterClient.dataChan).Should(Receive(Equal(expectedData)))
	})
})
