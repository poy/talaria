package files_test

import (
	"github.com/apoydence/talaria/files"
	"github.com/gorilla/websocket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileLeader", func() {

	var (
		mockWriter      *mockWriter
		mockHttpStarter *mockHttpStarter
		replicatedFile  *files.ReplicatedFileLeader
		url             string
	)

	BeforeEach(func() {
		mockWriter = newMockWriter()
		mockHttpStarter = newMockHttpStarter()
		replicatedFile = files.NewReplicatedFileLeader(mockWriter, mockHttpStarter)
		url = "ws" + mockHttpStarter.server.URL[4:]
	})

	AfterEach(func() {
		mockHttpStarter.stop()
	})

	It("Accepts websocket connections", func() {
		_, _, err := websocket.DefaultDialer.Dial(url, nil)
		Expect(err).ToNot(HaveOccurred())
	})

	It("Writes the length and data to the websocket for each write", func(done Done) {
		defer close(done)

		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		Expect(err).ToNot(HaveOccurred())
		expectedData := []byte{1, 2, 3, 4, 5}

		n, err := replicatedFile.Write(expectedData)
		Expect(err).ToNot(HaveOccurred())
		Expect(n).To(Equal(len(expectedData)))
		Expect(mockWriter.dataChan).To(Receive(Equal(expectedData)))

		_, data, err := conn.ReadMessage()
		Expect(err).ToNot(HaveOccurred())
		Expect(data[:4]).To(Equal([]byte{5, 0, 0, 0}))
		Expect(data[4:]).To(Equal(expectedData))
	})
})
