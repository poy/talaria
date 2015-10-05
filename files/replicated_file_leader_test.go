package files_test

import (
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/apoydence/talaria/files"
	"github.com/apoydence/talaria/messages"
	"github.com/gorilla/websocket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicatedFileLeader", func() {

	var (
		tmpFile         *os.File
		mockWriter      *mockWriter
		mockHttpStarter *mockHttpStarter
		replicatedFile  *files.ReplicatedFileLeader
		url             string
	)

	BeforeEach(func() {
		var err error
		tmpFile, err = ioutil.TempFile("", "replicated")
		Expect(err).ToNot(HaveOccurred())
		mockWriter = newMockWriter()
		mockHttpStarter = newMockHttpStarter()
		replicatedFile = files.NewReplicatedFileLeader(mockWriter, tmpFile, mockHttpStarter)
		url = "ws" + mockHttpStarter.server.URL[4:]
	})

	AfterEach(func() {
		mockHttpStarter.stop()
		Expect(os.Remove(tmpFile.Name())).To(Succeed())
	})

	It("writes the data to the listener and waits for a success before writing to the wrapped writer", func(done Done) {
		defer close(done)
		var wg sync.WaitGroup
		defer wg.Wait()

		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		Expect(err).ToNot(HaveOccurred())
		expectedData := []byte("some-data")

		wg.Add(1)
		go writeMessage(replicatedFile, expectedData, &wg)

		By("waiting for the listeners to respond")
		Consistently(mockWriter.dataChan).ShouldNot(Receive())

		checkMessageFromLeader(conn, expectedData)
		Eventually(mockWriter.dataChan).Should(Receive(Equal(expectedData)))
	})

	It("accepts multiple clients", func(done Done) {
		defer close(done)
		var wg sync.WaitGroup
		defer wg.Wait()

		var conns []*websocket.Conn
		count := 3
		for i := 0; i < count; i++ {
			conn, _, err := websocket.DefaultDialer.Dial(url, nil)
			Expect(err).ToNot(HaveOccurred())
			conns = append(conns, conn)
		}

		expectedData := []byte{1, 2, 3, 4, 5}

		wg.Add(1)
		go writeMessage(replicatedFile, expectedData, &wg)

		for _, conn := range conns {
			checkMessageFromLeader(conn, expectedData)
		}
	})

	It("writes any data available before accepting writes", func(done Done) {
		defer close(done)
		var wg sync.WaitGroup
		defer wg.Wait()

		By("writing pre-data")
		expectedPreData := []byte("some-pre-data")
		tmpFile.Write(expectedPreData)
		tmpFile.Sync()

		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		Expect(err).ToNot(HaveOccurred())
		expectedData := []byte("some-data")

		By("reading pre-data")
		checkMessageFromLeader(conn, expectedPreData)

		wg.Add(1)
		go writeMessage(replicatedFile, expectedData, &wg)

		By("waiting for the listeners to respond")
		Consistently(mockWriter.dataChan).ShouldNot(Receive())

		checkMessageFromLeader(conn, expectedData)
		Eventually(mockWriter.dataChan).Should(Receive(Equal(expectedData)))
	})
})

func writeMessage(writer io.Writer, data []byte, wg *sync.WaitGroup) {
	defer GinkgoRecover()
	defer wg.Done()
	n, err := writer.Write(data)
	Expect(err).ToNot(HaveOccurred())
	Expect(n).To(Equal(len(data)))
}

func checkMessageFromLeader(conn *websocket.Conn, expectedData []byte) {
	_, data, err := conn.ReadMessage()
	Expect(err).ToNot(HaveOccurred())
	Expect(data).To(Equal(expectedData))
	err = conn.WriteMessage(websocket.BinaryMessage, buildFileOffset())
	Expect(err).ToNot(HaveOccurred())
}

func buildFileOffset() []byte {
	msg := &messages.Server{
		MessageType: messages.Server_FileOffset.Enum(),
		FileOffset:  &messages.FileOffset{},
	}

	data, err := msg.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}
