package broker_test

import (
	"fmt"
	"net/http/httptest"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/messages"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Broker", func() {
	var (
		mockControllerProvider *mockControllerProvider
		mockController         *mockController
		recorder               *httptest.ResponseRecorder
		server                 *httptest.Server
		handler                *broker.Broker
		wsUrl                  string
	)

	BeforeEach(func() {
		mockControllerProvider = newMockControllerProvider()
		mockController = newMockController()
		recorder = httptest.NewRecorder()
		handler = broker.NewBroker(mockControllerProvider)
		server = httptest.NewServer(handler)
		wsUrl = "ws" + server.URL[4:]
		mockControllerProvider.controllerCh <- mockController
	})

	AfterEach(func() {
		server.CloseClientConnections()
		server.Close()
	})

	It("Accepts websocket connections and grabs a controller", func() {
		_, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(mockControllerProvider.controllerCh).To(BeEmpty())
	})

	Context("FetchFile", func() {

		It("Reports an error", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.fetchFileIdCh <- 0
			mockController.fetchFileErrCh <- broker.NewFetchFileError("some-error", "")

			fetchFile := buildFetchFile(99, "some-file")
			data, err := proto.Marshal(fetchFile)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))

			_, data, err = conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_Error))
			Expect(server.Error).ToNot(BeNil())
			Expect(server.FileLocation).To(BeNil())
			Expect(server.Error.GetMessage()).To(Equal("some-error"))
		})

		It("Reports the file ID", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.fetchFileIdCh <- 8
			mockController.fetchFileErrCh <- nil

			fetchFile := buildFetchFile(99, "some-file")
			data, err := proto.Marshal(fetchFile)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))

			_, data, err = conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())

			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_FileLocation))
			Expect(server.Error).To(BeNil())
			Expect(server.FileLocation).ToNot(BeNil())
		})
	})

	Context("WriteToFile", func() {

		It("returns an error if there is one", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.writeFileOffsetCh <- 0
			mockController.writeFileErrCh <- fmt.Errorf("some-error")

			expectedData := []byte("some-data")
			writeToFile := buildWriteToFile(99, 8, expectedData)
			data, err := proto.Marshal(writeToFile)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.writeFileIdCh).Should(Receive(BeEquivalentTo(8)))
			Eventually(mockController.writeFileDataCh).Should(Receive(Equal(expectedData)))

			_, data, err = conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_Error))
			Expect(server.Error).ToNot(BeNil())
			Expect(server.Error.GetMessage()).To(Equal("some-error"))
		})

		It("returns the new offset", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.writeFileOffsetCh <- 77
			mockController.writeFileErrCh <- nil

			expectedData := []byte("some-data")
			writeToFile := buildWriteToFile(99, 8, expectedData)
			data, err := proto.Marshal(writeToFile)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.writeFileIdCh).Should(Receive(BeEquivalentTo(8)))
			Eventually(mockController.writeFileDataCh).Should(Receive(Equal(expectedData)))

			_, data, err = conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_FileOffset))
			Expect(server.FileOffset).ToNot(BeNil())
			Expect(server.FileOffset.GetOffset()).To(BeEquivalentTo(77))
		})
	})

	Context("ReadFromFile", func() {
		It("returns an error if there is one", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.readFileDataCh <- nil
			mockController.readFileErrCh <- fmt.Errorf("some-error")

			readFromFile := buildReadFromFile(99, 8)
			data, err := proto.Marshal(readFromFile)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.readFileIdCh).Should(Receive(BeEquivalentTo(8)))

			_, data, err = conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_Error))
			Expect(server.Error).ToNot(BeNil())
			Expect(server.Error.GetMessage()).To(Equal("some-error"))
		})

		It("returns the data", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			expectedData := []byte("some-data")
			mockController.readFileDataCh <- expectedData
			mockController.readFileErrCh <- nil

			readFromFile := buildReadFromFile(99, 8)
			data, err := proto.Marshal(readFromFile)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.readFileIdCh).Should(Receive(BeEquivalentTo(8)))

			_, data, err = conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_ReadData))
			Expect(server.ReadData).ToNot(BeNil())
			Expect(server.ReadData.GetData()).To(Equal(expectedData))
		})
	})
})

func buildFetchFile(id uint64, name string) *messages.Client {
	messageType := messages.Client_FetchFile
	return &messages.Client{
		MessageType: &messageType,
		MessageId:   &id,
		FetchFile: &messages.FetchFile{
			Name: &name,
		},
	}
}

func buildWriteToFile(msgId, fileId uint64, data []byte) *messages.Client {
	messageType := messages.Client_WriteToFile
	return &messages.Client{
		MessageType: &messageType,
		MessageId:   &msgId,
		WriteToFile: &messages.WriteToFile{
			FileId: &fileId,
			Data:   data,
		},
	}
}

func buildReadFromFile(msgId, fileId uint64) *messages.Client {
	messageType := messages.Client_ReadFromFile
	return &messages.Client{
		MessageType: &messageType,
		MessageId:   &msgId,
		ReadFromFile: &messages.ReadFromFile{
			FileId: &fileId,
		},
	}
}
