package broker_test

import (
	"fmt"
	"net/http/httptest"
	"sync"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/pb/messages"
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
	})

	It("Accepts websocket connections and grabs a controller", func() {
		_, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(mockControllerProvider.controllerCh).To(BeEmpty())
	})

	Describe("FetchFile()", func() {

		It("reports an error", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.fetchFileIdCh <- 0
			mockController.fetchFileErrCh <- broker.NewConnectionError("some-error", "", false)

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

		It("reports the file ID", func(done Done) {
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
			Expect(server.FileLocation.GetLocal()).To(BeTrue())
		})

		It("reports the redirected URI", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			expectedUri := "http://some.url"
			mockController.fetchFileIdCh <- 8
			mockController.fetchFileErrCh <- broker.NewConnectionError("some-error", expectedUri, false)

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
			Expect(server.FileLocation.GetUri()).To(Equal(expectedUri))
			Expect(server.FileLocation.GetLocal()).To(BeFalse())
		})
	})

	Describe("WriteToFile()", func() {

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

		Measure("Writes to a file 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				count := 1000
				go func() {
					for i := 0; i < count; i++ {
						mockController.writeFileOffsetCh <- 77
						mockController.writeFileErrCh <- nil
					}
				}()

				go func() {
					for _ = range mockController.writeFileIdCh {
						//NOP
					}
				}()

				go func() {
					for _ = range mockController.writeFileDataCh {
						//NOP
					}
				}()

				conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
				Expect(err).ToNot(HaveOccurred())

				expectedData := []byte("some-data")
				writeToFile := buildWriteToFile(99, 8, expectedData)
				data, err := proto.Marshal(writeToFile)
				Expect(err).ToNot(HaveOccurred())

				for i := 0; i < count; i++ {
					conn.WriteMessage(websocket.BinaryMessage, data)
					_, _, err = conn.ReadMessage()
				}
				mockController.closeChannels()
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))
		}, 5)
	})

	Describe("ReadFromFile()", func() {

		var (
			conn         *websocket.Conn
			expectedData []byte
		)

		BeforeEach(func() {
			expectedData = []byte("some-data")

			var err error
			conn, _, err = websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())
		})

		It("returns an error if there is one", func(done Done) {
			defer close(done)

			mockController.readFileDataCh <- nil
			mockController.readOffsetCh <- 0
			mockController.readFileErrCh <- fmt.Errorf("some-error")

			Expect(conn.WriteMessage(websocket.BinaryMessage, buildReadFromFile(99, 8))).To(Succeed())

			Eventually(mockController.readFileIdCh).Should(Receive(BeEquivalentTo(8)))

			_, data, err := conn.ReadMessage()
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

			mockController.readFileDataCh <- expectedData
			mockController.readOffsetCh <- 1010
			mockController.readFileErrCh <- nil

			Expect(conn.WriteMessage(websocket.BinaryMessage, buildReadFromFile(99, 8))).To(Succeed())

			Eventually(mockController.readFileIdCh).Should(Receive(BeEquivalentTo(8)))

			_, data, err := conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_ReadData))
			Expect(server.ReadData).ToNot(BeNil())
			Expect(server.ReadData.GetData()).To(Equal(expectedData))
			Expect(server.ReadData.GetOffset()).To(BeEquivalentTo(1010))
		})

		Measure("Reads from a file 1000 times in under a second", func(b Benchmarker) {
			var wg sync.WaitGroup
			wg.Add(2)
			defer wg.Wait()

			runtime := b.Time("runtime", func() {
				count := 1000
				go func() {
					for i := 0; i < count; i++ {
						mockController.readFileDataCh <- expectedData
						mockController.readOffsetCh <- int64(i)
						mockController.readFileErrCh <- nil
					}
				}()

				go func() {
					defer wg.Done()
					for _ = range mockController.readFileIdCh {
						//NOP
					}
				}()

				go func() {
					defer wg.Done()
					for _ = range mockController.writeFileDataCh {
						//NOP
					}
				}()

				readFromFile := buildReadFromFile(99, 8)
				for i := 0; i < count; i++ {
					conn.WriteMessage(websocket.BinaryMessage, readFromFile)
					_, _, err := conn.ReadMessage()
					Expect(err).ToNot(HaveOccurred())
				}
				mockController.closeChannels()
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))
		}, 5)

	})

	Describe("InitWriteIndex()", func() {
		It("returns an error if there is one", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.initOffsetCh <- 0
			mockController.initErrCh <- fmt.Errorf("some-error")

			initWriteIndex := buildInitWriteIndex(99, 8, 101, []byte("some-data"))
			data, err := proto.Marshal(initWriteIndex)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.initIdCh).Should(Receive(BeEquivalentTo(8)))

			_, data, err = conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := &messages.Server{}
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(99))
			Expect(server.GetMessageType()).To(Equal(messages.Server_Error))
			Expect(server.Error).ToNot(BeNil())
			Expect(server.Error.GetMessage()).To(Equal("some-error"))
		}, 3)

		It("returns the new offset", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.initOffsetCh <- 77
			mockController.initErrCh <- nil

			expectedData := []byte("some-data")
			initWriteIndex := buildInitWriteIndex(99, 8, 101, expectedData)
			data, err := proto.Marshal(initWriteIndex)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)

			Eventually(mockController.initIdCh).Should(Receive(BeEquivalentTo(8)))
			Eventually(mockController.initIndexCh).Should(Receive(BeEquivalentTo(101)))
			Eventually(mockController.initDataCh).Should(Receive(Equal(expectedData)))

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

func buildReadFromFile(msgId, fileId uint64) []byte {
	messageType := messages.Client_ReadFromFile
	msg := &messages.Client{
		MessageType: &messageType,
		MessageId:   &msgId,
		ReadFromFile: &messages.ReadFromFile{
			FileId: &fileId,
		},
	}

	data, err := proto.Marshal(msg)
	Expect(err).ToNot(HaveOccurred())
	return data
}

func buildInitWriteIndex(msgId, fileId uint64, index int64, data []byte) *messages.Client {
	messageType := messages.Client_InitWriteIndex
	return &messages.Client{
		MessageType: messageType.Enum(),
		MessageId:   proto.Uint64(msgId),
		InitWriteIndex: &messages.InitWriteIndex{
			FileId: proto.Uint64(fileId),
			Index:  proto.Int64(index),
			Data:   data,
		},
	}
}
