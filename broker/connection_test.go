package broker_test

import (
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/pb/messages"
	"github.com/gogo/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection", func() {
	var (
		connection *broker.Connection
		server     *httptest.Server
		mockServer *mockServer
	)

	BeforeEach(func() {
		mockServer = newMockServer()
		server = httptest.NewServer(mockServer)
		var err error
		connection, err = broker.NewConnection("ws" + server.URL[4:])
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("FetchFile()", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		It("returns an error", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

			mockServer.serverCh <- buildError(1, "some-error")

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				err := connection.FetchFile(9, "some-file")
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile.GetName()).To(Equal("some-file"))
			Expect(clientMsg.FetchFile.GetFileId()).To(BeEquivalentTo(9))
		}, 3)

		It("returns the file ID", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

			mockServer.serverCh <- buildFileLocation(1)

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				err := connection.FetchFile(9, "some-file")
				Expect(err).To(BeNil())
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal("some-file"))
		})

		It("returns a redirect error", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

			mockServer.serverCh <- buildRemoteFileLocation(1, "ws://some.uri")

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				err := connection.FetchFile(9, "some-file")
				Expect(err).ToNot(BeNil())
				Expect(err.Uri).To(Equal("ws://some.uri"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal("some-file"))
		})

		It("returns an error for a dead connection", func(done Done) {
			defer close(done)
			server.CloseClientConnections()
			err := connection.FetchFile(9, "some-file")
			Expect(err).To(HaveOccurred())
			Expect(err.WebsocketError).To(BeTrue())
		}, 3)
	})

	Describe("WriteToFile()", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		It("returns an error", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildError(1, "some-error")

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_, err := connection.WriteToFile(8, expectedData)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_WriteToFile))
			Expect(clientMsg.WriteToFile).ToNot(BeNil())
			Expect(clientMsg.WriteToFile.GetFileId()).To(BeEquivalentTo(8))
			Expect(clientMsg.WriteToFile.GetData()).To(Equal(expectedData))
		})

		It("returns the new file offset", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildFileOffset(1, 101)

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				fileOffset, err := connection.WriteToFile(8, expectedData)
				Expect(err).ToNot(HaveOccurred())
				Expect(fileOffset).To(BeEquivalentTo(101))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_WriteToFile))
			Expect(clientMsg.WriteToFile).ToNot(BeNil())
			Expect(clientMsg.WriteToFile.GetFileId()).To(BeEquivalentTo(8))
			Expect(clientMsg.WriteToFile.GetData()).To(Equal(expectedData))
		})

		It("returns an error for a dead connection", func(done Done) {
			defer close(done)
			server.CloseClientConnections()
			expectedData := []byte("some-data")
			_, err := connection.WriteToFile(8, expectedData)
			Expect(err).To(HaveOccurred())
			Expect(err.WebsocketError).To(BeTrue())
		}, 3)

		It("returns an error for a dead connection detected by reading", func(done Done) {
			defer close(done)

			server.CloseClientConnections()
			By("waiting for the read core to detect the problem")
			time.Sleep(1 * time.Second)

			expectedData := []byte("some-data")
			_, err := connection.WriteToFile(8, expectedData)
			Expect(err).To(HaveOccurred())
			Expect(err.WebsocketError).To(BeTrue())
		}, 3)
	})

	Describe("ReadFromFile()", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		It("returns an error", func(done Done) {
			defer close(done)
			wg := new(sync.WaitGroup)
			defer wg.Wait()

			mockServer.serverCh <- buildError(1, "some-error")

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_, _, err := connection.ReadFromFile(8)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(clientMsg.ReadFromFile).ToNot(BeNil())
			Expect(clientMsg.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
		}, 3)

		It("returns the data and offset", func(done Done) {
			defer close(done)
			wg := new(sync.WaitGroup)
			defer wg.Wait()

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildReadData(1, expectedData, 101)

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				data, index, err := connection.ReadFromFile(8)
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
				Expect(index).To(BeEquivalentTo(101))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(clientMsg.ReadFromFile).ToNot(BeNil())
			Expect(clientMsg.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
		}, 3)

		It("returns an error for a dead connection", func(done Done) {
			defer close(done)
			server.CloseClientConnections()
			_, _, err := connection.ReadFromFile(8)
			Expect(err).To(HaveOccurred())
			Expect(err.WebsocketError).To(BeTrue())
		}, 3)

	})

	Describe("InitWriteIndex()", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		It("returns an error", func(done Done) {
			defer close(done)
			wg := new(sync.WaitGroup)
			defer wg.Wait()
			expectedData := []byte("some-data")

			mockServer.serverCh <- buildError(1, "some-error")

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				_, err := connection.InitWriteIndex(8, 101, expectedData)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_InitWriteIndex))
			Expect(clientMsg.InitWriteIndex).ToNot(BeNil())
			Expect(clientMsg.InitWriteIndex.GetFileId()).To(BeEquivalentTo(8))
			Expect(clientMsg.InitWriteIndex.GetData()).To(Equal(expectedData))
			Expect(clientMsg.InitWriteIndex.GetIndex()).To(BeEquivalentTo(101))
		})

		It("returns the new file offset", func(done Done) {
			defer close(done)
			wg := new(sync.WaitGroup)
			defer wg.Wait()

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildFileOffset(1, 101)

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				fileOffset, err := connection.InitWriteIndex(8, 101, expectedData)
				Expect(err).ToNot(HaveOccurred())
				Expect(fileOffset).To(BeEquivalentTo(101))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_InitWriteIndex))
			Expect(clientMsg.InitWriteIndex).ToNot(BeNil())
			Expect(clientMsg.InitWriteIndex.GetFileId()).To(BeEquivalentTo(8))
			Expect(clientMsg.InitWriteIndex.GetIndex()).To(BeEquivalentTo(101))
			Expect(clientMsg.InitWriteIndex.GetData()).To(Equal(expectedData))
		})

		It("returns an error for a dead connection", func(done Done) {
			defer close(done)
			server.CloseClientConnections()
			expectedData := []byte("some-data")
			_, err := connection.InitWriteIndex(8, 101, expectedData)
			Expect(err).To(HaveOccurred())
			Expect(err.WebsocketError).To(BeTrue())
		}, 3)
	})

	Describe("Close()", func() {
		It("closes the connection to the server", func(done Done) {
			defer close(done)
			connection.Close()
			server.Close()
		})
	})

})

func buildError(messageId uint64, errStr string) []byte {
	msgType := messages.Server_Error
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(messageId),
		Error: &messages.Error{
			Message: proto.String(errStr),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}

func buildFileLocation(messageId uint64) []byte {
	msgType := messages.Server_FileLocation
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(messageId),
		FileLocation: &messages.FileLocation{
			Local: proto.Bool(true),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}

func buildRemoteFileLocation(messageId uint64, uri string) []byte {
	uri = switchProtocol(uri)
	msgType := messages.Server_FileLocation
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(messageId),
		FileLocation: &messages.FileLocation{
			Local: proto.Bool(false),
			Uri:   proto.String(uri),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}

func switchProtocol(uri string) string {
	if !strings.HasPrefix(uri, "http") {
		return uri
	}

	return "ws" + uri[4:]
}

func buildFileOffset(messageId uint64, offset int64) []byte {
	msgType := messages.Server_FileOffset
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(messageId),
		FileOffset: &messages.FileOffset{
			Offset: proto.Int64(offset),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}

func buildReadData(messageId uint64, data []byte, index int64) []byte {
	msgType := messages.Server_ReadData
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(messageId),
		ReadData: &messages.ReadData{
			Data:   data,
			Offset: proto.Int64(index),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}
