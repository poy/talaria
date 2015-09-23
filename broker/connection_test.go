package broker_test

import (
	"net/http"
	"net/http/httptest"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/messages"
	"github.com/gogo/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection", func() {
	var (
		recorder   *httptest.ResponseRecorder
		req        *http.Request
		connection *broker.Connection
		server     *httptest.Server
		mockServer *mockServer
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		req, _ = http.NewRequest("GET", "http://some.url", nil)
		mockServer = newMockServer()
		server = httptest.NewServer(mockServer)
		var err error
		connection, err = broker.NewConnection("ws" + server.URL[4:])
		Expect(err).ToNot(HaveOccurred())
	})

	Context("FetchFile", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		It("returns an error", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
				err := connection.FetchFile(9, "some-file")
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile.GetName()).To(Equal("some-file"))
			Expect(clientMsg.FetchFile.GetFileId()).To(BeEquivalentTo(9))
		})

		It("returns the file ID", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildFileLocation(99)

			go func() {
				defer GinkgoRecover()
				err := connection.FetchFile(9, "some-file")
				Expect(err).To(BeNil())
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal("some-file"))
		})

		It("Returns a redirect error", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildRemoteFileLocation(99, "http://some.uri")

			go func() {
				defer GinkgoRecover()
				err := connection.FetchFile(9, "some-file")
				Expect(err).ToNot(BeNil())
				Expect(err.Uri).To(Equal("http://some.uri"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal("some-file"))
		})
	})

	Context("WriteToFile", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		It("Returns an error", func(done Done) {
			defer close(done)

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
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

		It("Returns the new file offset", func(done Done) {
			defer close(done)

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildFileOffset(99, 101)

			go func() {
				defer GinkgoRecover()
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
	})

	Context("ReadFromFile", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		It("Returns an error", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
				_, err := connection.ReadFromFile(8)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(clientMsg.ReadFromFile).ToNot(BeNil())
			Expect(clientMsg.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
		})

		It("Returns the data and offset", func(done Done) {
			defer close(done)

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildReadData(99, expectedData)

			go func() {
				defer GinkgoRecover()
				data, err := connection.ReadFromFile(8)
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(clientMsg.ReadFromFile).ToNot(BeNil())
			Expect(clientMsg.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
		})
	})

	Context("Close", func() {
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

func buildReadData(messageId uint64, data []byte) []byte {
	msgType := messages.Server_ReadData
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(messageId),
		ReadData: &messages.ReadData{
			Data: data,
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}
