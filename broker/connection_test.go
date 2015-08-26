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
		It("Returns an error", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
				_, err := connection.FetchFile("some-file")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var connection *messages.Client
			Eventually(mockServer.connectionCh).Should(Receive(&connection))
			Expect(connection.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(connection.FetchFile.GetName()).To(Equal("some-file"))
		})

		It("Returns the file ID", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildFileLocation(99, 8)

			go func() {
				defer GinkgoRecover()
				fileId, err := connection.FetchFile("some-file")
				Expect(err).ToNot(HaveOccurred())
				Expect(fileId).To(BeEquivalentTo(8))
			}()

			var connection *messages.Client
			Eventually(mockServer.connectionCh).Should(Receive(&connection))
			Expect(connection.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(connection.FetchFile).ToNot(BeNil())
			Expect(connection.FetchFile.GetName()).To(Equal("some-file"))
		})
	})

	Context("WriteToFile", func() {
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

			var connection *messages.Client
			Eventually(mockServer.connectionCh).Should(Receive(&connection))
			Expect(connection.GetMessageType()).To(Equal(messages.Client_WriteToFile))
			Expect(connection.WriteToFile).ToNot(BeNil())
			Expect(connection.WriteToFile.GetFileId()).To(BeEquivalentTo(8))
			Expect(connection.WriteToFile.GetData()).To(Equal(expectedData))
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

			var connection *messages.Client
			Eventually(mockServer.connectionCh).Should(Receive(&connection))
			Expect(connection.GetMessageType()).To(Equal(messages.Client_WriteToFile))
			Expect(connection.WriteToFile).ToNot(BeNil())
			Expect(connection.WriteToFile.GetFileId()).To(BeEquivalentTo(8))
			Expect(connection.WriteToFile.GetData()).To(Equal(expectedData))
		})
	})

	Context("ReadFromFile", func() {
		It("Returns an error", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
				_, err := connection.ReadFromFile(8)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var connection *messages.Client
			Eventually(mockServer.connectionCh).Should(Receive(&connection))
			Expect(connection.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(connection.ReadFromFile).ToNot(BeNil())
			Expect(connection.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
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

			var connection *messages.Client
			Eventually(mockServer.connectionCh).Should(Receive(&connection))
			Expect(connection.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(connection.ReadFromFile).ToNot(BeNil())
			Expect(connection.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
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

func buildFileLocation(messageId, fileId uint64) []byte {
	msgType := messages.Server_FileLocation
	server := &messages.Server{
		MessageType: &msgType,
		MessageId:   proto.Uint64(messageId),
		FileLocation: &messages.FileLocation{
			Local:  proto.Bool(true),
			FileId: proto.Uint64(fileId),
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