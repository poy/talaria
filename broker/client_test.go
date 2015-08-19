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

var _ = FDescribe("Client", func() {
	var (
		recorder   *httptest.ResponseRecorder
		req        *http.Request
		client     *broker.Client
		server     *httptest.Server
		mockServer *mockServer
	)

	BeforeEach(func() {
		recorder = httptest.NewRecorder()
		req, _ = http.NewRequest("GET", "http://some.url", nil)
		mockServer = newMockServer()
		server = httptest.NewServer(mockServer)
		client = broker.NewClient("ws" + server.URL[4:])
	})

	Context("FetchFile", func() {
		It("Returns an error", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
				_, err := client.FetchFile("some-file")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var client *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&client))
			Expect(client.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(client.FetchFile.GetName()).To(Equal("some-file"))
		})

		It("Returns the file ID", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildFileLocation(99, 8)

			go func() {
				defer GinkgoRecover()
				fileId, err := client.FetchFile("some-file")
				Expect(err).ToNot(HaveOccurred())
				Expect(fileId).To(BeEquivalentTo(8))
			}()

			var client *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&client))
			Expect(client.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(client.FetchFile).ToNot(BeNil())
			Expect(client.FetchFile.GetName()).To(Equal("some-file"))
		})
	})

	Context("WriteToFile", func() {
		It("Returns an error", func(done Done) {
			defer close(done)

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
				_, err := client.WriteToFile(8, expectedData)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var client *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&client))
			Expect(client.GetMessageType()).To(Equal(messages.Client_WriteToFile))
			Expect(client.WriteToFile).ToNot(BeNil())
			Expect(client.WriteToFile.GetFileId()).To(BeEquivalentTo(8))
			Expect(client.WriteToFile.GetData()).To(Equal(expectedData))
		})

		It("Returns the new file offset", func(done Done) {
			defer close(done)

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildFileOffset(99, 101)

			go func() {
				defer GinkgoRecover()
				fileOffset, err := client.WriteToFile(8, expectedData)
				Expect(err).ToNot(HaveOccurred())
				Expect(fileOffset).To(BeEquivalentTo(101))
			}()

			var client *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&client))
			Expect(client.GetMessageType()).To(Equal(messages.Client_WriteToFile))
			Expect(client.WriteToFile).ToNot(BeNil())
			Expect(client.WriteToFile.GetFileId()).To(BeEquivalentTo(8))
			Expect(client.WriteToFile.GetData()).To(Equal(expectedData))
		})
	})

	Context("ReadFromFile", func() {
		It("Returns an error", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildError(99, "some-error")

			go func() {
				defer GinkgoRecover()
				_, err := client.ReadFromFile(8)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("some-error"))
			}()

			var client *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&client))
			Expect(client.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(client.ReadFromFile).ToNot(BeNil())
			Expect(client.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
		})

		It("Returns the data and offset", func(done Done) {
			defer close(done)

			expectedData := []byte("some-data")
			mockServer.serverCh <- buildReadData(99, expectedData)

			go func() {
				defer GinkgoRecover()
				data, err := client.ReadFromFile(8)
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
			}()

			var client *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&client))
			Expect(client.GetMessageType()).To(Equal(messages.Client_ReadFromFile))
			Expect(client.ReadFromFile).ToNot(BeNil())
			Expect(client.ReadFromFile.GetFileId()).To(BeEquivalentTo(8))
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
