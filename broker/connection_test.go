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

		expectedName string
	)

	BeforeEach(func() {
		expectedName = "some-file"

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

		Context("with error", func() {

			Context("server returns error", func() {
				var (
					expectedError string
				)

				BeforeEach(func() {
					expectedError = "some-error"

					mockServer.serverCh <- buildError(1, expectedError)
				})

				It("returns an error", func() {
					err := connection.FetchFile(9, expectedName)

					Expect(err).To(MatchError(expectedError))
				})

				It("returns true for Errored()", func() {
					connection.FetchFile(9, expectedName)

					Expect(connection.Errored()).To(BeTrue())
				})

				It("closes the connection", func() {
					connection.FetchFile(9, expectedName)

					Eventually(mockServer.connDoneCh).Should(HaveLen(1))
				})
			})

			Context("dead connection", func() {

				BeforeEach(func() {
					server.CloseClientConnections()
				})

				It("returns a websocket error", func(done Done) {
					defer close(done)
					err := connection.FetchFile(9, expectedName)

					Expect(err).To(HaveOccurred())
					Expect(err.WebsocketError).To(BeTrue())
				}, 3)

				It("returns true for Errored()", func() {
					connection.FetchFile(9, expectedName)

					Expect(connection.Errored()).To(BeTrue())
				})
			})

		})

		It("returns the file ID", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

			mockServer.serverCh <- buildFileLocation(1)

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				err := connection.FetchFile(9, expectedName)
				Expect(err).To(BeNil())
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal(expectedName))
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
				err := connection.FetchFile(9, expectedName)
				Expect(err).ToNot(BeNil())
				Expect(err.Uri).To(Equal("ws://some.uri"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal(expectedName))
			Expect(connection.Errored()).To(BeFalse())
		})

	})

	Describe("WriteToFile()", func() {

		var (
			expectedData []byte
		)

		BeforeEach(func() {
			expectedData = []byte("some-data")
		})

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		Context("with errors", func() {

			Context("server returns error", func() {
				var (
					expectedError string
				)

				BeforeEach(func() {
					expectedError = "some-error"

					mockServer.serverCh <- buildError(1, "some-error")
				})

				It("returns an error", func() {
					_, err := connection.WriteToFile(8, expectedData)

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(expectedError))
				})

				It("returns true for Errored()", func() {
					connection.WriteToFile(8, expectedData)

					Expect(connection.Errored()).To(BeTrue())
				})
			})

			Context("dead connection", func() {

				BeforeEach(func() {
					server.CloseClientConnections()
				})

				It("returns an error for a dead connection", func() {
					_, err := connection.WriteToFile(8, expectedData)

					Expect(err).To(HaveOccurred())
					Expect(err.WebsocketError).To(BeTrue())
				})

				It("returns true for Errored()", func() {
					connection.WriteToFile(8, expectedData)

					Expect(connection.Errored()).To(BeTrue())
				})

				It("returns an error for a dead connection detected by reading", func(done Done) {
					defer close(done)

					By("waiting for the read core to detect the problem")
					time.Sleep(1 * time.Second)

					_, err := connection.WriteToFile(8, expectedData)

					Expect(err).To(HaveOccurred())
					Expect(err.WebsocketError).To(BeTrue())
				}, 3)
			})
		})

		It("returns the new file offset", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

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

			Expect(connection.Errored()).To(BeFalse())
		})

	})

	Describe("ReadFromFile()", func() {

		AfterEach(func() {
			connection.Close()
			server.Close()
		})

		Context("with errors", func() {

			Context("server error", func() {
				var (
					expectedError string
				)

				BeforeEach(func() {
					expectedError = "some-error"

					mockServer.serverCh <- buildError(1, "some-error")
				})

				It("returns an error", func() {
					_, _, err := connection.ReadFromFile(8)

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal(expectedError))
				})

				It("returns true for Errored()", func() {
					connection.ReadFromFile(8)

					Expect(connection.Errored()).To(BeTrue())
				})
			})

			Context("dead connection", func() {

				BeforeEach(func() {
					server.CloseClientConnections()
				})

				It("returns an error for a dead connection", func() {
					_, _, err := connection.ReadFromFile(8)

					Expect(err).To(HaveOccurred())
					Expect(err.WebsocketError).To(BeTrue())
				})

				It("returns true for Errored()", func() {
					connection.ReadFromFile(8)

					Expect(connection.Errored()).To(BeTrue())
				})
			})
		})

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

	})

	Describe("SeekIndex()", func() {

		var (
			expectedIndex uint64
		)

		BeforeEach(func() {
			expectedIndex = 101
		})

		Context("live connection", func() {
			It("returns the new file offset", func(done Done) {
				defer close(done)

				mockServer.serverCh <- buildFileOffset(1, int64(expectedIndex))

				err := connection.SeekIndex(8, expectedIndex)
				Expect(err).ToNot(HaveOccurred())

				var clientMsg *messages.Client
				Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
				Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_SeekIndex))
				Expect(clientMsg.SeekIndex).ToNot(BeNil())
				Expect(clientMsg.SeekIndex.GetFileId()).To(BeEquivalentTo(8))
				Expect(clientMsg.SeekIndex.GetIndex()).To(Equal(expectedIndex))
			})

			It("returns an error if the returned index does not match", func(done Done) {
				defer close(done)

				mockServer.serverCh <- buildFileOffset(1, int64(expectedIndex+1))

				err := connection.SeekIndex(8, expectedIndex)
				Expect(err).To(HaveOccurred())
			})

		})

		Context("dead connection", func() {

			BeforeEach(func() {
				server.CloseClientConnections()
			})

			It("returns an error for a dead connection", func(done Done) {
				defer close(done)
				err := connection.SeekIndex(8, expectedIndex)

				Expect(err).To(HaveOccurred())
				Expect(err.WebsocketError).To(BeTrue())
			}, 3)
		})

	})

	Describe("Close()", func() {
		It("closes the connection to the server", func(done Done) {
			defer close(done)
			connection.Close()
			server.Close()
		})
	})

	Describe("Errored()", func() {
		Context("nil Connection", func() {
			connection = nil
		})

		It("returns false", func() {
			Expect(connection.Errored()).To(BeFalse())
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
