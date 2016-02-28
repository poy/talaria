package client_test

import (
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/apoydence/talaria/client"
	"github.com/apoydence/talaria/pb/messages"
	"github.com/gogo/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection", func() {
	var (
		connection *client.Connection
		server     *httptest.Server
		mockServer *mockServer

		expectedName string
	)

	BeforeEach(func() {
		expectedName = "some-file"

		mockServer = newMockServer()
		server = httptest.NewServer(mockServer)

		var err error
		connection, err = client.NewConnection("ws" + server.URL[4:])
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
					err := connection.FetchFile(9, expectedName, true)

					Expect(err).To(MatchError(expectedError))
				})

				It("returns true for Errored()", func() {
					connection.FetchFile(9, expectedName, true)

					Expect(connection.Errored()).To(BeTrue())
				})

				It("closes the connection", func() {
					connection.FetchFile(9, expectedName, true)

					Eventually(mockServer.connDoneCh).Should(HaveLen(1))
				})

				It("populates the error with the connection URI", func() {
					err := connection.FetchFile(9, expectedName, true)

					Expect(err).To(HaveOccurred())
					Expect(err.ConnURL).To(Equal(connection.URL))
				})
			})

			Context("dead connection", func() {

				BeforeEach(func() {
					server.CloseClientConnections()
				})

				It("returns a websocket error", func(done Done) {
					defer close(done)
					err := connection.FetchFile(9, expectedName, true)

					Expect(err).To(HaveOccurred())
					Expect(err.WebsocketError).To(BeTrue())
				}, 3)

				It("returns true for Errored()", func() {
					connection.FetchFile(9, expectedName, true)

					Expect(connection.Errored()).To(BeTrue())
				})

				It("populates the error with the connection URI", func() {
					err := connection.FetchFile(9, expectedName, true)

					Expect(err).To(HaveOccurred())
					Expect(err.ConnURL).To(Equal(connection.URL))
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
				err := connection.FetchFile(9, expectedName, true)
				Expect(err).To(BeNil())
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal(expectedName))
			Expect(clientMsg.FetchFile.GetCreate()).To(BeTrue())
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
				err := connection.FetchFile(9, expectedName, false)
				Expect(err).ToNot(BeNil())
				Expect(err.Uri).To(Equal("ws://some.uri"))
			}()

			var clientMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
			Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg.FetchFile).ToNot(BeNil())
			Expect(clientMsg.FetchFile.GetName()).To(Equal(expectedName))
			Expect(clientMsg.FetchFile.GetCreate()).To(BeFalse())
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

				It("populates the error with the connection URI", func() {
					_, err := connection.WriteToFile(8, expectedData)

					Expect(err).To(HaveOccurred())
					Expect(err.ConnURL).To(Equal(connection.URL))
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

				It("populates the error with the connection URI", func() {
					_, err := connection.WriteToFile(8, expectedData)

					Expect(err).To(HaveOccurred())
					Expect(err.ConnURL).To(Equal(connection.URL))
				})
			})
		})

		It("returns the new file index", func(done Done) {
			defer close(done)
			var wg sync.WaitGroup
			defer wg.Wait()

			mockServer.serverCh <- buildFileIndex(1, 101)

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				fileIndex, err := connection.WriteToFile(8, expectedData)
				Expect(err).ToNot(HaveOccurred())
				Expect(fileIndex).To(BeEquivalentTo(101))
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

				It("populates the error with the connection URI", func() {
					_, _, err := connection.ReadFromFile(8)

					Expect(err).To(HaveOccurred())
					Expect(err.ConnURL).To(Equal(connection.URL))
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

				It("populates the error with the connection URI", func() {
					_, _, err := connection.ReadFromFile(8)

					Expect(err).To(HaveOccurred())
					Expect(err.ConnURL).To(Equal(connection.URL))
				})
			})
		})

		It("returns the data and index", func(done Done) {
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
			It("returns the new file index", func(done Done) {
				defer close(done)

				mockServer.serverCh <- buildFileIndex(1, int64(expectedIndex))

				err := connection.SeekIndex(8, expectedIndex)
				Expect(err).ToNot(HaveOccurred())

				var clientMsg *messages.Client
				Eventually(mockServer.clientCh).Should(Receive(&clientMsg))
				Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_SeekIndex))
				Expect(clientMsg.SeekIndex).ToNot(BeNil())
				Expect(clientMsg.SeekIndex.GetFileId()).To(BeEquivalentTo(8))
				Expect(clientMsg.SeekIndex.GetIndex()).To(Equal(expectedIndex))
			})

			Context("non-matching returned index", func() {
				BeforeEach(func() {
					mockServer.serverCh <- buildFileIndex(1, int64(expectedIndex+1))
				})

				It("returns an error if the returned index does not match", func(done Done) {
					defer close(done)

					err := connection.SeekIndex(8, expectedIndex)
					Expect(err).To(HaveOccurred())
				})

				It("populates the error with the connection URI", func() {
					err := connection.SeekIndex(8, expectedIndex)

					Expect(err).To(HaveOccurred())
					Expect(err.ConnURL).To(Equal(connection.URL))
				})
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

			It("populates the error with the connection URI", func() {
				err := connection.SeekIndex(8, expectedIndex)

				Expect(err).To(HaveOccurred())
				Expect(err.ConnURL).To(Equal(connection.URL))
			})
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

	Describe("ValidateLeader", func() {
		var (
			expectedName  string
			expectedIndex uint64
		)

		BeforeEach(func() {
			expectedName = "some-name"
		})

		Context("impeach leader", func() {

			BeforeEach(func() {
				mockServer.serverCh <- buildImpeach(1, true)
			})

			It("returns true", func() {
				impeach := connection.ValidateLeader(expectedName, expectedIndex)

				Expect(impeach).To(BeTrue())
			})

			It("sends the proper client message", func() {
				connection.ValidateLeader(expectedName, expectedIndex)

				var client *messages.Client
				Eventually(mockServer.clientCh).Should(Receive(&client))
				Expect(client.GetMessageType()).To(Equal(messages.Client_ViableLeader))
				Expect(client.ViableLeader).ToNot(BeNil())
				Expect(client.ViableLeader.GetName()).To(Equal(expectedName))
				Expect(client.ViableLeader.GetIndex()).To(Equal(expectedIndex))
			})

		})

		Context("don't impeach leader", func() {

			BeforeEach(func() {
				mockServer.serverCh <- buildImpeach(1, false)
			})

			It("returns false", func() {
				impeach := connection.ValidateLeader(expectedName, expectedIndex)

				Expect(impeach).To(BeFalse())
			})
		})
	})

})

func buildError(messageId uint64, errStr string) []byte {
	server := &messages.Server{
		MessageType: messages.Server_Error.Enum(),
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
	server := &messages.Server{
		MessageType: messages.Server_FileLocation.Enum(),
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
	server := &messages.Server{
		MessageType: messages.Server_FileLocation.Enum(),
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

func buildFileIndex(messageId uint64, index int64) []byte {
	server := &messages.Server{
		MessageType: messages.Server_FileIndex.Enum(),
		MessageId:   proto.Uint64(messageId),
		FileIndex: &messages.FileIndex{
			Index: proto.Int64(index),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}

func buildReadData(messageId uint64, data []byte, index int64) []byte {
	server := &messages.Server{
		MessageType: messages.Server_ReadData.Enum(),
		MessageId:   proto.Uint64(messageId),
		ReadData: &messages.ReadData{
			Data:  data,
			Index: proto.Int64(index),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}

func buildImpeach(messageId uint64, impeach bool) []byte {
	server := &messages.Server{
		MessageType: messages.Server_ImpeachLeader.Enum(),
		MessageId:   proto.Uint64(messageId),
		ImpeachLeader: &messages.ImpeachLeader{
			Impeach: proto.Bool(impeach),
		},
	}

	data, err := server.Marshal()
	Expect(err).ToNot(HaveOccurred())
	return data
}
