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

		var (
			expectedMsgId      uint64
			expectedCreateFile bool

			conn *websocket.Conn
		)

		var writeFetchFile = func() {
			fetchFile := buildFetchFile(expectedMsgId, "some-file", expectedCreateFile)
			data, err := proto.Marshal(fetchFile)
			Expect(err).ToNot(HaveOccurred())

			conn.WriteMessage(websocket.BinaryMessage, data)
		}

		var readMessage = func() *messages.Server {
			_, data, err := conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := new(messages.Server)
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			return server
		}

		BeforeEach(func() {
			expectedMsgId = 99

			var err error
			conn, _, err = websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())
		})

		JustBeforeEach(func() {
			writeFetchFile()
		})

		Context("without error", func() {

			BeforeEach(func() {
				mockController.fetchFileIdCh <- 8
				mockController.fetchFileErrCh <- nil
			})

			Context("creating file", func() {

				BeforeEach(func() {
					expectedCreateFile = true
				})

				It("responds with the expected message ID", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.GetMessageId()).To(Equal(expectedMsgId))
				})

				It("responds with a FileLocation message", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.GetMessageType()).To(Equal(messages.Server_FileLocation))
					Expect(server.FileLocation).ToNot(BeNil())
				})

				It("responds that the file is local", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.FileLocation.GetLocal()).To(BeTrue())
				})

				It("responds with a nil error", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.Error).To(BeNil())
				})

				It("tells the controller to create a file", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCreateCh).Should(Receive(Equal(true)))
				})
			})

			Context("not creating file", func() {

				BeforeEach(func() {
					expectedCreateFile = false
				})

				It("tells the controller to not create a file", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCreateCh).Should(Receive(Equal(false)))
				})
			})
		})

		Context("with error", func() {

			var (
				expectedFileId        uint64
				expectedConnectionErr *broker.ConnectionError
			)

			JustBeforeEach(func() {
				mockController.fetchFileIdCh <- expectedFileId
				mockController.fetchFileErrCh <- expectedConnectionErr
			})

			Context("internal error", func() {

				BeforeEach(func() {
					expectedFileId = 0
					expectedConnectionErr = broker.NewConnectionError("some-error", "", false)
				})

				It("responds with the expected message ID", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.GetMessageId()).To(Equal(expectedMsgId))
				})

				It("responds with a error message", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.GetMessageType()).To(Equal(messages.Server_Error))
					Expect(server.Error).ToNot(BeNil())
				})

				It("responds with the expected error message", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.Error.GetMessage()).To(Equal("some-error"))
				})

				It("responds with a nil FileLocation", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.FileLocation).To(BeNil())
				})
			})

			Context("with redirect error", func() {

				var (
					expectedUri string
				)

				BeforeEach(func() {
					expectedUri = "http://some.url"
					expectedFileId = 8
					expectedConnectionErr = broker.NewConnectionError("some-error", expectedUri, false)
				})

				It("responds with the expected message ID", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.GetMessageId()).To(Equal(expectedMsgId))
				})

				It("responds with a FileLocation message", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.GetMessageType()).To(Equal(messages.Server_FileLocation))
					Expect(server.FileLocation).ToNot(BeNil())
				})

				It("responds that the file is remote", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.FileLocation.GetUri()).To(Equal(expectedUri))
					Expect(server.FileLocation.GetLocal()).To(BeFalse())
				})

				It("responds with a nil error", func(done Done) {
					defer close(done)

					Eventually(mockController.fetchFileCh).Should(Receive(Equal("some-file")))
					server := readMessage()

					Expect(server.Error).To(BeNil())
				})
			})
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

	Describe("SeekIndex()", func() {
		var (
			expectedError error
			expectedIndex uint64
			conn          *websocket.Conn
			wg            sync.WaitGroup
		)

		JustBeforeEach(func() {
			expectedIndex = 101

			var err error
			conn, _, err = websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				callback := <-mockController.seekCallbackCh
				callback(expectedError)
			}()
		})

		AfterEach(func() {
			wg.Wait()
		})

		Context("an error occurred", func() {

			BeforeEach(func() {
				expectedError = fmt.Errorf("some-error")
			})

			It("returns the error", func(done Done) {
				defer close(done)

				seekIndex := buildSeekIndex(99, 8, expectedIndex)
				data, err := proto.Marshal(seekIndex)
				Expect(err).ToNot(HaveOccurred())
				conn.WriteMessage(websocket.BinaryMessage, data)

				Eventually(mockController.seekIdCh).Should(Receive(BeEquivalentTo(8)))
				Eventually(mockController.seekIndexCh).Should(Receive(Equal(expectedIndex)))

				_, data, err = conn.ReadMessage()
				Expect(err).ToNot(HaveOccurred())

				server := new(messages.Server)
				err = server.Unmarshal(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(server.GetMessageId()).To(BeEquivalentTo(99))
				Expect(server.GetMessageType()).To(Equal(messages.Server_Error))
				Expect(server.Error).ToNot(BeNil())
				Expect(server.Error.GetMessage()).To(Equal(expectedError.Error()))
			})
		})

		Context("no error", func() {

			BeforeEach(func() {
				expectedError = nil
			})

			It("returns the given index", func(done Done) {
				defer close(done)

				seekIndex := buildSeekIndex(99, 8, expectedIndex)
				data, err := proto.Marshal(seekIndex)
				Expect(err).ToNot(HaveOccurred())
				conn.WriteMessage(websocket.BinaryMessage, data)

				Eventually(mockController.seekIdCh).Should(Receive(BeEquivalentTo(8)))
				Eventually(mockController.seekIndexCh).Should(Receive(Equal(expectedIndex)))

				_, data, err = conn.ReadMessage()
				Expect(err).ToNot(HaveOccurred())

				server := new(messages.Server)
				err = server.Unmarshal(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(server.GetMessageId()).To(BeEquivalentTo(99))
				Expect(server.GetMessageType()).To(Equal(messages.Server_FileOffset))
				Expect(server.Error).To(BeNil())
				Expect(server.FileOffset.GetOffset()).To(Equal(int64(expectedIndex)))
			})
		})

	})

})

func buildFetchFile(id uint64, name string, create bool) *messages.Client {
	return &messages.Client{
		MessageType: messages.Client_FetchFile.Enum(),
		MessageId:   proto.Uint64(id),
		FetchFile: &messages.FetchFile{
			Name:   proto.String(name),
			Create: proto.Bool(create),
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

func buildSeekIndex(msgId, fileId, index uint64) *messages.Client {
	messageType := messages.Client_SeekIndex
	return &messages.Client{
		MessageType: &messageType,
		MessageId:   &msgId,
		SeekIndex: &messages.SeekIndex{
			FileId: &fileId,
			Index:  proto.Uint64(index),
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
