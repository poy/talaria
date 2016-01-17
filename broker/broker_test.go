package broker_test

import (
	"fmt"
	"net/http/httptest"
	"sync"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/common"
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
				expectedConnectionErr *common.ConnectionError
			)

			JustBeforeEach(func() {
				mockController.fetchFileIdCh <- expectedFileId
				mockController.fetchFileErrCh <- expectedConnectionErr
			})

			Context("internal error", func() {

				BeforeEach(func() {
					expectedFileId = 0
					expectedConnectionErr = common.NewConnectionError("some-error", "", "", false)
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
					expectedConnectionErr = common.NewConnectionError("some-error", expectedUri, "", false)
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

			mockController.writeFileIndexCh <- 0
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

		It("returns the new index", func(done Done) {
			defer close(done)
			conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.writeFileIndexCh <- 77
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
			Expect(server.GetMessageType()).To(Equal(messages.Server_FileIndex))
			Expect(server.FileIndex).ToNot(BeNil())
			Expect(server.FileIndex.GetIndex()).To(BeEquivalentTo(77))
		})

		Measure("Writes to a file 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				count := 1000
				go func() {
					for i := 0; i < count; i++ {
						mockController.writeFileIndexCh <- 77
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
			mockController.readIndexCh <- 0
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
			mockController.readIndexCh <- 1010
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
			Expect(server.ReadData.GetIndex()).To(BeEquivalentTo(1010))
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
						mockController.readIndexCh <- int64(i)
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
				Expect(server.GetMessageType()).To(Equal(messages.Server_FileIndex))
				Expect(server.Error).To(BeNil())
				Expect(server.FileIndex.GetIndex()).To(Equal(int64(expectedIndex)))
			})
		})

	})

	Describe("ValidateLeader()", func() {

		var (
			expectedMsgIndex    uint64
			expectedName        string
			expectedIndex       uint64
			expectedValidLeader bool
			conn                *websocket.Conn
		)

		var writeMsg = func() {
			viableLeader := buildViableLeader(expectedMsgIndex, expectedName, expectedIndex)
			data, err := proto.Marshal(viableLeader)
			Expect(err).ToNot(HaveOccurred())
			conn.WriteMessage(websocket.BinaryMessage, data)
		}

		var readServerMsg = func() *messages.ImpeachLeader {
			_, data, err := conn.ReadMessage()
			Expect(err).ToNot(HaveOccurred())

			server := new(messages.Server)
			err = server.Unmarshal(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(server.GetMessageId()).To(BeEquivalentTo(expectedMsgIndex))
			Expect(server.GetMessageType()).To(Equal(messages.Server_ImpeachLeader))
			Expect(server.ImpeachLeader).ToNot(BeNil())

			return server.ImpeachLeader
		}

		JustBeforeEach(func() {
			expectedMsgIndex = 99
			expectedName = "some-name"
			expectedIndex = 101
			expectedValidLeader = true

			var err error
			conn, _, err = websocket.DefaultDialer.Dial(wsUrl, nil)
			Expect(err).ToNot(HaveOccurred())

			mockController.validLeaderResultCh <- expectedValidLeader

			writeMsg()
		})

		It("passes the correct name and index to the controller", func() {
			Eventually(mockController.validLeaderNameCh).Should(Receive(Equal(expectedName)))
			Eventually(mockController.validLeaderIndexCh).Should(Receive(Equal(expectedIndex)))
		})

		It("returns the file controllers response", func() {
			Expect(readServerMsg().GetImpeach()).To(Equal(!expectedValidLeader))
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
	return &messages.Client{
		MessageType: messages.Client_WriteToFile.Enum(),
		MessageId:   &msgId,
		WriteToFile: &messages.WriteToFile{
			FileId: &fileId,
			Data:   data,
		},
	}
}

func buildSeekIndex(msgId, fileId, index uint64) *messages.Client {
	return &messages.Client{
		MessageType: messages.Client_SeekIndex.Enum(),
		MessageId:   &msgId,
		SeekIndex: &messages.SeekIndex{
			FileId: &fileId,
			Index:  proto.Uint64(index),
		},
	}
}

func buildReadFromFile(msgId, fileId uint64) []byte {
	msg := &messages.Client{
		MessageType: messages.Client_ReadFromFile.Enum(),
		MessageId:   &msgId,
		ReadFromFile: &messages.ReadFromFile{
			FileId: &fileId,
		},
	}

	data, err := proto.Marshal(msg)
	Expect(err).ToNot(HaveOccurred())
	return data
}

func buildViableLeader(msgId uint64, name string, index uint64) *messages.Client {
	return &messages.Client{
		MessageType: messages.Client_ViableLeader.Enum(),
		MessageId:   &msgId,
		ViableLeader: &messages.ViableLeader{
			Name:  proto.String(name),
			Index: proto.Uint64(index),
		},
	}
}
