package client_test

import (
	"net/http/httptest"

	"github.com/apoydence/talaria/client"
	"github.com/apoydence/talaria/pb/messages"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("client", func() {

	var (
		mockServer *mockServer
		server     *httptest.Server

		clt *client.Client
	)

	var createMockServer = func() string {
		mockServer, server = startMockServer()
		return convertToWs(server.URL)
	}

	BeforeEach(func() {
		URL := createMockServer()

		var err error
		clt, err = client.New(URL)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		clt.Close()
		close(mockServer.serverCh)

		server.CloseClientConnections()
		server.Close()
	})

	Describe("CreateFile()", func() {

		var (
			expectedFileName string
		)

		BeforeEach(func() {
			expectedFileName = "some-file-1"
		})

		Context("without errors", func() {
			BeforeEach(func() {
				mockServer.serverCh <- buildFileLocation(1)
			})

			It("writes to the correct clt", func(done Done) {
				defer close(done)

				Expect(clt.CreateFile(expectedFileName)).To(Succeed())

				var msg *messages.Client
				Eventually(mockServer.clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
				Expect(msg.FetchFile.GetCreate()).To(BeTrue())
			}, 5)

		})
	})

	Describe("FetchWriter()", func() {

		var (
			expectedFileName string
			expectedData     []byte
		)

		BeforeEach(func() {
			expectedFileName = "some-file-1"
			expectedData = []byte("some-data")
		})

		Context("without errors", func() {

			BeforeEach(func() {

				mockServer.serverCh <- buildFileLocation(1)

				for i := 0; i < 10; i++ {
					mockServer.serverCh <- buildFileIndex(uint64(i+2), 101)
				}
			})

			It("writes to the correct clt", func(done Done) {
				defer close(done)

				writer, err := clt.FetchWriter(expectedFileName)
				Expect(err).ToNot(HaveOccurred())

				_, err = writer.WriteToFile(expectedData)
				Expect(err).ToNot(HaveOccurred())

				var msg *messages.Client
				Eventually(mockServer.clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))

				Eventually(mockServer.clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_WriteToFile))
				Expect(msg.GetWriteToFile().GetData()).To(Equal(expectedData))
			}, 5)

		})

		Measure("Writes to a file 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				mockServer.serverCh <- buildFileLocation(1)
				count := 1000

				go func() {
					for i := 0; i < count; i++ {
						mockServer.serverCh <- buildFileIndex(uint64(i+2), 101)
					}
				}()

				go func() {
					for _ = range mockServer.clientCh {
						//NOP
					}
				}()

				writer, _ := clt.FetchWriter(expectedFileName)

				for i := 0; i < count; i++ {
					writer.WriteToFile(expectedData)
				}
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))

		}, 5)
	})

	Describe("FetchReader()", func() {

		var (
			expectedFileName string
			expectedData     []byte
		)

		BeforeEach(func() {
			expectedFileName = "some-file-1"
			expectedData = []byte("some-data")
		})

		Context("for known server", func() {

			BeforeEach(func() {
				mockServer.serverCh <- buildFileLocation(1)
				mockServer.serverCh <- buildReadData(2, expectedData, 101)
			})

			It("reads from the correct clt", func(done Done) {
				defer close(done)

				reader, err := clt.FetchReader(expectedFileName)
				Expect(err).ToNot(HaveOccurred())

				data, index, err := reader.ReadFromFile()
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
				Expect(index).To(BeEquivalentTo(101))

				var msg *messages.Client
				Eventually(mockServer.clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
				Expect(msg.GetFetchFile().GetName()).To(Equal(expectedFileName))
			})
		})

		Measure("Reads from a file 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				mockServer.serverCh <- buildFileLocation(1)
				count := 1000
				data := []byte("some-data")

				go func() {
					for i := 0; i < count; i++ {
						mockServer.serverCh <- buildReadData(uint64(i+2), data, 0)
					}
				}()

				go func() {
					for _ = range mockServer.clientCh {
						//NOP
					}
				}()

				reader, err := clt.FetchReader("some-file")
				Expect(err).ToNot(HaveOccurred())
				for i := 0; i < count; i++ {
					reader.ReadFromFile()
				}
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))

		}, 5)
	})

	Describe("FileMeta()", func() {
		var (
			expectedFileName string
		)

		BeforeEach(func(done Done) {
			defer close(done)
			expectedFileName = "some-name"
		})

		Context("created file", func() {

			BeforeEach(func() {
				mockServer.serverCh <- buildFileLocation(1)
			})

			It("returns the clt URI that is the leader of the given file", func(done Done) {
				defer close(done)
				meta, err := clt.FileMeta(expectedFileName)

				Expect(err).ToNot(HaveOccurred())
				Expect(meta.GetReplicaURIs()).To(HaveLen(1))
				Expect(meta.GetReplicaURIs()[0]).To(Equal(htmlToWs(server.URL)))
			})
		})

		Context("file not created ", func() {

			var (
				expectedError string
			)

			BeforeEach(func() {
				expectedError = "some-error"

				mockServer.serverCh <- buildError(1, expectedError)
			})

			It("returns an error for an unknown file", func(done Done) {
				defer close(done)
				_, err := clt.FileMeta(expectedFileName)

				Expect(err).To(MatchError(expectedError))
			})
		})
	})

})

func htmlToWs(URI string) string {
	return "ws" + URI[4:]
}

func startMockServer() (*mockServer, *httptest.Server) {
	mockServer := newMockServer()
	server := httptest.NewServer(mockServer)
	return mockServer, server
}
