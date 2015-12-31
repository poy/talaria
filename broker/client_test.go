package broker_test

import (
	"net/http/httptest"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/pb/messages"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {

	var (
		mockServers []*mockServer
		servers     []*httptest.Server
		client      *broker.Client

		extraMockServer *mockServer
		extraServerURL  string
	)

	var convertToWs = func(URL string) string {
		return "ws" + URL[4:]
	}

	var createKnownMockServers = func() []string {
		var urls []string
		for i := 0; i < 3; i++ {
			mockServer, server := startMockServer()
			mockServers = append(mockServers, mockServer)
			servers = append(servers, server)
			urls = append(urls, convertToWs(server.URL))
		}

		return urls
	}

	var createExtraMockServer = func() {
		mockServer, server := startMockServer()
		extraMockServer = mockServer
		servers = append(servers, server)
		extraServerURL = convertToWs(server.URL)
	}

	BeforeEach(func() {
		mockServers = nil
		servers = nil
		URLs := createKnownMockServers()
		createExtraMockServer()

		var err error
		client, err = broker.NewClient(URLs...)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		client.Close()
		for _, server := range mockServers {
			close(server.serverCh)
		}

		for _, server := range servers {
			server.CloseClientConnections()
			server.Close()
		}
	})

	It("connects to each broker", func() {
		for _, server := range mockServers {
			Eventually(server.connEstablishedCh).Should(BeClosed())
		}
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

				mockServers[0].serverCh <- buildRemoteFileLocation(1, servers[1].URL)
				mockServers[1].serverCh <- buildFileLocation(1)

				for i := 0; i < 10; i++ {
					mockServers[1].serverCh <- buildFileOffset(uint64(i+2), 101)
				}
			})

			It("writes to the correct broker", func(done Done) {
				defer close(done)

				writer, err := client.FetchWriter(expectedFileName)
				Expect(err).ToNot(HaveOccurred())

				_, err = writer.WriteToFile(expectedData)
				Expect(err).ToNot(HaveOccurred())

				var msg *messages.Client
				Eventually(mockServers[1].clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))

				Eventually(mockServers[1].clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_WriteToFile))
				Expect(msg.GetWriteToFile().GetData()).To(Equal(expectedData))
			}, 5)

		})

		Measure("Writes to a file 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				mockServers[0].serverCh <- buildFileLocation(1)
				count := 1000

				go func() {
					for i := 0; i < count; i++ {
						mockServers[0].serverCh <- buildFileOffset(uint64(i+2), 101)
					}
				}()

				go func() {
					for _ = range mockServers[0].clientCh {
						//NOP
					}
				}()

				writer, _ := client.FetchWriter(expectedFileName)

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
				mockServers[0].serverCh <- buildRemoteFileLocation(1, servers[1].URL)
				mockServers[1].serverCh <- buildFileLocation(1)
				mockServers[1].serverCh <- buildReadData(2, expectedData, 101)
			})

			It("reads from the correct broker", func(done Done) {
				defer close(done)

				reader, err := client.FetchReader(expectedFileName)
				Expect(err).ToNot(HaveOccurred())

				data, index, err := reader.ReadFromFile()
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
				Expect(index).To(BeEquivalentTo(101))

				var msg *messages.Client
				Eventually(mockServers[1].clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
				Expect(msg.GetFetchFile().GetName()).To(Equal(expectedFileName))
			})
		})

		Context("for unknown server", func() {
			BeforeEach(func() {
				mockServers[0].serverCh <- buildRemoteFileLocation(1, extraServerURL)
				extraMockServer.serverCh <- buildFileLocation(1)
				extraMockServer.serverCh <- buildReadData(2, expectedData, 101)
			})

			It("reads from the new broker", func(done Done) {
				defer close(done)

				reader, err := client.FetchReader(expectedFileName)
				Expect(err).ToNot(HaveOccurred())

				data, index, err := reader.ReadFromFile()
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(Equal(expectedData))
				Expect(index).To(BeEquivalentTo(101))

				var msg *messages.Client
				Eventually(extraMockServer.clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
				Expect(msg.GetFetchFile().GetName()).To(Equal(expectedFileName))
			})
		})

		Measure("Reads from a file 1000 times in under a second", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				mockServers[0].serverCh <- buildFileLocation(1)
				count := 1000
				data := []byte("some-data")

				go func() {
					for i := 0; i < count; i++ {
						mockServers[0].serverCh <- buildReadData(uint64(i+2), data, 0)
					}
				}()

				go func() {
					for _ = range mockServers[0].clientCh {
						//NOP
					}
				}()

				reader, err := client.FetchReader("some-file")
				Expect(err).ToNot(HaveOccurred())
				for i := 0; i < count; i++ {
					reader.ReadFromFile()
				}
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))

		}, 5)
	})

	XDescribe("LeaderOf()", func() {
		var (
			id uint64
		)

		BeforeEach(func(done Done) {
			defer close(done)
			mockServers[0].serverCh <- buildFileLocation(1)
		})

		It("returns the broker URI that is the leader of the given file", func(done Done) {
			defer close(done)
			uri, err := client.LeaderOf(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(uri).To(Equal(htmlToWs(servers[0].URL)))
		})

		It("returns an error for an unknown file ID", func(done Done) {
			defer close(done)
			_, err := client.LeaderOf(999)
			Expect(err).To(HaveOccurred())
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
