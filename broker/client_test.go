package broker_test

import (
	"fmt"
	"net/http/httptest"
	"sync"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/messages"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {

	var (
		mockServers []*mockServer
		servers     []*httptest.Server
		client      *broker.Client
	)

	BeforeEach(func() {
		mockServers = nil
		servers = nil
		var urls []string
		for i := 0; i < 3; i++ {
			mockServer, server := startMockServer()
			mockServers = append(mockServers, mockServer)
			servers = append(servers, server)
			urls = append(urls, "ws"+server.URL[4:])
		}

		var err error
		client, err = broker.NewClient(urls...)
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

	Context("FetchFile", func() {
		It("round robins the brokers while fetching new files", func(done Done) {
			defer close(done)

			for _, server := range mockServers {
				server.serverCh <- buildFileLocation(99)
			}

			var wg sync.WaitGroup
			defer wg.Wait()
			wg.Add(len(mockServers))
			for i := range mockServers {
				go func(iter int) {
					defer GinkgoRecover()
					defer wg.Done()
					_, err := client.FetchFile(fmt.Sprintf("some-file-%d", iter))
					Expect(err).ToNot(HaveOccurred())
				}(i)
			}

			for i, server := range mockServers {
				var msg *messages.Client
				Eventually(server.clientCh).Should(Receive(&msg))
				Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
				Expect(msg.GetFetchFile().GetName()).To(Equal(fmt.Sprintf("some-file-%d", i)))
			}
		}, 5)

		It("re-fetches a file if it is redirected", func(done Done) {
			defer close(done)
			mockServers[0].serverCh <- buildRemoteFileLocation(99, servers[1].URL)
			mockServers[1].serverCh <- buildFileLocation(99)

			_, err := client.FetchFile("some-file-1")
			Expect(err).ToNot(HaveOccurred())

			var msg *messages.Client
			Eventually(mockServers[1].clientCh).Should(Receive(&msg))
			Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(msg.GetFetchFile().GetName()).To(Equal("some-file-1"))
		})
	})

	Context("WriteToFile", func() {
		It("writes to the correct broker", func(done Done) {
			defer close(done)
			mockServers[0].serverCh <- buildRemoteFileLocation(99, servers[1].URL)
			mockServers[1].serverCh <- buildFileLocation(99)

			for i := 0; i < 10; i++ {
				mockServers[1].serverCh <- buildFileOffset(99, 101)
			}

			id, ffErr := client.FetchFile("some-file-1")
			Expect(ffErr).ToNot(HaveOccurred())

			var msg *messages.Client
			Eventually(mockServers[1].clientCh).Should(Receive(&msg))
			Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(msg.GetFetchFile().GetName()).To(Equal("some-file-1"))

			var wg sync.WaitGroup
			defer wg.Wait()
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					expectedData := []byte("some-data")
					_, err := client.WriteToFile(id, expectedData)
					Expect(err).ToNot(HaveOccurred())

					var msg *messages.Client
					Eventually(mockServers[1].clientCh).Should(Receive(&msg))
					Expect(msg.GetMessageType()).To(Equal(messages.Client_WriteToFile))
					Expect(msg.GetWriteToFile().GetData()).To(Equal(expectedData))
				}()
			}
		}, 5)
	})

	Context("ReadFromFile", func() {
		It("reads from the correct broker", func(done Done) {
			defer close(done)
			expectedData := []byte("some-data")
			mockServers[0].serverCh <- buildRemoteFileLocation(99, servers[1].URL)
			mockServers[1].serverCh <- buildFileLocation(99)
			mockServers[1].serverCh <- buildReadData(99, expectedData)

			id, ffErr := client.FetchFile("some-file-1")
			Expect(ffErr).ToNot(HaveOccurred())

			var msg *messages.Client
			Eventually(mockServers[1].clientCh).Should(Receive(&msg))
			Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(msg.GetFetchFile().GetName()).To(Equal("some-file-1"))

			data, err := client.ReadFromFile(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal(expectedData))
		})
	})

})

func startMockServer() (*mockServer, *httptest.Server) {
	mockServer := newMockServer()
	server := httptest.NewServer(mockServer)
	return mockServer, server
}
