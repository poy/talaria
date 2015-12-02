package broker_test

import (
	"fmt"
	"net/http/httptest"
	"sync"

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

	Describe("FetchFile", func() {
		It("round robins the brokers while fetching new files", func(done Done) {
			defer close(done)

			for _, server := range mockServers {
				server.serverCh <- buildFileLocation(1)
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
			mockServers[0].serverCh <- buildRemoteFileLocation(1, servers[1].URL)
			mockServers[1].serverCh <- buildFileLocation(1)

			_, err := client.FetchFile("some-file-1")
			Expect(err).ToNot(HaveOccurred())

			var msg *messages.Client
			Eventually(mockServers[1].clientCh).Should(Receive(&msg))
			Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(msg.GetFetchFile().GetName()).To(Equal("some-file-1"))
		})
	})

	Describe("WriteToFile()", func() {
		It("writes to the correct broker", func(done Done) {
			defer close(done)
			mockServers[0].serverCh <- buildRemoteFileLocation(1, servers[1].URL)
			mockServers[1].serverCh <- buildFileLocation(1)

			for i := 0; i < 10; i++ {
				mockServers[1].serverCh <- buildFileOffset(uint64(i+2), 101)
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

				id, ffErr := client.FetchFile("some-file-1")
				Expect(ffErr).ToNot(HaveOccurred())

				data := []byte("some-data")

				for i := 0; i < count; i++ {
					client.WriteToFile(id, data)
				}
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))

		}, 5)
	})

	Describe("ReadFromFile()", func() {
		It("reads from the correct broker", func(done Done) {
			defer close(done)
			expectedData := []byte("some-data")
			mockServers[0].serverCh <- buildRemoteFileLocation(1, servers[1].URL)
			mockServers[1].serverCh <- buildFileLocation(1)
			mockServers[1].serverCh <- buildReadData(2, expectedData, 101)

			id, ffErr := client.FetchFile("some-file-1")
			Expect(ffErr).ToNot(HaveOccurred())

			var msg *messages.Client
			Eventually(mockServers[1].clientCh).Should(Receive(&msg))
			Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(msg.GetFetchFile().GetName()).To(Equal("some-file-1"))

			data, index, err := client.ReadFromFile(id)
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(Equal(expectedData))
			Expect(index).To(BeEquivalentTo(101))
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

				id, ffErr := client.FetchFile("some-file-1")
				Expect(ffErr).ToNot(HaveOccurred())

				for i := 0; i < count; i++ {
					client.ReadFromFile(id)
				}
			})

			Expect(runtime.Seconds()).To(BeNumerically("<", 1))

		}, 5)
	})

	Describe("InitWriteIndex()", func() {
		It("initialize a file write index", func(done Done) {
			defer close(done)
			expectedData := []byte("some-data")
			mockServers[0].serverCh <- buildFileLocation(1)
			mockServers[0].serverCh <- buildFileOffset(uint64(2), 101)

			id, ffErr := client.FetchFile("some-file-1")
			Expect(ffErr).ToNot(HaveOccurred())

			var msg *messages.Client
			Eventually(mockServers[0].clientCh).Should(Receive(&msg))
			Expect(msg.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(msg.GetFetchFile().GetName()).To(Equal("some-file-1"))

			index, err := client.InitWriteIndex(id, 101, expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(index).To(BeEquivalentTo(101))
		})
	})

	Describe("LeaderOf()", func() {
		var (
			id uint64
		)

		BeforeEach(func(done Done) {
			defer close(done)
			mockServers[0].serverCh <- buildFileLocation(1)

			var ffErr error
			id, ffErr = client.FetchFile("some-file-1")
			Expect(ffErr).ToNot(HaveOccurred())
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
