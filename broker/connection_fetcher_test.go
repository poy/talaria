package broker_test

import (
	"github.com/apoydence/talaria/broker"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConnectionFetcher", func() {

	var (
		mockServers []*mockServer
		servers     []*httptest.Server

		fetcher *broker.ConnectionFetcher
	)

	var createMockServers = func() []string {
		mockServers = nil
		servers = nil

		var urls []string
		for i := 0; i < 3; i++ {
			mockServer, server := startMockServer()
			mockServers = append(mockServers, mockServer)
			servers = append(servers, server)
			urls = append(urls, convertToWs(server.URL))
		}

		return urls
	}

	BeforeEach(func() {
		urls := createMockServers()

		var err error
		fetcher, err = broker.NewConnectionFetcher(urls...)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		for _, server := range mockServers {
			close(server.serverCh)
		}

		for _, server := range servers {
			server.CloseClientConnections()
			server.Close()
		}
	})

	Describe("Fetch()", func() {

		It("connects to each broker", func() {
			for _, server := range mockServers {
				Eventually(server.connEstablishedCh).Should(BeClosed())
			}
		})

		Context("for known servers", func() {
			Context("same broker", func() {
				BeforeEach(func() {
					mockServers[0].serverCh <- buildFileLocation(1)
				})

				It("it returns the correct connection", func() {
					conn, _, err := fetcher.Fetch("some-file")

					Expect(err).ToNot(HaveOccurred())
					Expect(conn.URL).To(Equal(convertToWs(servers[0].URL)))
				})
			})

			Context("redirects different broker", func() {
				BeforeEach(func() {
					mockServers[0].serverCh <- buildRemoteFileLocation(1, servers[1].URL)
					mockServers[1].serverCh <- buildFileLocation(1)
				})

				It("it returns the correct connection", func() {
					conn, _, err := fetcher.Fetch("some-file")

					Expect(err).ToNot(HaveOccurred())
					Expect(conn.URL).To(Equal(convertToWs(servers[1].URL)))
				})
			})
		})

		Context("for unknown server", func() {
			var (
				extraMockServer *mockServer
				extraServerURL  string
			)

			var createExtraMockServer = func() {
				mockServer, server := startMockServer()
				extraMockServer = mockServer
				servers = append(servers, server)
				extraServerURL = convertToWs(server.URL)
			}

			BeforeEach(func() {
				createExtraMockServer()

				mockServers[0].serverCh <- buildRemoteFileLocation(1, extraServerURL)
				extraMockServer.serverCh <- buildFileLocation(1)
			})

			It("returns the connection for the new broker", func() {
				conn, _, err := fetcher.Fetch("some-file")

				Expect(err).ToNot(HaveOccurred())
				Expect(conn.URL).To(Equal(extraServerURL))
			})

		})
	})
})
