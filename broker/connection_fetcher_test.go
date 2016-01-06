package broker_test

import (
	"github.com/apoydence/talaria/broker"
	"net/http/httptest"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConnectionFetcher", func() {

	var (
		expectedFile string

		mockServers []*mockServer
		servers     []*httptest.Server
		URLs        []string
		blacklist   []string

		fetcher *broker.ConnectionFetcher
	)

	var createMockServers = func() []string {
		expectedFile = "some-file"

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
		URLs = createMockServers()
	})

	JustBeforeEach(func() {
		var err error
		fetcher, err = broker.NewConnectionFetcher(blacklist, URLs...)
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
				Eventually(server.connEstablishedCh).Should(HaveLen(1))
			}
		})

		Context("for known servers", func() {
			Context("without blacklist", func() {
				BeforeEach(func() {
					blacklist = nil
				})

				Context("same broker", func() {
					BeforeEach(func() {
						mockServers[0].serverCh <- buildFileLocation(1)
					})

					It("it returns the correct connection", func() {
						conn, _, err := fetcher.Fetch(expectedFile)

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
						conn, _, err := fetcher.Fetch(expectedFile)

						Expect(err).ToNot(HaveOccurred())
						Expect(conn.URL).To(Equal(convertToWs(servers[1].URL)))
					})
				})
			})

			Context("with blacklist", func() {
				BeforeEach(func() {
					blacklist = []string{URLs[0]}

					mockServers[0].serverCh <- buildFileLocation(1)
				})

				It("returns a blacklisted error", func() {
					_, _, err := fetcher.Fetch(expectedFile)

					Expect(err).To(MatchError(broker.BlacklistedErr))
				})
			})

			Context("with errored connection", func() {

				var (
					expectedError string
				)

				BeforeEach(func() {
					expectedError = "some-error"
					mockServers[0].serverCh <- buildError(1, expectedError)
					mockServers[1].serverCh <- buildRemoteFileLocation(1, servers[0].URL)
					mockServers[0].serverCh <- buildFileLocation(1)
				})

				JustBeforeEach(func() {
					fetcher.Fetch(expectedFile)
				})

				It("reconnects to broker", func() {
					conn, _, err := fetcher.Fetch(expectedFile)

					Expect(err).ToNot(HaveOccurred())
					Expect(conn.URL).To(Equal(convertToWs(servers[0].URL)))
					Expect(mockServers[0].connEstablishedCh).To(HaveLen(2))
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

			Context("without blacklist", func() {
				BeforeEach(func() {
					blacklist = nil
				})

				It("returns the connection for the new broker", func() {
					conn, _, err := fetcher.Fetch(expectedFile)

					Expect(err).ToNot(HaveOccurred())
					Expect(conn.URL).To(Equal(extraServerURL))
				})

			})

			Context("with blacklist", func() {
				BeforeEach(func() {
					blacklist = []string{extraServerURL}
				})

				It("returns a blacklisted error", func() {
					_, _, err := fetcher.Fetch(expectedFile)

					Expect(err).To(MatchError(broker.BlacklistedErr))
				})
			})
		})
	})
})
