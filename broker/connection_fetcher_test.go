package broker_test

import (
	"net/http/httptest"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/pb/messages"

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

	Describe("FetchConnection()", func() {
		Context("connection available", func() {

			var (
				expectedURL string
			)

			BeforeEach(func() {
				expectedURL = convertToWs(servers[0].URL)
			})

			It("returns the correct connection", func() {
				conn, err := fetcher.FetchConnection(expectedURL)

				Expect(err).ToNot(HaveOccurred())
				Expect(conn.URL).To(Equal(expectedURL))
			})

			Context("populated blacklist", func() {
				BeforeEach(func() {
					blacklist = []string{expectedURL}
				})

				It("does not return a connection on the blacklist", func() {
					_, err := fetcher.FetchConnection(expectedURL)

					Expect(err).To(HaveOccurred())
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

				It("returns the connection", func() {
					conn, err := fetcher.FetchConnection(extraServerURL)

					Expect(err).ToNot(HaveOccurred())
					Expect(conn.URL).To(Equal(extraServerURL))
				})

			})

		})

		Context("connection not available", func() {
			It("returns an error", func() {
				_, err := fetcher.FetchConnection("ws://some.url")

				Expect(err).To(HaveOccurred())
			})
		})
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
						conn, _, err := fetcher.Fetch(expectedFile, false)

						Expect(err).ToNot(HaveOccurred())
						Expect(conn.URL).To(Equal(convertToWs(servers[0].URL)))
					})

					It("does a create fetch", func() {
						fetcher.Fetch(expectedFile, true)

						var clientMsg *messages.Client
						Eventually(mockServers[0].clientCh).Should(Receive(&clientMsg))
						Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
						Expect(clientMsg.FetchFile.GetCreate()).To(BeTrue())
					})

					It("does not do a create fetch", func() {
						fetcher.Fetch(expectedFile, false)

						var clientMsg *messages.Client
						Eventually(mockServers[0].clientCh).Should(Receive(&clientMsg))
						Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
						Expect(clientMsg.FetchFile.GetCreate()).To(BeFalse())
					})

				})

				Context("redirects different broker", func() {
					BeforeEach(func() {
						mockServers[0].serverCh <- buildRemoteFileLocation(1, servers[1].URL)
						mockServers[1].serverCh <- buildFileLocation(1)
					})

					It("it returns the correct connection", func() {
						conn, _, err := fetcher.Fetch(expectedFile, false)

						Expect(err).ToNot(HaveOccurred())
						Expect(conn.URL).To(Equal(convertToWs(servers[1].URL)))
					})

					It("does a create fetch", func() {
						fetcher.Fetch(expectedFile, true)

						var clientMsg *messages.Client
						Eventually(mockServers[0].clientCh).Should(Receive(&clientMsg))
						Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
						Expect(clientMsg.FetchFile.GetCreate()).To(BeTrue())
					})

					It("does not do a create fetch", func() {
						fetcher.Fetch(expectedFile, false)

						var clientMsg *messages.Client
						Eventually(mockServers[1].clientCh).Should(Receive(&clientMsg))
						Expect(clientMsg.GetMessageType()).To(Equal(messages.Client_FetchFile))
						Expect(clientMsg.FetchFile.GetCreate()).To(BeFalse())
					})
				})
			})

			Context("with blacklist", func() {
				BeforeEach(func() {
					blacklist = []string{URLs[0]}

					mockServers[0].serverCh <- buildFileLocation(1)
				})

				It("returns a blacklisted error", func() {
					_, _, err := fetcher.Fetch(expectedFile, false)

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
					mockServers[0].serverCh <- buildFileLocation(2)
				})

				JustBeforeEach(func() {
					fetcher.Fetch(expectedFile, false)
				})

				It("reconnects to broker", func(done Done) {
					defer close(done)
					conn, _, err := fetcher.Fetch(expectedFile, false)

					Expect(err).ToNot(HaveOccurred())
					Expect(conn.URL).To(Equal(convertToWs(servers[0].URL)))
					Expect(mockServers[0].connEstablishedCh).To(HaveLen(2))
				})

				Context("the broker is dead", func() {

					BeforeEach(func() {
						mockServers[1].serverCh <- buildFileLocation(2)
					})

					JustBeforeEach(func() {
						servers[0].Close()
					})

					It("tries a different connection", func(done Done) {
						defer close(done)
						conn, _, err := fetcher.Fetch(expectedFile, false)

						Expect(err).ToNot(HaveOccurred())
						Expect(conn.URL).To(Equal(convertToWs(servers[1].URL)))
					})
				})
			})

			Context("one of the connections gives a websocket error", func() {
				JustBeforeEach(func() {
					servers[0].CloseClientConnections()
					mockServers[1].serverCh <- buildFileLocation(1)
				})

				It("should try to use the next connection to fetch", func(done Done) {
					defer close(done)
					conn, _, err := fetcher.Fetch(expectedFile, false)

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

			Context("without blacklist", func() {
				BeforeEach(func() {
					blacklist = nil
				})

				It("returns the connection for the new broker", func() {
					conn, _, err := fetcher.Fetch(expectedFile, false)

					Expect(err).ToNot(HaveOccurred())
					Expect(conn.URL).To(Equal(extraServerURL))
				})

			})

			Context("with blacklist", func() {
				BeforeEach(func() {
					blacklist = []string{extraServerURL}
				})

				It("returns a blacklisted error", func() {
					_, _, err := fetcher.Fetch(expectedFile, false)

					Expect(err).To(MatchError(broker.BlacklistedErr))
				})
			})
		})
	})
})
