package broker_test

import (
	"net/http"
	"net/http/httptest"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/pb/messages"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("InnerBroker", func() {
	var (
		innerBroker *broker.InnerBroker
		server      *httptest.Server
		mockServer  *mockServer
		URL         string
	)

	BeforeEach(func() {
		mockServer = newMockServer()
		server = httptest.NewServer(mockServer)
		URL = "ws" + server.URL[4:]
		innerBroker = broker.NewInnerBroker()
	})

	Describe("ProvideConn()", func() {
		It("provides a writer for the given URI", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildFileLocation(1)
			mockServer.serverCh <- buildFileLocation(2)

			expectedName1 := "some-name-1"
			expectedName2 := "some-name-2"
			writer1 := innerBroker.ProvideConn(expectedName1, URL)

			Eventually(mockServer.connEstablishedCh).Should(BeClosed())
			var req *http.Request
			Eventually(mockServer.reqCh).Should(Receive(&req))
			Expect(req.RequestURI).To(Equal(broker.InnerEndpoint))
			Expect(writer1).ToNot(BeNil())
			var clientMsg1 *messages.Client
			Expect(mockServer.clientCh).To(Receive(&clientMsg1))
			Expect(clientMsg1.GetMessageType()).To(Equal(messages.Client_FetchFile))

			By("not trying to use a second connection to the same broker")
			writer2 := innerBroker.ProvideConn(expectedName2, URL)
			Expect(writer2).ToNot(BeNil())
			var clientMsg2 *messages.Client
			Expect(mockServer.clientCh).To(Receive(&clientMsg2))
			Expect(clientMsg2.GetMessageType()).To(Equal(messages.Client_FetchFile))
			Expect(clientMsg1.FetchFile.GetFileId()).ToNot(Equal(clientMsg2.FetchFile.GetFileId()))
		})

		It("writes to the connection", func(done Done) {
			defer close(done)

			mockServer.serverCh <- buildFileLocation(1)
			mockServer.serverCh <- buildFileOffset(2, 101)

			expectedName := "some-name"
			expectedData := []byte("some-data")

			writer := innerBroker.ProvideConn(expectedName, URL)

			n, err := writer.Write(expectedData)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(len(expectedData)))
			var writeMsg *messages.Client
			Eventually(mockServer.clientCh).Should(Receive(&writeMsg))
		})
	})

})
