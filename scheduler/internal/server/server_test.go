//go:generate hel
package server_test

import (
	"fmt"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/internal/server"
	"golang.org/x/net/context"

	. "github.com/apoydence/eachers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
	var (
		mockNodeFetcher *mockNodeFetcher
		mockNodeClient  *mockNodeClient
		createInfo      *pb.CreateInfo
		nodeURI         string

		s *server.Server
	)

	BeforeEach(func() {
		mockNodeClient = newMockNodeClient()
		mockNodeFetcher = newMockNodeFetcher()
		nodeURI = "some-uri"

		s = server.New(mockNodeFetcher)

		createInfo = &pb.CreateInfo{
			Name: "some-name",
		}
	})

	JustBeforeEach(func() {
		close(mockNodeFetcher.FetchNodeOutput.Client)
		close(mockNodeFetcher.FetchNodeOutput.URI)

		close(mockNodeClient.CreateOutput.Ret0)
		close(mockNodeClient.CreateOutput.Ret1)
	})

	Describe("Create()", func() {
		Context("nodes available", func() {
			BeforeEach(func() {
				mockNodeFetcher.FetchNodeOutput.Client <- mockNodeClient
				mockNodeFetcher.FetchNodeOutput.URI <- nodeURI
				mockNodeFetcher.FetchNodeOutput.Client <- mockNodeClient
				mockNodeFetcher.FetchNodeOutput.URI <- nodeURI
			})

			It("does not return an error and gives the URI", func() {
				resp, err := s.Create(context.Background(), createInfo)
				Expect(err).ToNot(HaveOccurred())
				Expect(resp.Uri).To(Equal(nodeURI))
			})

			It("fetches a new node each time", func() {
				s.Create(context.Background(), createInfo)
				s.Create(context.Background(), createInfo)

				Eventually(mockNodeFetcher.FetchNodeCalled).Should(HaveLen(2))
			})

			It("writes to fetched node", func() {
				expected := &intra.CreateInfo{
					Name: createInfo.Name,
				}
				s.Create(context.Background(), createInfo)

				Eventually(mockNodeClient.CreateInput.In).Should(BeCalled(With(expected)))
			})

			Context("node returns an error", func() {
				BeforeEach(func() {
					mockNodeClient.CreateOutput.Ret1 <- fmt.Errorf("some-error")
				})

				It("tries a new node", func() {
					_, err := s.Create(context.Background(), createInfo)
					Expect(err).ToNot(HaveOccurred())

					Eventually(mockNodeFetcher.FetchNodeCalled).Should(HaveLen(2))
				})

				Context("nodes always return an error", func() {
					BeforeEach(func() {
						for i := 0; i < 10; i++ {
							mockNodeClient.CreateOutput.Ret1 <- fmt.Errorf("some-error")
							mockNodeFetcher.FetchNodeOutput.Client <- mockNodeClient
						}
					})

					It("gives up after 5 tries and returns an error", func() {
						_, err := s.Create(context.Background(), createInfo)
						Expect(err).To(HaveOccurred())

						Eventually(mockNodeFetcher.FetchNodeCalled).Should(HaveLen(5))
					})
				})
			})
		})

		Context("no nodes available", func() {
			It("returns an error", func() {
				_, err := s.Create(context.Background(), createInfo)
				Expect(err).To(HaveOccurred())
			})
		})
	})
})
