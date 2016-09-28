package end2end_test

import (
	"fmt"
	"math/rand"

	"golang.org/x/net/context"

	. "github.com/apoydence/eachers"
	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("End2end", func() {
	Describe("Create()", func() {
		var (
			createInfo *pb.CreateInfo
		)

		BeforeEach(func() {
			createInfo = &pb.CreateInfo{
				Name: createName(),
			}
		})

		JustBeforeEach(func() {
			testhelpers.AlwaysReturn(mockServer.CreateOutput.Ret0, new(intra.CreateResponse))
			close(mockServer.CreateOutput.Ret1)
		})

		Context("Create() doesn't return an error", func() {
			It("selects Node to create buffer via intra API", func() {
				_, err := schedulerClient.Create(context.Background(), createInfo)
				Expect(err).ToNot(HaveOccurred())
				expected := &intra.CreateInfo{
					Name: createInfo.Name,
				}

				Eventually(mockServer.CreateInput.Arg1).Should(BeCalled(With(expected)))
			})
		})
	})
})

func createName() string {
	return fmt.Sprintf("some-buffer-%d", rand.Int63())
}
