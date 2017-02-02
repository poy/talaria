package auditor

import (
	"log"

	"github.com/apoydence/talaria/api/intra"

	"google.golang.org/grpc"
)

func Generate(URIs ...string) map[Node]string {
	nodes := make(map[Node]string)
	for _, URI := range URIs {
		c, err := grpc.Dial(URI, grpc.WithInsecure())
		if err != nil {
			log.Printf("unable to dial %s: %s", URI, err)
			continue
		}
		client := intra.NewNodeClient(c)
		nodes[client] = URI
	}

	return nodes
}
