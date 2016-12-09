package auditor

import (
	"log"

	"github.com/apoydence/talaria/pb/intra"

	"google.golang.org/grpc"
)

func Generate(URIs ...string) []Node {
	var nodes []Node
	for _, URI := range URIs {
		c, err := grpc.Dial(URI, grpc.WithInsecure())
		if err != nil {
			log.Printf("unable to dial %s: %s", URI, err)
			continue
		}
		client := intra.NewNodeClient(c)
		nodes = append(nodes, client)
	}

	return nodes
}
