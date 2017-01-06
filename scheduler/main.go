package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/scheduler/config"
	"github.com/apoydence/talaria/scheduler/internal/auditor"
	"github.com/apoydence/talaria/scheduler/internal/nodefetcher"
	"github.com/apoydence/talaria/scheduler/internal/server"
	"google.golang.org/grpc"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log.Print("Starting Talaria Scheduler...")
	defer log.Print("Closing Talaria Scheduler")

	conf := config.Load()
	var nodeAddrs []string
	for _, node := range conf.Nodes {
		addr, err := net.ResolveTCPAddr("tcp4", node)
		if err != nil {
			log.Printf("Unable to resolve node address %s: %s", node, err)
			continue
		}
		nodeAddrs = append(nodeAddrs, addr.String())
	}

	lis, err := net.Listen("tcp4", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on port %d", conf.Port)

	auditor := auditor.Start(time.Second, auditor.Generate(nodeAddrs...))

	nodeFetcher := nodefetcher.New(nodeAddrs)
	scheduler := server.New(nodeFetcher, auditor)

	grpcServer := grpc.NewServer()

	pb.RegisterSchedulerServer(grpcServer, scheduler)
	grpcServer.Serve(lis)
}
