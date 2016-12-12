package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
	"github.com/apoydence/talaria/scheduler/config"
	"github.com/apoydence/talaria/scheduler/internal/auditor"
	"github.com/apoydence/talaria/scheduler/internal/intraserver"
	"github.com/apoydence/talaria/scheduler/internal/nodefetcher"
	"github.com/apoydence/talaria/scheduler/internal/server"
	"google.golang.org/grpc"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log.Print("Starting Talaria Scheduler...")
	defer log.Print("Closing Talaria Scheduler")

	conf := config.Load()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on port %d", conf.Port)

	auditor := auditor.Start(time.Second, auditor.Generate(conf.Nodes...))

	nodeFetcher := nodefetcher.New(conf.Nodes)
	scheduler := server.New(nodeFetcher, auditor)
	intraServer := intraserver.New(nodeFetcher)

	grpcServer := grpc.NewServer()

	pb.RegisterSchedulerServer(grpcServer, scheduler)
	intra.RegisterSchedulerServer(grpcServer, intraServer)
	grpcServer.Serve(lis)
}
