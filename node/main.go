package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	pb "github.com/apoydence/talaria/api/v1"
	"github.com/apoydence/talaria/node/config"
	"github.com/apoydence/talaria/node/internal/raft"
	"github.com/apoydence/talaria/node/internal/raft/iofetcher"
	"github.com/apoydence/talaria/node/internal/raft/network"
	"github.com/apoydence/talaria/node/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	_ "net/http/pprof"
)

func main() {
	seed := time.Now().UnixNano()
	log.Print("Starting Talaria Node...")
	rand.Seed(seed)
	defer log.Print("Closing Talaria Node")
	conf := config.Load()

	grpclog.SetLogger(log.New(os.Stderr, "[GRPC]", log.LstdFlags))

	lis, err := net.Listen("tcp4", conf.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on %s", conf.Addr)

	ID := uint64(rand.Int63())
	log.Printf("Node ID=%d", ID)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// TODO: netowrk.Inbound should NOT create a grpc server and
	// therefore require the SchedulerInbound. We can then get rid
	// of this goofy function
	var intraInbound *network.Inbound
	ioFetcher := iofetcher.New(iofetcher.RaftClusterCreator(func(name string, bufferSize uint64, peers []string) (iofetcher.RaftCluster, error) {
		return raft.Build(name, intraInbound,
			raft.WithBufferSize(int(bufferSize)),
			raft.WithPeers(peers),
			raft.WithLogger(log.New(os.Stderr, fmt.Sprintf("[RAFT %s] ", name), log.LstdFlags)),
		)
	}), func() string { return intraInbound.Addr() })
	schedulerHandler := network.NewSchedulerInbound(lis.Addr().String(), ioFetcher)
	intraInbound = network.NewInbound(conf.IntraAddr, schedulerHandler)

	node := server.New(ioFetcher)

	grpcServer := grpc.NewServer()

	pb.RegisterNodeServer(grpcServer, node)
	grpcServer.Serve(lis)
}
