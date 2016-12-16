package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/apoydence/talaria/node/config"
	"github.com/apoydence/talaria/node/internal/server"
	"github.com/apoydence/talaria/node/internal/storage"
	"github.com/apoydence/talaria/node/internal/storage/intraserver"
	"github.com/apoydence/talaria/node/internal/storage/intraserver/router"
	"github.com/apoydence/talaria/node/internal/storage/urifinder"
	"github.com/apoydence/talaria/pb"
	"github.com/apoydence/talaria/pb/intra"
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

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on port %d", conf.Port)

	ID := uint64(rand.Int63())
	log.Printf("Node ID=%d", ID)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	uriFinder := urifinder.New(conf.SchedulerURI)
	router := router.New()
	store := storage.New(ID, router, uriFinder)
	talaria := server.New(store)
	intraServer := intraserver.New(ID, store, router)

	grpcServer := grpc.NewServer()

	intra.RegisterNodeServer(grpcServer, intraServer)
	pb.RegisterTalariaServer(grpcServer, talaria)
	grpcServer.Serve(lis)
}
