package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/apoydence/talaria/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	verbose = flag.Bool("verbose", false, "Verbose mode")

	schedulerUri = flag.String("scheduler", "", "The URI for the scheduler")
	nodeUri      = flag.String("node", "", "The URI for the node")
	bufferName   = flag.String("buffer", "", "The buffer to interact with")

	list      = flag.Bool("list", false, "List cluster info")
	create    = flag.Bool("create", false, "Create a buffer")
	tail      = flag.Bool("tail", false, "The buffer to tail")
	writeData = flag.Bool("write", false, "Write data from STDIN")
)

func main() {
	flag.Parse()

	if !*verbose {
		grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	}

	if *create {
		createCommand()
		return
	}

	if *tail {
		tailCommand()
		return
	}

	if *writeData {
		writeDataCommand()
		return
	}

	if *list {
		listCommand()
		return
	}

	onlyOneCommandUsage()
}

func createCommand() {
	if *tail || *writeData || *list {
		onlyOneCommandUsage()
	}

	if *bufferName == "" {
		log.Fatal("You must provide a buffer name")
	}

	if *schedulerUri == "" {
		log.Fatal("You must provide a scheduler URI")
	}

	if *nodeUri != "" {
		log.Fatal("You can't provide a node URI")
	}

	client := setupSchedulerClient()

	_, err := client.Create(context.Background(), &pb.CreateInfo{Name: *bufferName})
	if err != nil {
		log.Fatalf("unable to create buffer %s: %s", *bufferName, err)
	}
}

func setupSchedulerClient() pb.SchedulerClient {
	conn, err := grpc.Dial(*schedulerUri, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("unable to connect to scheduler: %s", err)
	}
	return pb.NewSchedulerClient(conn)
}

func setupNodeClient() pb.TalariaClient {
	conn, err := grpc.Dial(*nodeUri, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("unable to connect to node: %s", err)
	}
	return pb.NewTalariaClient(conn)
}

func tailCommand() {
	if *create || *writeData || *list {
		onlyOneCommandUsage()
	}

	if *bufferName == "" {
		log.Fatal("You must provide a buffer name")
	}

	if *schedulerUri != "" {
		log.Fatal("You can't provide a scheduler URI")
	}

	if *nodeUri == "" {
		log.Fatal("You must provide a node URI")
	}

	client := setupNodeClient()
	rx, err := client.Read(context.Background(), &pb.BufferInfo{Name: *bufferName})
	if err != nil {
		log.Fatal(err)
	}

	for {
		packet, err := rx.Recv()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(packet)
	}
}

func writeDataCommand() {
	if *create || *tail || *list {
		onlyOneCommandUsage()
	}

	if *bufferName == "" {
		log.Fatal("You must provide a buffer name")
	}

	if *schedulerUri != "" {
		log.Fatal("You can't provide a scheduler URI")
	}

	if *nodeUri == "" {
		log.Fatal("You must provide a node URI")
	}

	client := setupNodeClient()
	tx, err := client.Write(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	buffer := make([]byte, 1024)
	for {
		n, err := os.Stdin.Read(buffer)
		if err == io.EOF {
			os.Exit(0)
		}

		if err != nil {
			log.Fatal(err)
		}

		tx.Send(&pb.WriteDataPacket{
			Name:    *bufferName,
			Message: buffer[:n],
		})
		fmt.Println("Wrote", string(buffer[:n]))
	}
}

func listCommand() {
	if *create || *tail || *writeData {
		onlyOneCommandUsage()
	}

	if *schedulerUri == "" {
		log.Fatal("You must provide a scheduler URI")
	}

	if *nodeUri != "" {
		log.Fatal("You can't provide a node URI")
	}

	client := setupSchedulerClient()

	listInfo := &pb.ListInfo{}
	if *bufferName != "" {
		listInfo.Names = []string{*bufferName}
	}

	resp, err := client.ListClusterInfo(context.Background(), listInfo)
	if err != nil {
		log.Fatalf("unable to create buffer %s: %s", *bufferName, err)
	}

	if len(resp.Info) == 0 {
		fmt.Println("<NO INFO REPORTED>")
		return
	}

	fmt.Println("BUFFER NAME:")
	fmt.Println(resp.Info[0].Name)
	fmt.Println("\nLEADER:")
	if resp.Info[0].Leader == "" {
		resp.Info[0].Leader = "<NO LEADER REPORTED>"
	}
	fmt.Println(resp.Info[0].Leader)

	fmt.Println("\nNODES:")
	for _, n := range resp.Info[0].Nodes {
		fmt.Printf("URI: %s -> ID:%x\n", n.URI, n.ID)
	}
}

func onlyOneCommandUsage() {
	log.Fatal("Use only one create, tail, list or write")
}
