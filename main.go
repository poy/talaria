package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/apoydence/talaria/broker"
	"github.com/apoydence/talaria/broker/leadervalidator"
	"github.com/apoydence/talaria/kvstore"
	"github.com/apoydence/talaria/logging"
	"github.com/apoydence/talaria/orchestrator"
	"github.com/codegangsta/cli"
)

const (
	dataDir       = "dataDir"
	logLevel      = "logLevel"
	segmentLength = "segmentLength"
	numSegments   = "numSegments"
	numReplicas   = "numReplicas"
	port          = "port"
	healthPort    = "healthPort"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	app := cli.NewApp()
	app.Name = "talaria"
	app.Usage = "Distribute your data"
	app.Action = func(c *cli.Context) {
		run(c)
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  dataDir + ", d",
			Usage: "The directory where the segmented files are stored",
		},
		cli.StringFlag{
			Name:  logLevel,
			Value: "INFO",
			Usage: "The log level",
		},
		cli.IntFlag{
			Name:  segmentLength + ", l",
			Value: 1024 * 1024,
			Usage: "The desired number of bytes for each segment",
		},
		cli.IntFlag{
			Name:  numSegments + ", n",
			Value: 1024 * 1024,
			Usage: "The desired number of bytes for each segment",
		},
		cli.IntFlag{
			Name:  numReplicas + ", r",
			Value: 2,
			Usage: "The number of replicas for each partition",
		},
		cli.IntFlag{
			Name:  port + ", p",
			Value: 8888,
			Usage: "The port to use",
		},
		cli.IntFlag{
			Name:  healthPort + ", hp",
			Value: 8889,
			Usage: "The port to use for health checking",
		},
	}

	app.Run(os.Args)
}

func run(c *cli.Context) {
	validateFlags(c)
	setLogLevel(c)

	clientAddr := fmt.Sprintf("ws://localhost:%d", c.Int(port))
	kvStore := kvstore.New(clientAddr, c.Int(healthPort))
	ioProvider := broker.NewFileProvider(c.String(dataDir), uint64(c.Int(segmentLength)), uint64(c.Int(numSegments)), time.Second)

	readerFetcher := broker.NewLazyReaderFetcher(clientAddr)
	replicaManager := broker.NewReplicatedFileManager(ioProvider, readerFetcher)

	leaderValidator := leadervalidator.New(kvStore, readerFetcher)

	orch := orchestrator.New(clientAddr, uint(c.Int(numReplicas)), kvStore, replicaManager, leaderValidator, ioProvider)
	orch.ParticipateInElections()

	broker.StartBrokerServer(c.Int(port), orch, ioProvider)
}

func setLogLevel(c *cli.Context) {
	logFlag := c.String(logLevel)
	var logLevel logging.LogLevel
	err := logLevel.UnmarshalJSON([]byte(logFlag))
	if err != nil {
		quit(fmt.Sprintf("Unable to parse log level: %s", logFlag), c)
	}

	logging.SetLevel(logLevel)
}

func validateFlags(c *cli.Context) {
	if !c.IsSet(dataDir) {
		quit(fmt.Sprintf("%s is required", dataDir), c)
	}
}

func quit(msg string, c *cli.Context) {
	fmt.Println(msg)
	cli.ShowAppHelp(c)
	os.Exit(1)
}
