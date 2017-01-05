package raft

import (
	"log"

	rafthashi "github.com/hashicorp/raft"
)

type Config struct {
	*rafthashi.Config
	peers      []string
	bufferSize int
}

type BuildOp func(*Config)

func WithLogger(logger *log.Logger) func(*Config) {
	return func(conf *Config) {
		conf.Logger = logger
	}
}

func WithPeers(peers []string) func(*Config) {
	return func(conf *Config) {
		conf.peers = peers
	}
}

func WithBufferSize(size int) func(*Config) {
	if size <= 0 {
		log.Panicf("Invalid buffer size %d. Size must be greater than 0", size)
	}

	return func(conf *Config) {
		conf.bufferSize = size
	}
}
