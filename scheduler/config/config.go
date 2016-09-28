package config

import (
	"log"

	"github.com/bradylove/envstruct"
)

type Scheduler struct {
	Port  uint16   `env:"port"`
	Nodes []string `env:"nodes"`
}

func Load() Scheduler {
	c := Scheduler{
		Port: 8080,
	}

	if err := envstruct.Load(&c); err != nil {
		log.Fatal(err.Error())
	}

	return c
}
