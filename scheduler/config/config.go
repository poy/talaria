package config

import (
	"log"

	"github.com/bradylove/envstruct"
)

type Scheduler struct {
	Addr  string   `env:"ADDR,required"`
	Nodes []string `env:"NODES"`
}

func Load() Scheduler {
	c := Scheduler{}

	if err := envstruct.Load(&c); err != nil {
		log.Fatal(err.Error())
	}

	return c
}
