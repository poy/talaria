package config

import (
	"log"

	"github.com/bradylove/envstruct"
)

type Talaria struct {
	Port         uint16 `env:"port"`
	IntraPort    uint16 `env:"intra_port"`
	SchedulerURI string `env:"scheduler_uri,required"`
}

func Load() Talaria {
	c := Talaria{
		Port:      8080,
		IntraPort: 8081,
	}

	if err := envstruct.Load(&c); err != nil {
		log.Fatal(err.Error())
	}

	return c
}
