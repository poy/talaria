package config

import (
	"log"

	"github.com/bradylove/envstruct"
)

type Talaria struct {
	Port uint16 `env:"port"`
}

func Load() Talaria {
	c := Talaria{
		Port: 8080,
	}

	if err := envstruct.Load(&c); err != nil {
		log.Fatal(err.Error())
	}

	return c
}
