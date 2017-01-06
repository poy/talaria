package config

import (
	"log"

	"github.com/bradylove/envstruct"
)

type Talaria struct {
	Addr      string `env:"ADDR,required"`
	IntraAddr string `env:"INTRA_ADDR,required"`
}

func Load() Talaria {
	c := Talaria{}

	if err := envstruct.Load(&c); err != nil {
		log.Fatal(err.Error())
	}

	return c
}
