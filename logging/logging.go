package logging

import (
	"fmt"
	"os"
	"sync"

	gologging "github.com/op/go-logging"
)

var (
	syncRoot       *sync.RWMutex
	backendLeveled gologging.LeveledBackend
)

type Logger struct {
	log *gologging.Logger
}

func init() {
	syncRoot = &sync.RWMutex{}
	format := gologging.MustStringFormatter("%{color}%{time:15:04:05:000} %{shortfunc} > %{level:.4s} %{id:03x}%{color:reset} %{message}")
	backend := gologging.NewLogBackend(os.Stdout, "", 0)
	backendFormatter := gologging.NewBackendFormatter(backend, format)
	backendLeveled = gologging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(gologging.INFO, "")
	gologging.SetBackend(backendLeveled)
}

func SetLevel(level LogLevel) {
	syncRoot.Lock()
	defer syncRoot.Unlock()
	backendLeveled.SetLevel(convertLogLevel(level), "")
}

func Log(name string) Logger {
	return Logger{
		log: gologging.MustGetLogger(name),
	}
}

func (l Logger) Error(msg string, values ...interface{}) {
	l.log.Error(msg, values...)
}

func (l Logger) Info(msg string, values ...interface{}) {
	l.log.Info(msg, values...)
}

func (l Logger) Debug(msg string, values ...interface{}) {
	l.log.Debug(msg, values...)
}

func (l Logger) Fatal(msg string, values ...interface{}) {
	l.log.Fatal(fmt.Sprintf(msg, values...))
}

func (l Logger) Panic(msg string, values ...interface{}) {
	l.log.Panic(fmt.Sprintf(msg, values...))
}

func convertLogLevel(level LogLevel) gologging.Level {
	switch level {
	case ERROR:
		return gologging.ERROR
	case INFO:
		return gologging.INFO
	case DEBUG:
		return gologging.DEBUG
	default:
		panic(fmt.Sprintf("Unknown LogLevel: %v", level))
	}
}
