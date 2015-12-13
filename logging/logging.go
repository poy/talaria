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
	l := gologging.MustGetLogger(name)
	l.ExtraCalldepth = 1
	return Logger{
		log: l,
	}
}

func (l Logger) Error(msg string, err error) {
	l.log.Error(fmt.Sprintf("%s: %v", msg, err))
}

func (l Logger) Errorf(msg string, values ...interface{}) {
	l.log.Error(msg, values...)
}

func (l Logger) Info(msg string, values ...interface{}) {
	l.log.Info(msg, values...)
}

func (l Logger) Debug(msg string, values ...interface{}) {
	l.log.Debug(msg, values...)
}

func (l Logger) Fatal(msg string, err error) {
	l.log.Fatalf("%s: %v", msg, err)
}

func (l Logger) Fatalf(msg string, values ...interface{}) {
	l.log.Fatalf(msg, values...)
}

func (l Logger) Panic(msg string, err error) {
	l.log.Panicf("%s: %v", msg, err)
}

func (l Logger) Panicf(msg string, values ...interface{}) {
	l.log.Panicf(msg, values...)
}

func convertLogLevel(level LogLevel) gologging.Level {
	switch level {
	case CRITICAL:
		return gologging.CRITICAL
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
