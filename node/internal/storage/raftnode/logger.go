package raftnode

import "log"

type logger struct {
}

func newLogger() logger {
	return logger{}
}

func (l logger) Debug(v ...interface{})                   { log.Print(v...) }
func (l logger) Debugf(format string, v ...interface{})   { log.Printf(format, v...) }
func (l logger) Error(v ...interface{})                   { log.Print(v...) }
func (l logger) Errorf(format string, v ...interface{})   { log.Printf(format, v...) }
func (l logger) Info(v ...interface{})                    { log.Print(v...) }
func (l logger) Infof(format string, v ...interface{})    { log.Printf(format, v...) }
func (l logger) Warning(v ...interface{})                 { log.Print(v...) }
func (l logger) Warningf(format string, v ...interface{}) { log.Printf(format, v...) }
func (l logger) Fatal(v ...interface{})                   { log.Print(v...) }
func (l logger) Fatalf(format string, v ...interface{})   { log.Printf(format, v...) }
func (l logger) Panic(v ...interface{})                   { log.Print(v...) }
func (l logger) Panicf(format string, v ...interface{})   { log.Printf(format, v...) }
