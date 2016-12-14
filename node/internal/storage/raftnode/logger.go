package raftnode

import "log"

type logger struct {
	l *log.Logger
}

func newLogger(l *log.Logger) logger {
	return logger{
		l: l,
	}
}

func (l logger) Debug(v ...interface{})                   { l.l.Print(v...) }
func (l logger) Debugf(format string, v ...interface{})   { l.l.Printf(format, v...) }
func (l logger) Error(v ...interface{})                   { l.l.Print(v...) }
func (l logger) Errorf(format string, v ...interface{})   { l.l.Printf(format, v...) }
func (l logger) Info(v ...interface{})                    { l.l.Print(v...) }
func (l logger) Infof(format string, v ...interface{})    { l.l.Printf(format, v...) }
func (l logger) Warning(v ...interface{})                 { l.l.Print(v...) }
func (l logger) Warningf(format string, v ...interface{}) { l.l.Printf(format, v...) }
func (l logger) Fatal(v ...interface{})                   { l.l.Print(v...) }
func (l logger) Fatalf(format string, v ...interface{})   { l.l.Printf(format, v...) }
func (l logger) Panic(v ...interface{})                   { l.l.Print(v...) }
func (l logger) Panicf(format string, v ...interface{})   { l.l.Printf(format, v...) }
