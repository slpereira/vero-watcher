package vero_watcher

import (
	"cloud.google.com/go/logging"
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type GoogleLogging struct {
	client              *logging.Client
	logger              *logging.Logger
	chanSendGoogleAsync chan logging.Entry
	running             atomic.Bool
}

func NewCloudLogging(projectId, loggerName string, bufferSize int) (*GoogleLogging, error) {
	if bufferSize <= 0 {
		return nil, errors.New("zero or negative buffer size")
	}
	cl, err := logging.NewClient(context.Background(), projectId)
	if err != nil {
		return nil, err
	}
	return &GoogleLogging{client: cl, logger: cl.Logger(loggerName),
		chanSendGoogleAsync: make(chan logging.Entry, bufferSize)}, nil
}

func (g *GoogleLogging) Start() error {
	if !g.running.CAS(false, true) {
		return errors.New("already running")
	}
	go g.process()
	return nil
}

func (g *GoogleLogging) Levels() []log.Level {
	return []log.Level{
		log.PanicLevel,
		log.TraceLevel,
		log.DebugLevel,
		log.InfoLevel,
		log.WarnLevel,
		log.ErrorLevel,
	}
}

func (g *GoogleLogging) Fire(entry *log.Entry) error {
	if !g.running.Load() {
		return nil
	}
	e := logging.Entry{
		Timestamp: entry.Time,
		Severity:  g.getSeverity(entry.Level),
		Payload:   entry.Message,
	}
	g.chanSendGoogleAsync <- e
	return nil
}

func (g *GoogleLogging) getSeverity(level log.Level) logging.Severity {
	switch level {
	case log.DebugLevel:
		return logging.Debug
	case log.InfoLevel:
		return logging.Info
	case log.WarnLevel:
		return logging.Warning
	case log.ErrorLevel:
		return logging.Error
	case log.PanicLevel:
		return logging.Critical
	case log.FatalLevel:
		return logging.Critical
	default:
		return logging.Default
	}
}

func (g *GoogleLogging) Close() error {
	g.running.Store(false)
	close(g.chanSendGoogleAsync)
	return g.logger.Flush()
}

func (g *GoogleLogging) process() {
	for {
		select {
		case e, ok := <-g.chanSendGoogleAsync:
			if !ok {
				return
			}
			g.logger.Log(e)
		}
	}
}
