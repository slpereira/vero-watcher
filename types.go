package vero_watcher

import (
	"errors"
	"github.com/rjeczalik/notify"
	"time"
)

var (
	ErrAlreadyRunning = errors.New("already running")
	ErrNotRunning     = errors.New("not running")
	ErrSkip 		  = errors.New("skip")
)

const (
	DefaultWatcherChannelSize = 128
	FileEventBucket = "file-event"
	DirEventBucket = "dir-event"
	ProcessingEventBucket = "file-event-processing"
)

// FileEvent is a representation of a file event
type FileEvent struct {
	Name    string // the complete path and name of the file
	ModTime time.Time
	Event   notify.Event
}

type DirEvent struct {
	Name    string
	ModTime time.Time
}

