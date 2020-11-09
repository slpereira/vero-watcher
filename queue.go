package vero_watcher

import (
	"github.com/rjeczalik/notify"
	"os"
)

// Queue represents the interface for any kind of queue
type Queue interface {
	Open() error
	Close() error
	Restore(filters []DirFilter) error
	AddFileEvent(value *FileEvent) error
	GetFileEvents() ([]*FileEvent, error)
	RemoveFileEvent(value *FileEvent) error
	GetStatistics() (*QueueStatistics, error)
}

type QueueStatistics struct {
	QueuedEvents     []*FileEvent `json:"queued_events"`
	ProcessingEvents []*FileEvent `json:"processing_events"`
	QueuePath        string `json:"queue_path"`
	DirEvents        []*DirEvent `json:"dir_events"`
}

type QueueWatcherCommand struct {
	q Queue
}

func NewQueueCommand(q Queue) *QueueWatcherCommand {
	return &QueueWatcherCommand{q}
}

func (q *QueueWatcherCommand) Process(e notify.EventInfo) error {
	info, err := os.Stat(e.Path())
	if err != nil {
		return err
	}
	return q.q.AddFileEvent(&FileEvent{
		Name:    e.Path(),
		Event:   e.Event(),
		ModTime: info.ModTime(),
	})
}
