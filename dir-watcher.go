package vero_watcher

import (
	"github.com/rjeczalik/notify"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type DirWatcherCommand interface {
	Process(e notify.EventInfo) error
}

type DirFilter func(name string) error

type DirWatcher struct {
	paths   []string
	events  []notify.Event
	size    int
	c       chan notify.EventInfo
	command DirWatcherCommand
	r       atomic.Bool
	filters []DirFilter
}

func NewDirWatcher(paths []string, command DirWatcherCommand, size int, filters []DirFilter, events ...notify.Event) *DirWatcher {
	return &DirWatcher{
		paths:   paths,
		events:  events,
		size:    size,
		command: command,
		filters: filters,
	}
}

func (w *DirWatcher) Start() error {
	if !w.r.CAS(false, true) {
		return ErrAlreadyRunning
	}
	w.c = make(chan notify.EventInfo, w.size)
	for _,p:=range w.paths {
		log.Infof("watching %s", p)
		if err := notify.Watch(p, w.c, w.events...); err != nil {
			return err
		}
	}
	go w.receiver()
	return nil
}

func (w *DirWatcher) Stop() error {
	if !w.r.CAS(true, false) {
		return ErrNotRunning
	}
	notify.Stop(w.c)
	close(w.c)
	return nil
}

func (w *DirWatcher) receiver() {
	for {
		select {
		case ev, ok := <-w.c:
			if !ok {
				return
			}
			log.Debugf("event %v", ev)
			filtered := true
			for _, f := range w.filters {
				if err := f(ev.Path()); err != nil {
					if err != ErrSkip {
						log.Warnf("filtered function failed for event %v: %v", ev, err)
					}
					filtered = false
					break
				}
			}
			if !filtered {
				log.Debugf("event %v skipped", ev)
				continue
			}
			log.Infof("watch event:%v", ev)
			if err := w.command.Process(ev); err != nil {
				log.Errorf("error processing %s: %v", ev.Path(), err)
			}
		}
	}
}
