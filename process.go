package vero_watcher

import (
	"context"
	"github.com/rjeczalik/notify"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"
	"path/filepath"
	"sync"
	"time"
)

type ProcessCommand interface {
	Execute(event *FileEvent) error
}

type Process struct {
	w       *DirWatcher
	q       Queue
	running atomic.Bool
	t       *time.Ticker
	cmd     ProcessCommand
	sem     *semaphore.Weighted // control how many commands are processed in parallel
	mu      sync.Mutex          // this lock is to avoid stop the process while executing commands
	wg      sync.WaitGroup
	filters []DirFilter
}

func NewProcess(paths []string, q Queue, filters []DirFilter, cmd ProcessCommand,
	parallel int64, recursive bool, processInterval time.Duration) *Process {
	var ps []string
	if recursive {
		for _, p := range paths {
			ps = append(ps, filepath.Join(p, "..."))
		}
	} else {
		ps = paths
	}
	w := NewDirWatcher(ps, &QueueWatcherCommand{q}, DefaultWatcherChannelSize, filters,
		notify.Create, notify.Write, notify.Rename)
	return &Process{
		w:       w,
		q:       q,
		t:       time.NewTicker(processInterval),
		cmd:     cmd,
		sem:     semaphore.NewWeighted(parallel),
		filters: filters,
	}

}

func (p *Process) Start() error {
	if !p.running.CAS(false, true) {
		return ErrAlreadyRunning
	}
	if err := p.q.Open(); err != nil {
		return err
	}
	if err := p.q.Restore(p.filters); err != nil {
		return err
	}
	if err := p.w.Start(); err != nil {
		return err
	}
	go p.processQueue()
	return nil
}

func (p *Process) Stop() error {
	if !p.running.Load() {
		return ErrNotRunning
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// wait for all go routines executing to finish
	// we cannot close the queue if there is a go routine processing a file
	p.wg.Wait()
	if err := p.q.Close(); err != nil {
		log.Warnf("cannot close the queue: %v", err)
	}
	err := p.w.Stop()
	p.t.Stop()
	p.running.Store(false)
	return err
}

func (p *Process) Wait() {

}

// processQueue is the go routine responsible to process all files in the queue
func (p *Process) processQueue() {
	for {
		select {
		case <-p.t.C:
			p.mu.Lock()
			if !p.running.Load() {
				log.Info("process queue finished")
				p.mu.Unlock()
				return
			}
			evs, err := p.q.GetFileEvents()
			if err != nil {
				// TODO We must return????
				log.Errorf("error while looking for new events: %v", err)
			} else {
				for _, ev := range evs {
					if err := p.sem.Acquire(context.Background(), 1); err != nil {
						log.Errorf("cannot acquire semaphore for %s: %v", ev.Name, err)
					} else {
						p.wg.Add(1)
						go func(ev *FileEvent) {
							defer func() {
								p.sem.Release(1)
								p.wg.Done()
							}()
							log.Infof("processing file:%s", ev.Name)
							if err = p.cmd.Execute(ev); err != nil {
								log.Errorf("cannot execute command for %s: %v", ev.Name, err)
							} else {
								if err = p.q.RemoveFileEvent(ev); err != nil {
									log.Errorf("cannot remove event from queue for %s: %v", ev.Name, err)
								}
							}
						}(ev)
					}
				}
			}
			p.mu.Unlock()
		}
	}
}