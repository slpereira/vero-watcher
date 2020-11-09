package vero_watcher

import (
	"github.com/rjeczalik/notify"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

type WatcherDisplay struct {
	Count int
}

func (w *WatcherDisplay) Process(e notify.EventInfo) error {
	w.Count++
	log.Printf("event: %v", e)
	return nil
}

func waitFor(t time.Duration) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(t)
		wg.Done()
	}()
	wg.Wait()
}

func TestWatcher_Start(t *testing.T) {
	type fields struct {
		paths   []string
		events  []notify.Event
		size    int
		command DirWatcherCommand
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{ "test start", fields{
			paths:   []string{"./..."},
			events:  []notify.Event{notify.Create, notify.Write},
			size:    16,
			command: &WatcherDisplay{},
		}, false },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &DirWatcher{
				paths:   tt.fields.paths,
				events:  tt.fields.events,
				size:    tt.fields.size,
				command: tt.fields.command,
			}
			if err := w.Start(); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
			os.Remove("./.notify.txt")
			// generate a file
			_ = ioutil.WriteFile("./.notify.txt", []byte("text"), 0644)
			defer os.Remove("./.notify.txt")
			waitFor(time.Second*2)
			if tt.fields.command.(*WatcherDisplay).Count == 0 {
				t.Error("invalid count")
			}
			if err := w.Stop(); (err != nil) != tt.wantErr {
				t.Errorf("Stop() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWatcher_StartTwice(t *testing.T) {
	type fields struct {
		paths   []string
		events  []notify.Event
		size    int
		command DirWatcherCommand
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &DirWatcher{
				paths:   tt.fields.paths,
				events:  tt.fields.events,
				size:    tt.fields.size,
				command: tt.fields.command,
			}
			_ = w.Start()
			if err := w.Start(); err == nil {
				t.Error("Start does not fail for the second call")
			}
			_ = w.Stop()
			if err := w.Stop(); err == nil {
				t.Errorf("Stop() error = %v", err)
			}
		})
	}
}
