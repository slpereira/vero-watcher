package vero_watcher

import (
	"github.com/rjeczalik/notify"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type DisplayCommand struct {
	Count int
	Event notify.Event
}

func (d *DisplayCommand) Execute(event *FileEvent) error {
//	if event.Event&d.Event == d.Event {
		d.Count++
		log.Printf("test event: %v", event)
//	}
	return nil
}

func TestNewProcessNoFilter(t *testing.T) {
	//log.SetLevel(log.DebugLevel)
	t.Run("process", func(t *testing.T) {
		curr, _ := os.Getwd()
		fdb := filepath.Join(curr, dbPath)
		os.Remove(fdb)
		defer os.Remove(fdb)
		c := &DisplayCommand{Event: notify.Write}
		q := NewQueueBolt(dbPath, time.Second*10)
		file := filepath.Join(curr, "test.txt")
		os.Remove(file)
		defer os.Remove(file)
		p := NewProcess([]string{curr}, q, nil, c, 1, false, time.Second)
		if err := p.Start(); err != nil {
			t.Error(err)
		}
		ioutil.WriteFile(file, []byte("test"), 0600)
		waitFor(time.Second*30)
		if c.Count != 2 {
			t.Error("failed")
		}
		if err := p.Stop(); err != nil {
			t.Error(err)
		}
	})
}

func TestNewProcessFilter(t *testing.T) {
	t.Run("process", func(t *testing.T) {
		os.Remove(dbPath)
		defer os.Remove(dbPath)
		c := &DisplayCommand{}
		q := NewQueueBolt(dbPath, time.Second*5)
		curr, _ := os.Getwd()
		file := filepath.Join(curr, "test.txt")
		var df = func(name string) error {
			if strings.HasSuffix(name, ".txt") {
				return nil
			}
			return ErrSkip
		}
		p := NewProcess([]string{curr}, q, []DirFilter{df}, c, 1, false, time.Second)
		if err := p.Start(); err != nil {
			t.Error(err)
		}
		ioutil.WriteFile(file, []byte("test"), 0600)
		defer os.Remove(file)
		waitFor(time.Second*10)
		if c.Count != 1 {
			t.Error("failed")
		}
		if err := p.Stop(); err != nil {
			t.Error(err)
		}
	})
}

