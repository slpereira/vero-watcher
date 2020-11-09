package vero_watcher

import (
	"github.com/rjeczalik/notify"
	bolt "go.etcd.io/bbolt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const dbPath = "./b.db"

func TestQueueBolt(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"bolt-test", dbPath},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Remove(dbPath)
			defer os.Remove(dbPath)
			q := NewQueueBolt(tt.path, time.Nanosecond)
			if err := q.Open(); err != nil {
				t.Errorf("Open() error = %v", err)
			}
			if err := q.AddFileEvent(&FileEvent{
				Name:    "test",
				ModTime: time.Now(),
				Event:   notify.Create,
			}); err != nil {
				t.Errorf("AddFileEvent() error = %v", err)
			}
			evs, err := q.GetFileEvents()
			if err != nil {
				t.Error(err)
			}
			if len(evs) != 1 {
				t.Error("event not added")
			}
			if err := q.RemoveFileEvent(evs[0]); err != nil {
				t.Error(err)
			}
			if err := q.Close(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestQueueBolt_Restore(t *testing.T) {
	t.Run("restore", func(t *testing.T) {
		os.Remove(dbPath)
		defer os.Remove(dbPath)
		q := NewQueueBolt(dbPath, time.Second*5)
		if err := q.Open(); err != nil {
			t.Errorf("Open() error = %v", err)
		}
		if err := q.AddFileEvent(&FileEvent{
			Name:    "test",
			ModTime: time.Now(),
			Event:   notify.Create,
		}); err != nil {
			t.Errorf("AddFileEvent() error = %v", err)
		}

	})
}

func TestQueueBolt_restoreProcessingBucket(t *testing.T) {
	t.Run("restoreProcessingBucket", func(t *testing.T) {
		os.Remove(dbPath)
		defer os.Remove(dbPath)
		q := NewQueueBolt(dbPath, time.Nanosecond).(*QueueBolt)
		if err := q.Open(); err != nil {
			t.Error(err)
		}
		q.db.Update(func(tx *bolt.Tx) error {
			return q.addFileEventInternally(tx, &FileEvent{
				Name:    "test",
				ModTime: time.Now(),
				Event:   notify.Create,
			})
		})
		// move to processing bucket
		fevs, err := q.GetFileEvents()
		if err != nil {
			t.Error(err)
		}
		q.Close()
		q.Open()
		// restore
		if err := q.restoreProcessingBucket(); err != nil {
			t.Errorf("restoreProcessingBucket() error = %v", err)
		}
		fevs2, err := q.GetFileEvents()
		if !reflect.DeepEqual(fevs, fevs2) {
			t.Error("restore failed")
		}
		q.Close()
	})
}

func TestQueueBolt_processLostFiles(t *testing.T) {
	t.Run("restoreProcessingBucket", func(t *testing.T) {
		os.Remove(dbPath)
		defer os.Remove(dbPath)
		q := NewQueueBolt(dbPath, time.Nanosecond).(*QueueBolt)
		if err := q.Open(); err != nil {
			t.Error(err)
		}
		cwd, _ := os.Getwd()
		f := &FileEvent{
			Name:    filepath.Join(cwd, "test"),
			ModTime: time.Now(),
			Event:   notify.Create,
		}
		q.AddFileEvent(f)
		// move to processing bucket
		_, err := q.GetFileEvents()
		if err != nil {
			t.Error(err)
		}
		q.RemoveFileEvent(f)
		// restore
		q.Close()
		// generate a file
		nfn := filepath.Join(cwd, "ftest.txt")
		os.Remove(nfn)
		ioutil.WriteFile(nfn, []byte("test"), 0600)
		q.Open()
		df := func(name string) error {
			if strings.HasSuffix(name,  ".txt") {
				return nil
			}
			return ErrSkip
		}
		if err := q.processLostFiles([]DirFilter{df}); err != nil {
			t.Errorf("restoreProcessingBucket() error = %v", err)
		}
		fevs2, err := q.GetFileEvents()
		found := false
		for _,fe := range fevs2 {
			if fe.Name == nfn {
				found = true
			}
		}
		if !found {
			t.Error("process lost files failed")
		}
		q.Close()
	})
}