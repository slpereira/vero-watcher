package vero_watcher

import (
	"bytes"
	"encoding/gob"
	"github.com/rjeczalik/notify"
	log "github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// QueueBolt is a queue implementation backed by bolt-db
type QueueBolt struct {
	path             string
	db               *bolt.DB
	dirCache         map[string]time.Time
	dispatchInterval time.Duration
	mu               sync.Mutex
}

func (q *QueueBolt) GetStatistics() (*QueueStatistics, error) {
	var queuedEvents []*FileEvent
	var processingEvents []*FileEvent
	var dirEvents []*DirEvent
	if err := q.db.View(func(tx *bolt.Tx) (err error) {
		queuedEvents, err = q.viewFileEvents(tx, FileEventBucket)
		if err != nil {
			return
		}
		processingEvents, err = q.viewFileEvents(tx, ProcessingEventBucket)
		if err != nil {
			return
		}
		b := tx.Bucket([]byte(DirEventBucket))
		if b != nil {
			if err = b.ForEach(func(k, v []byte) error {
				d := gob.NewDecoder(bytes.NewReader(v))
				var dev DirEvent
				if err = d.Decode(&dev); err != nil {
					return err
				}
				dirEvents = append(dirEvents, &dev)
				return nil
			}); err != nil {
				return
			}
		}
		return
	}); err != nil {
		return nil,  err
	}
	return &QueueStatistics{
		QueuedEvents:     queuedEvents,
		ProcessingEvents: processingEvents,
		QueuePath:        q.path,
		DirEvents:        dirEvents,
	}, nil
}

func (q *QueueBolt) viewFileEvents(tx *bolt.Tx, bucketName string) ([]*FileEvent, error) {
	var r []*FileEvent
	b := tx.Bucket([]byte(bucketName))
	if b != nil {
		if err := b.ForEach(func(k, v []byte) error {
			fe, err := q.extractFileEvent(v)
			if err != nil {
				return err
			}
			r = append(r, fe)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func NewQueueBolt(path string, dispatchInterval time.Duration) Queue {
	return &QueueBolt{
		path:             path,
		dirCache:         make(map[string]time.Time),
		dispatchInterval: dispatchInterval,
	}
}

func (q *QueueBolt) Open() error {
	db, err := bolt.Open(q.path, 0600, nil)
	if err != nil {
		return err
	}
	q.db = db
	return nil
}

func (q *QueueBolt) Close() error {
	return q.db.Close()
}

func (q *QueueBolt) getFileEvent(b *bolt.Bucket, name string) (*FileEvent, error) {
	v := b.Get([]byte(name))
	if v == nil {
		return nil, nil
	}
	return q.extractFileEvent(v)
}

func (q *QueueBolt) addFileEventInternally(tx *bolt.Tx, value *FileEvent) error {
	log.Debugf("adding event %v", value)
	b, err := tx.CreateBucketIfNotExists([]byte(FileEventBucket))
	if err != nil {
		return err
	}
	f, err := q.getFileEvent(b, value.Name)
	if err != nil {
		return err
	}
	if f != nil {
		value.Event |= f.Event
	}
	// check if there is some event already added
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(&value); err != nil {
		return err
	}
	return b.Put([]byte(value.Name), buffer.Bytes())
}

func (q *QueueBolt) addDirEventInternally(tx *bolt.Tx, value *DirEvent) error {
	log.Debugf("adding event %v", value)
	b, err := tx.CreateBucketIfNotExists([]byte(DirEventBucket))
	if err != nil {
		return err
	}
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(&value); err != nil {
		return err
	}
	return b.Put([]byte(value.Name), buffer.Bytes())
}

func (q *QueueBolt) AddFileEvent(value *FileEvent) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		if err := q.addFileEventInternally(tx, value); err != nil {
			return err
		}
		dir := filepath.Dir(value.Name)
		t, ok := q.dirCache[dir]
		if !ok || value.ModTime.After(t) {
			q.dirCache[value.Name] = value.ModTime
			return q.addDirEventInternally(tx, &DirEvent{
				Name:    dir,
				ModTime: value.ModTime,
			})
		}
		return nil
	})
}

func (q *QueueBolt) GetFileEvents() ([]*FileEvent, error) {
	var r []*FileEvent
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.db.Update(func(tx *bolt.Tx) error {
		destBucket, err := tx.CreateBucketIfNotExists([]byte(ProcessingEventBucket))
		if err != nil {
			return err
		}
		b := tx.Bucket([]byte(FileEventBucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for key, value := c.First(); key != nil; key, value = c.Next() {
			fev, err := q.extractFileEvent(value)
			if err != nil {
				return err
			}
			if time.Since(fev.ModTime) >= q.dispatchInterval {
				// move
				if err := destBucket.Put(key, value); err != nil {
					return err
				}
				if err := c.Delete(); err != nil {
					return err
				}
				r = append(r, fev)
			}
		}
		return nil
	})
	return r, err
}

func (q *QueueBolt) extractFileEvent(value []byte) (*FileEvent, error) {
	d := gob.NewDecoder(bytes.NewReader(value))
	var fev FileEvent
	if err := d.Decode(&fev); err != nil {
		return nil, err
	}
	return &fev, nil
}

func (q *QueueBolt) RemoveFileEvent(value *FileEvent) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ProcessingEventBucket))
		return b.Delete([]byte(value.Name))
	})
}

// Restore look for inconsistent keys in the queue
// Must be called before any processing
func (q *QueueBolt) Restore(filters []DirFilter) error {
	log.Info("restore...")
	if err := q.processLostFiles(filters); err != nil {
		return err
	}
	if err := q.restoreProcessingBucket(); err != nil {
		return err
	}
	return nil
}

// restoreProcessingBucket move the keys in the processing bucket back to the queue
func (q *QueueBolt) restoreProcessingBucket() error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ProcessingEventBucket))
		if b == nil {
			log.Infof("processing event bucket is not available")
			return nil
		}
		dest := tx.Bucket([]byte(FileEventBucket))
		c := b.Cursor()
		for key, value := c.First(); key != nil; key, value = c.Next() {
			log.Infof("restoring file to main queue: %s", string(key))
			dest.Put(key, value)
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

// processLostFiles look if exists files in directory generated after the last event in the dir bucket
func (q *QueueBolt) processLostFiles(filters []DirFilter) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(DirEventBucket))
		if b == nil {
			log.Infof("dir event bucket is not available")
			return nil
		}
		c := b.Cursor()
		for key, value := c.First(); key != nil; key, value = c.Next() {
			d := gob.NewDecoder(bytes.NewReader(value))
			var dev DirEvent
			if err := d.Decode(&dev); err != nil {
				return err
			}
			log.Infof("looking for changes in %s after %v", dev.Name, dev.ModTime)
			files, err := ioutil.ReadDir(dev.Name)
			if err != nil {
				if os.IsNotExist(err) {
					log.Infof("directory does not exists anymore: %s", dev.Name)
					if err = c.Delete(); err != nil {
						return err
					}
					continue
				}
				return err
			}
			lastModTime := dev.ModTime
			found := false
			for _, f := range files {
				// we do not follow sub dir from here
				if f.IsDir() {
					continue
				}
				name := filepath.Join(dev.Name, f.Name())
				skip := false
				// apply filters
				for _, filter := range filters {
					if err := filter(name); err != nil {
						if err == ErrSkip {
							skip = true
							break
						} else {
							return err
						}
					}
				}
				if skip {
					log.Debugf("process lost file skipped: %s", name)
					continue
				}
				if f.ModTime().After(dev.ModTime) {
					log.Infof("found file modified since las shutdown: %s", name)
					if err := q.addFileEventInternally(tx, &FileEvent{
						Name:    name,
						ModTime: f.ModTime(),
						Event:   notify.Write,
					}); err != nil {
						return err
					}
					if f.ModTime().After(lastModTime) {
						lastModTime = f.ModTime()
					}
					found = true
				}
			}
			// we create the cache here
			q.dirCache[dev.Name] = lastModTime
			if found {
				dev.ModTime = lastModTime
				if err := q.addDirEventInternally(tx, &dev); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
