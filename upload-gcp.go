package vero_watcher

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type UploadGcs struct {
	client       *storage.Client
	destBucket   string
	move         bool
	onlyFileName bool
	extractBase  bool
	base         string
	bufferSize   int
	bytesCopied  int64
}

func NewUploadGcs(destBucket string, move bool, onlyFileName bool, extractBase bool,
	base string, bufferSize int) (*UploadGcs, error) {
	cl, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}
	return &UploadGcs{
		client:       cl,
		destBucket:   destBucket,
		move:         move,
		onlyFileName: onlyFileName,
		extractBase:  extractBase,
		base:         base,
		bufferSize:   bufferSize,
	}, nil
}

func warnClose(name string, ref io.Closer) {
	if err := ref.Close(); err != nil {
		log.Warnf("error closing %s: %v", name, err)
	}
}

func (u *UploadGcs) Close() error {
	return u.client.Close()
}

func (u *UploadGcs) Execute(event *FileEvent) error {
	b := u.client.Bucket(u.destBucket)
	dest := event.Name
	source := event.Name
	if u.onlyFileName {
		dest = filepath.Base(dest)
	} else if u.extractBase {
		dest = strings.Replace(event.Name, u.base, "", 1)
	}
	log.Debugf("uploading %s to %s as %s", source, u.destBucket, dest)
	if runtime.GOOS == "windows" {
		dest = strings.ReplaceAll(dest, "\\", "/")
	}
	object := b.Object(dest)
	file, err := os.Open(source)
	if err != nil {
		return err
	}
	stat, err := file.Stat()
	if err != nil {
		warnClose(event.Name, file)
		return err
	}
	if stat.Size() == 0 {
		warnClose(event.Name, file)
		return errors.New(fmt.Sprintf("%s: 0 bytes file", event.Name))
	}
	writer := object.NewWriter(context.Background())
	writer.Metadata["source-path"] = event.Name
	bufferSize := int64(math.Min(float64(stat.Size()), float64(u.bufferSize)))
	buffer := make([]byte, bufferSize)
	n, err := io.CopyBuffer(writer, file, buffer)
	if err != nil {
		warnClose(event.Name, file)
		warnClose(event.Name, writer)
		return err
	}
	if n != stat.Size() {
		warnClose(event.Name, file)
		warnClose(event.Name, writer)
		return errors.New(fmt.Sprintf("invalid size (%d) copied from (%s): expected was %d", u.bytesCopied, source, stat.Size()))
	}
	u.bytesCopied += n
	warnClose(event.Name, file)
	if u.move {
		if err := os.Remove(source); err != nil {
			log.Warnf("file %s was not removed: %v", source, err)
		}
	}
	if err = writer.Close(); err != nil {
		return err
	}
	log.Infof("file %s uploaded to %s as %s", source, u.destBucket, dest)
	return nil
}
