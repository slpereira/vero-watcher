package vero_watcher

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	q := NewQueueBolt("/path onde ele vai salvar o arquivo de controle", time.Second*5)// tempo para ele enviar o evento depois que o evento ocorreu
	// function para filtrar
	var df = func(name string) error {
		if strings.HasSuffix(name, ".txt") {
			return nil
		}
		return ErrSkip
	}
	// extractBase == true ==> /user/silvio/xml
	// nome do arquivo /user/silvio/xml/2020/Junho/01/arq.xml
	// se extractBase 2020/Junho/01/arq.xml
	// base = /user/silvio/xml
	uploadCmd, _ := NewUploadGcs(dest-bucket, false, false, false, "", 1024)
	// paths que vc quer monitorar
	curr, _ := os.Getwd()

	// parallel o nuemro de copias em paralelo
	p := NewProcess([]string{curr}, q, []DirFilter{df}, uploadCmd, 1, false, time.Second)
	if err := p.Start(); err != nil {
		//t.Error(err)
	}
}
