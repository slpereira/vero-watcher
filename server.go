package vero_watcher

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type HttpServer struct {
	q Queue
}

type AppError struct {
	Message string
}

func NewHttpServer(queue Queue, port string) *HttpServer {
	return &HttpServer{q: queue}
}

func (h *HttpServer) HandlerQueueView(w http.ResponseWriter, req *http.Request) {
	jsonEncoded := json.NewEncoder(w)
	s, err := h.q.GetStatistics()
	if err != nil {
		log.Errorf("Processed request with error [%s]", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		jsonEncoded.Encode(AppError{Message: err.Error()})
		return
	}
	w.Header().Add("Content-Type", "application/json")
	log.Info("Processed request (OK)")
	w.WriteHeader(http.StatusOK)
	if err := jsonEncoded.Encode(s); err != nil {
		log.Error(err)
	}
}

