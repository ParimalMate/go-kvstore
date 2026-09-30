package api

import (
	"errors"
	"io"
	"log"
	"net/http"

	"kvstore/internal/store"
)

type Handler struct {
	store  *store.Store
	logger *log.Logger
}

func NewHandler(s *store.Store, logger *log.Logger) *Handler {
	return &Handler{
		store:  s,
		logger: logger,
	}
}

func (h *Handler) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("GET key=%q", key)

	value, exists, err := h.store.Get(key)
	if err != nil {
		http.Error(w, "store is unhealthy", http.StatusInternalServerError)
		return
	}

	if !exists {
		http.NotFound(w, r)
		return
	}

	w.Write([]byte(value))
}

func (h *Handler) PutHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	h.logger.Printf("PUT key=%q", key)

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}

		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	value := string(body)

	err = h.store.Put(key, value)
	if err != nil {
		http.Error(w, "failed to store value", http.StatusInternalServerError)
		return
	}

	w.Write([]byte("OK"))
}
