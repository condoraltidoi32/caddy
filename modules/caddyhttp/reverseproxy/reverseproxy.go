package reverseproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// dialProtectedBody wraps an io.ReadCloser to prevent net/http.Transport from
// closing the underlying body if a dial fails before any payload bytes are read.
type dialProtectedBody struct {
	orig        io.ReadCloser
	mu          sync.Mutex
	readStarted bool
	closed      bool
}

func newDialProtectedBody(orig io.ReadCloser) *dialProtectedBody {
	if orig == nil {
		return nil
 junta
	}
	return &dialProtectedBody{orig: orig}
}

func (b *dialProtectedBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	b.readStarted = true
	b.mu.Unlock()
	return b.orig.Read(p)
}

func (b *dialProtectedBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	// If the transport closes the body before any read happened (e.g. dial failure),
	// avoid closing the underlying body so downstream error handlers can read it.
	if !b.readStarted {
		return nil
	}
	b.closed = true
	return b.orig.Close()
}

func (b *dialProtectedBody) HasReadStarted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.readStarted
}

// Handler represents a reverse proxy handler.
type Handler struct {
	Transport      http.RoundTripper
	BufferRequests bool
	UpstreamAddr   string
	DialTimeout    time.Duration
}

// ServeHTTP implements the reverse proxy execution logic.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request) error) error {
	origBody := r.Body
	var bufferedBody []byte

	if h.BufferRequests && r.Body != nil && r.Body != http.NoBody {
		var err error
		bufferedBody, err = io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return err
		}
		r.Body = io.NopCloser(bytes.NewReader(bufferedBody))
		origBody = r.Body
	}

	outReq := r.Clone(r.Context())
	var protected *dialProtectedBody

	if bufferedBody != nil {
		outReq.Body = io.NopCloser(bytes.NewReader(bufferedBody))
	} else if r.Body != nil && r.Body != http.NoBody {
		protected = newDialProtectedBody(origBody)
		outReq.Body = protected
	}

	transport := h.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	resp, err := transport.RoundTrip(outReq)
	if err != nil {
		// Restore request body for downstream error handlers
		if bufferedBody != nil {
			r.Body = io.NopCloser(bytes.NewReader(bufferedBody))
		} else if protected != nil && !protected.HasReadStarted() {
			r.Body = origBody
		}
		return err
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	return err
}

// ErrorHandler handles errors by reading request body if present.
type ErrorHandler func(w http.ResponseWriter, r *http.Request, err error)
