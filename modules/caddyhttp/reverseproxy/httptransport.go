package reverseproxy

import (
	"net"
	"net/http"
	"time"
)

// HTTPTransport wraps http.Transport with configurable timeouts.
type HTTPTransport struct {
	DialTimeout time.Duration
	Transport   *http.Transport
}

// NewHTTPTransport creates an HTTPTransport with given dial timeout.
func NewHTTPTransport(dialTimeout time.Duration) *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}
