// Package vexo sends server-side events and identifies to Vexo through the capture API
// (https://docs.vexo.co/server-side/capture).
//
// A [Client] queues each message and sends the queue in batches from one goroutine: when FlushAt
// messages are queued, every FlushInterval, and when you call [Client.Flush] or [Client.Close].
// [Client.Capture] and [Client.Identify] never wait for the network.
package vexo

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	version     = "0.1.0"
	defaultHost = "https://api.vexo.co"
	// The API rejects a batch of more than 500 messages or a body over 5 MB.
	maxBatchSize  = 500
	maxBodyBytes  = 5 * 1024 * 1024
	batchEnvelope = `{"batch":[]}`
)

var (
	// ErrClosed is returned by Capture, Identify, and Flush after Close.
	ErrClosed = errors.New("vexo: the client is closed")
	// ErrQueueFull is returned by Capture and Identify when MaxQueueSize messages are waiting; the new
	// message is dropped.
	ErrQueueFull = errors.New("vexo: the queue is full; message dropped")
)

// Config holds the client's options. A zero field selects its default. A negative MaxRetries disables
// retries and a negative FlushInterval turns the interval timer off; any other negative field selects
// its default.
type Config struct {
	// Host is the API base URL. Default: https://api.vexo.co.
	Host string
	// FlushAt sends a batch when this many messages are queued, up to 500. Default: 100.
	FlushAt int
	// FlushInterval sends whatever is queued this often. Default: 10s. A negative value turns the
	// interval timer off, so only FlushAt, Flush, and Close send, like flushInterval 0 in the other
	// Vexo SDKs.
	FlushInterval time.Duration
	// MaxRetries is how many times a batch is retried after a network error or a 429 or 5xx
	// response. Default: 3. A negative value disables retries, like net.Dialer.KeepAlive.
	MaxRetries int
	// MaxQueueSize is how many messages wait in memory; once it's full, new messages are dropped.
	// Default: 10000.
	MaxQueueSize int
	// Timeout bounds each HTTP request. Default: 10s.
	Timeout time.Duration
	// ShutdownTimeout bounds how long Close keeps sending. Default: 10s.
	ShutdownTimeout time.Duration
	// OnError receives send failures and the messages the API dropped, usually as an *Error. It
	// runs on the client's goroutine: return quickly and don't call Flush or Close from it.
	// Default: log.Print.
	OnError func(error)
}

// Capture is an event that a person did.
type Capture struct {
	// DistinctID names the person: the ID you pass to identifyDevice() in the app SDK. Required.
	DistinctID string
	// Event is the event name. Required.
	Event string
	// Properties are encoded with encoding/json, so an encoding.TextMarshaler value is sent as a
	// string. A time.Time in Properties, or in a map[string]any or []any inside them, is sent the
	// way every Vexo SDK sends a date-time: in UTC with milliseconds, "2026-11-07T12:00:00.000Z".
	Properties map[string]any
	// Timestamp is when the event happened. Default: the time you call Capture.
	Timestamp time.Time
	// MessageID lets the API store a resent message once. Default: a new UUIDv4.
	MessageID string
}

// Identify merges traits into a person. A trait set to nil is removed.
type Identify struct {
	// DistinctID names the person. Required.
	DistinctID string
	// Traits are encoded like Capture.Properties.
	Traits map[string]any
}

// The JSON of the messages in a /v1/batch request.
type captureMessage struct {
	Type       string         `json:"type"`
	DistinctID string         `json:"distinctId"`
	Event      string         `json:"event"`
	Properties map[string]any `json:"properties"`
	Timestamp  int64          `json:"timestamp"`
	MessageID  string         `json:"messageId"`
}

type identifyMessage struct {
	Type       string         `json:"type"`
	DistinctID string         `json:"distinctId"`
	Traits     map[string]any `json:"traits"`
	MessageID  string         `json:"messageId"`
}

// Client queues messages and sends them to Vexo in batches. It is safe for concurrent use. Call
// Close when you're done with it: Close sends what is still queued and stops the client's goroutine.
type Client struct {
	apiKey          string
	url             string
	flushAt         int
	flushInterval   time.Duration // 0: no interval timer
	maxRetries      int
	maxQueueSize    int
	shutdownTimeout time.Duration
	onError         func(error)
	http            *http.Client

	mu     sync.Mutex
	queue  []json.RawMessage
	closed bool

	wake    chan struct{}      // FlushAt messages are queued; capacity 1
	flushes chan chan struct{} // Flush requests, each closed once its queue is sent
	closing chan struct{}      // closed by Close
	done    chan struct{}      // closed when the worker returns
	// Cancelled at the shutdown deadline: aborts the request in flight and any wait between retries.
	ctx    context.Context
	cancel context.CancelFunc

	// Set when a batch uses up its retries: later batches get one attempt until a request succeeds.
	// Only the worker goroutine reads and writes it.
	circuitOpen bool
}

// New returns a client that sends to the app with this API key, and starts its goroutine.
func New(apiKey string, config Config) (*Client, error) {
	if apiKey == "" {
		return nil, errors.New("vexo: an API key is required")
	}
	host := defaultHost
	if config.Host != "" {
		host = strings.TrimRight(config.Host, "/")
	}
	if u, err := url.Parse(host); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("vexo: Host must be an http or https URL, got %q", config.Host)
	}
	maxRetries := orDefault(config.MaxRetries, 3)
	if config.MaxRetries < 0 {
		maxRetries = 0
	}
	flushInterval := orDefault(config.FlushInterval, 10*time.Second)
	if config.FlushInterval < 0 {
		flushInterval = 0 // no interval timer
	}
	onError := config.OnError
	if onError == nil {
		onError = func(err error) { log.Print(err) }
	}
	c := &Client{
		apiKey:          apiKey,
		url:             host + "/v1/batch",
		flushAt:         min(orDefault(config.FlushAt, 100), maxBatchSize),
		flushInterval:   flushInterval,
		maxRetries:      maxRetries,
		maxQueueSize:    orDefault(config.MaxQueueSize, 10000),
		shutdownTimeout: orDefault(config.ShutdownTimeout, 10*time.Second),
		onError:         onError,
		http:            &http.Client{Timeout: orDefault(config.Timeout, 10*time.Second)},
		wake:            make(chan struct{}, 1),
		flushes:         make(chan chan struct{}),
		closing:         make(chan struct{}),
		done:            make(chan struct{}),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	go c.run()
	return c, nil
}

// Capture queues an event. It returns an error, and drops the message, when the message is
// invalid, can't be encoded as JSON, is over 5 MB, or the queue is full or closed. It never waits
// for the network.
func (c *Client) Capture(m Capture) error {
	if m.DistinctID == "" || m.Event == "" {
		return errors.New("vexo: capture needs a DistinctID and an Event")
	}
	timestamp := m.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	id, err := messageID(m.MessageID)
	if err != nil {
		return err
	}
	return c.enqueue(captureMessage{
		Type:       "capture",
		DistinctID: m.DistinctID,
		Event:      m.Event,
		Properties: jsonObject(m.Properties, 0),
		Timestamp:  timestamp.UnixMilli(),
		MessageID:  id,
	})
}

// Identify queues an identify. It returns an error, and drops the message, for the same reasons
// as Capture. It never waits for the network.
func (c *Client) Identify(m Identify) error {
	if m.DistinctID == "" {
		return errors.New("vexo: identify needs a DistinctID")
	}
	id, err := messageID("")
	if err != nil {
		return err
	}
	return c.enqueue(identifyMessage{Type: "identify", DistinctID: m.DistinctID, Traits: jsonObject(m.Traits, 0), MessageID: id})
}

// Flush sends everything queued and returns when the queue is empty, or with ctx's error when ctx
// is done first; the client then keeps sending in the background. Send failures go to OnError.
func (c *Client) Flush(ctx context.Context) error {
	sent := make(chan struct{})
	select {
	case c.flushes <- sent:
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-sent:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close sends everything queued, stops the client's goroutine, and returns. It gives up at
// ShutdownTimeout or when ctx is done, whichever comes first: it aborts the request in flight,
// reports what is still queued to OnError, and returns the context error. Later calls to Capture,
// Identify, and Flush return ErrClosed.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	first := !c.closed
	c.closed = true
	c.mu.Unlock()
	if !first {
		select {
		case <-c.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithTimeout(ctx, c.shutdownTimeout)
	defer cancel()
	close(c.closing)
	select {
	case <-c.done:
	case <-ctx.Done():
		c.cancel()
		<-c.done
	}
	c.cancel()
	return ctx.Err()
}

func (c *Client) enqueue(m any) error {
	// Encoded now, so one bad value drops only its own message and later changes to the maps are not sent.
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("vexo: message dropped, it can't be sent as JSON: %w", err)
	}
	if len(raw)+len(batchEnvelope) > maxBodyBytes {
		return errors.New("vexo: message dropped, it is larger than 5 MB")
	}

	c.mu.Lock()
	switch {
	case c.closed:
		c.mu.Unlock()
		return ErrClosed
	case len(c.queue) >= c.maxQueueSize:
		c.mu.Unlock()
		return ErrQueueFull
	}
	c.queue = append(c.queue, raw)
	ready := len(c.queue) >= c.flushAt
	c.mu.Unlock()

	if ready {
		select {
		case c.wake <- struct{}{}:
		default: // already woken
		}
	}
	return nil
}

// run is the client's one goroutine: it does all the sending.
func (c *Client) run() {
	defer close(c.done)
	var tick <-chan time.Time // nil, so never ready, when the interval timer is off
	if c.flushInterval > 0 {
		ticker := time.NewTicker(c.flushInterval)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case <-tick:
			c.drain()
		case <-c.wake:
			c.drain()
		case sent := <-c.flushes:
			c.drain()
			close(sent)
		case <-c.closing:
			c.drain()
			return
		}
	}
}

// messageID returns the caller's message ID, or a random (version 4) UUID when there is none.
func messageID(given string) (string, error) {
	if given != "" {
		return given, nil
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("vexo: message dropped, no message ID: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// The form every Vexo SDK sends a date-time in (SPEC.md M11), applied to a time in UTC.
const timeLayout = "2006-01-02T15:04:05.000Z"

// maxDepth bounds the copy that jsonObject makes, so a map that contains itself reaches
// encoding/json, which reports the cycle, instead of recursing forever.
const maxDepth = 1000

// jsonObject returns a copy of properties or traits to encode: an empty map for nil, so the API gets
// {} rather than null, and each time.Time inside it in the form of timeLayout.
func jsonObject(m map[string]any, depth int) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = jsonValue(v, depth+1)
	}
	return out
}

func jsonValue(v any, depth int) any {
	if depth > maxDepth {
		return v
	}
	switch v := v.(type) {
	case time.Time:
		return v.UTC().Format(timeLayout)
	case *time.Time:
		if v != nil {
			return v.UTC().Format(timeLayout)
		}
	case map[string]any:
		if v != nil {
			return jsonObject(v, depth)
		}
	case []any:
		if v != nil {
			out := make([]any, len(v))
			for i, item := range v {
				out[i] = jsonValue(item, depth+1)
			}
			return out
		}
	}
	return v
}

func orDefault[T int | time.Duration](value, def T) T {
	if value <= 0 {
		return def
	}
	return value
}
