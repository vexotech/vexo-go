package vexo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

const (
	maxBackoff    = 30 * time.Second
	maxRetryAfter = 60 * time.Second
)

// Error reports messages the client could not deliver. OnError receives it.
type Error struct {
	// Status is the HTTP status of a rejected batch, or 0.
	Status int
	// Batch holds the JSON of the messages that weren't sent: a batch that failed or was
	// rejected, or what was still queued at the shutdown deadline.
	Batch []json.RawMessage
	// Dropped lists the messages the API dropped, or still reported as failed after the last
	// retry, each with the API's reason.
	Dropped []DroppedMessage
	// Err is the underlying error, such as a network error, or nil.
	Err error

	msg string
}

func (e *Error) Error() string { return e.msg }

func (e *Error) Unwrap() error { return e.Err }

// DroppedMessage is a message the API didn't store.
type DroppedMessage struct {
	// Message is the message's JSON.
	Message json.RawMessage
	// Reason is the API's reason, such as "event is required".
	Reason string
}

// One entry of the dropped or failed list of an API response; Index is the message's position in the request.
type messageResult struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
}

// drain sends everything queued, a batch at a time. Once the shutdown deadline has passed, it
// reports what is left instead.
func (c *Client) drain() {
	for {
		if c.ctx.Err() != nil {
			c.dropQueue()
			return
		}
		batch := c.nextBatch()
		if len(batch) == 0 {
			return
		}
		if err := c.send(batch); err != nil {
			c.onError(err)
		}
	}
}

// nextBatch takes up to FlushAt messages that fit in one request body.
func (c *Client) nextBatch() []json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	size, n := len(batchEnvelope), 0
	// n is also the number of commas between the messages taken so far.
	for n < len(c.queue) && n < c.flushAt && size+len(c.queue[n])+n <= maxBodyBytes {
		size += len(c.queue[n])
		n++
	}
	batch := c.queue[:n:n]
	c.queue = c.queue[n:]
	return batch
}

func (c *Client) dropQueue() {
	c.mu.Lock()
	dropped := c.queue
	c.queue = nil
	c.mu.Unlock()
	if len(dropped) > 0 {
		c.onError(&Error{msg: fmt.Sprintf("vexo: shutdown deadline reached; %d message(s) dropped", len(dropped)), Batch: dropped})
	}
}

// send posts one batch to /v1/batch. Network errors, 429, and 5xx are retried with exponential
// backoff (or the API's Retry-After); the messages the API reports as failed are sent again the
// same way. Dropped messages are reported, not retried.
func (c *Client) send(batch []json.RawMessage) *Error {
	maxRetries := c.maxRetries
	if c.circuitOpen {
		maxRetries = 0
	}
	for attempt := 0; ; attempt++ {
		resp, body, err := c.post(batch)
		last := attempt >= maxRetries || c.ctx.Err() != nil
		wait := backoff(attempt)
		switch {
		case err != nil:
			if last {
				c.circuitOpen = true
				return &Error{msg: "vexo: batch not sent: " + err.Error(), Batch: batch, Err: err}
			}
		case resp.StatusCode/100 == 2:
			c.circuitOpen = false
			var result struct {
				Data struct {
					Dropped []messageResult `json:"dropped"`
					Failed  []messageResult `json:"failed"`
				} `json:"data"`
			}
			// A 2xx body that isn't the API's envelope counts as nothing dropped or failed.
			_ = json.Unmarshal(body, &result)
			c.report(batch, result.Data.Dropped, "the server dropped")
			failed := pick(batch, result.Data.Failed)
			if len(failed) == 0 {
				return nil
			}
			if last {
				c.report(batch, result.Data.Failed, "the server could not store")
				return nil
			}
			batch = failed
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			if last {
				c.circuitOpen = true
				return rejected(resp.StatusCode, body, batch)
			}
			if d, ok := retryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
				wait = d
			}
		default:
			return rejected(resp.StatusCode, body, batch)
		}
		c.sleep(wait)
	}
}

// post sends one request. The response body is read and closed.
func (c *Client) post(batch []json.RawMessage) (*http.Response, []byte, error) {
	var payload bytes.Buffer
	payload.WriteString(`{"batch":[`)
	for i, m := range batch {
		if i > 0 {
			payload.WriteByte(',')
		}
		payload.Write(m)
	}
	payload.WriteString("]}")

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.url, &payload)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", c.apiKey)
	req.Header.Set("User-Agent", "vexo-go/"+version)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	return resp, body, nil
}

// report sends the messages listed in an API response's dropped or failed list to OnError.
func (c *Client) report(batch []json.RawMessage, results []messageResult, what string) {
	if len(results) == 0 {
		return
	}
	err := &Error{msg: fmt.Sprintf("vexo: %s %d message(s): %s", what, len(results), results[0].Reason)}
	for _, r := range results {
		var m json.RawMessage
		if r.Index >= 0 && r.Index < len(batch) {
			m = batch[r.Index]
		}
		err.Dropped = append(err.Dropped, DroppedMessage{Message: m, Reason: r.Reason})
	}
	c.onError(err)
}

// sleep waits, but never past the shutdown deadline.
func (c *Client) sleep(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-c.ctx.Done():
	}
}

func rejected(status int, body []byte, batch []json.RawMessage) *Error {
	return &Error{Status: status, Batch: batch, msg: fmt.Sprintf("vexo: batch rejected with HTTP %d: %s", status, body)}
}

// pick returns the messages of batch at the indexes in results.
func pick(batch []json.RawMessage, results []messageResult) []json.RawMessage {
	var picked []json.RawMessage
	for _, r := range results {
		if r.Index >= 0 && r.Index < len(batch) {
			picked = append(picked, batch[r.Index])
		}
	}
	return picked
}

// backoff is exponential with jitter: half of 1s, 2s, 4s... (capped at 30s) plus a random part of the other half.
func backoff(attempt int) time.Duration {
	base := maxBackoff
	if attempt < 5 {
		base = time.Second << attempt
	}
	return base/2 + time.Duration(rand.Int63n(int64(base/2)+1))
}

// retryAfter reads a Retry-After header, in seconds or as an HTTP date, capped at 60 seconds.
func retryAfter(header string, now time.Time) (time.Duration, bool) {
	if seconds, err := strconv.ParseFloat(header, 64); err == nil {
		if seconds >= 0 { // false for NaN
			return time.Duration(math.Min(seconds, maxRetryAfter.Seconds()) * float64(time.Second)), true
		}
		return 0, false
	}
	if at, err := http.ParseTime(header); err == nil {
		return min(max(at.Sub(now), 0), maxRetryAfter), true
	}
	return 0, false
}
