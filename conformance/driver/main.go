// Command driver runs one vexo-sdk-conformance scenario against this SDK: it reads the scenario as JSON
// on stdin and prints JSON lines on stdout. Run it with
//
//	python3 run.py --driver "go run ./conformance/driver" --skip exit_hook
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/vexotech/vexo-go"
)

type scenario struct {
	Host    string `json:"host"`
	APIKey  string `json:"apiKey"`
	Options struct {
		FlushAt           *int `json:"flushAt"`
		FlushIntervalMs   *int `json:"flushIntervalMs"`
		MaxRetries        *int `json:"maxRetries"`
		MaxQueueSize      *int `json:"maxQueueSize"`
		TimeoutMs         *int `json:"timeoutMs"`
		ShutdownTimeoutMs *int `json:"shutdownTimeoutMs"`
	} `json:"options"`
	Steps []step `json:"steps"`
}

type step struct {
	Op         string         `json:"op"`
	DistinctID string         `json:"distinctId"`
	Event      string         `json:"event"`
	Properties map[string]any `json:"properties"`
	Traits     map[string]any `json:"traits"`
	Timestamp  *float64       `json:"timestamp"`
	At         string         `json:"at"`
	MessageID  string         `json:"messageId"`
	Poison     bool           `json:"poison"`
	NaN        bool           `json:"nan"`
	DateTime   string         `json:"dateTime"`
	Decimal    *string        `json:"decimal"`
	Invalid    bool           `json:"invalidUnicode"`
	Ms         int            `json:"ms"`
}

var stdout sync.Mutex

func emit(line map[string]any) {
	stdout.Lock()
	defer stdout.Unlock()
	b, _ := json.Marshal(line)
	os.Stdout.Write(append(b, '\n'))
}

// report prints an error the SDK passed to OnError or returned from a call.
func report(err error) {
	var status any // JSON null
	var e *vexo.Error
	if errors.As(err, &e) && e.Status != 0 {
		status = e.Status
	}
	emit(map[string]any{"type": "error", "message": err.Error(), "status": status})
}

func main() {
	var s scenario
	if err := json.NewDecoder(os.Stdin).Decode(&s); err != nil {
		log.Fatal(err)
	}
	config := vexo.Config{Host: s.Host, OnError: report}
	o := s.Options
	if o.FlushAt != nil {
		config.FlushAt = *o.FlushAt
	}
	if o.MaxRetries != nil {
		config.MaxRetries = *o.MaxRetries
		if *o.MaxRetries == 0 {
			config.MaxRetries = -1 // Go's zero value means the default
		}
	}
	if o.MaxQueueSize != nil {
		config.MaxQueueSize = *o.MaxQueueSize
	}
	config.FlushInterval = millis(o.FlushIntervalMs)
	if o.FlushIntervalMs != nil && *o.FlushIntervalMs == 0 {
		config.FlushInterval = -1 // the timer off: Go's zero value means the default
	}
	config.Timeout = millis(o.TimeoutMs)
	config.ShutdownTimeout = millis(o.ShutdownTimeoutMs)
	client, err := vexo.New(s.APIKey, config)
	if err != nil {
		log.Fatal(err)
	}

	for _, st := range s.Steps {
		started := time.Now()
		var err error
		switch st.Op {
		case "capture":
			err = client.Capture(capture(st))
		case "identify":
			err = client.Identify(vexo.Identify{DistinctID: st.DistinctID, Traits: st.Traits})
		case "flush":
			err = client.Flush(context.Background())
		case "shutdown":
			err = client.Close(context.Background())
		case "sleep":
			time.Sleep(time.Duration(st.Ms) * time.Millisecond)
		case "exit":
			os.Exit(0)
		}
		// Capture and Identify return a problem with the message instead of passing it to OnError.
		if err != nil {
			report(err)
		}
		emit(map[string]any{"type": "step", "op": st.Op, "ms": time.Since(started).Milliseconds()})
	}
}

func capture(st step) vexo.Capture {
	c := vexo.Capture{DistinctID: st.DistinctID, Event: st.Event, Properties: st.Properties, MessageID: st.MessageID}
	if st.Timestamp != nil {
		c.Timestamp = time.UnixMilli(int64(*st.Timestamp))
	}
	if st.At != "" {
		c.Timestamp = parseTime(st.At)
	}
	if c.Properties == nil {
		c.Properties = map[string]any{}
	}
	if st.Poison {
		c.Properties["poison"] = make(chan int) // encoding/json refuses a channel
	}
	if st.NaN {
		c.Properties["nan"] = math.NaN()
	}
	if st.DateTime != "" {
		c.Properties["dateTime"] = parseTime(st.DateTime)
	}
	if st.Decimal != nil {
		c.Properties["decimal"] = *st.Decimal // Go has no decimal type in the standard library
	}
	if st.Invalid {
		c.Properties["invalidUnicode"] = "ok\xff"
	}
	return c
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func millis(ms *int) time.Duration {
	if ms == nil {
		return 0 // the SDK's default
	}
	return time.Duration(*ms) * time.Millisecond
}
