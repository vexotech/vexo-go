package vexo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// A local API that records each batch request and answers with the next scripted response.
type fakeAPI struct {
	*httptest.Server
	mu        sync.Mutex
	requests  []apiRequest
	responses []apiResponse
	held      chan struct{}
}

type apiRequest struct {
	path   string
	header http.Header
	size   int
	batch  []map[string]any
	at     time.Time
}

type apiResponse struct {
	status int
	header map[string]string
	body   string
	hangUp bool // close the connection without a response
}

func newFakeAPI(t *testing.T, responses ...apiResponse) *fakeAPI {
	api := &fakeAPI{responses: responses}
	api.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var parsed struct{ Batch []map[string]any }
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		if err := decoder.Decode(&parsed); err != nil {
			t.Errorf("request body isn't JSON: %v", err)
		}
		api.mu.Lock()
		api.requests = append(api.requests, apiRequest{r.URL.Path, r.Header, len(body), parsed.Batch, time.Now()})
		next := apiResponse{status: http.StatusOK}
		if len(api.responses) > 0 {
			next, api.responses = api.responses[0], api.responses[1:]
		}
		held := api.held
		api.mu.Unlock()

		if held != nil {
			<-held
		}
		if next.hangUp {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		if next.body == "" && next.status == http.StatusOK {
			next.body = fmt.Sprintf(`{"success":true,"code":200,"data":{"accepted":%d,"dropped":[],"failed":[]}}`, len(parsed.Batch))
		}
		for name, value := range next.header {
			w.Header().Set(name, value)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(next.status)
		io.WriteString(w, next.body)
	}))
	t.Cleanup(api.Close)
	return api
}

// hold makes the API wait before it answers, until release is called.
func (a *fakeAPI) hold(t *testing.T) (release func()) {
	a.mu.Lock()
	a.held = make(chan struct{})
	held := a.held
	a.mu.Unlock()
	var once sync.Once
	release = func() { once.Do(func() { close(held) }) }
	t.Cleanup(release)
	return release
}

func (a *fakeAPI) snapshot() []apiRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]apiRequest(nil), a.requests...)
}

func (a *fakeAPI) events() []string {
	var events []string
	for _, r := range a.snapshot() {
		for _, m := range r.batch {
			events = append(events, fmt.Sprint(m["event"]))
		}
	}
	return events
}

type recorder struct {
	mu   sync.Mutex
	errs []error
}

func (r *recorder) add(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *recorder) all() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.errs...)
}

// vexoErrors returns the recorded errors as *Error, failing the test on any other kind.
func (r *recorder) vexoErrors(t *testing.T) []*Error {
	t.Helper()
	var out []*Error
	for _, err := range r.all() {
		var e *Error
		if !errors.As(err, &e) {
			t.Fatalf("OnError got %T, want *Error: %v", err, err)
		}
		out = append(out, e)
	}
	return out
}

func newClient(t *testing.T, host string, config Config) (*Client, *recorder) {
	t.Helper()
	errs := &recorder{}
	config.Host = host
	if config.FlushInterval == 0 {
		config.FlushInterval = time.Minute
	}
	config.OnError = errs.add
	c, err := New("test-key", config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c, errs
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewAppliesDefaultsAndLimits(t *testing.T) {
	c, err := New("key", Config{})
	must(t, err)
	defer c.Close(context.Background())
	got := []any{c.url, c.flushAt, c.flushInterval, c.maxRetries, c.maxQueueSize, c.http.Timeout, c.shutdownTimeout}
	want := []any{"https://api.vexo.co/v1/batch", 100, 10 * time.Second, 3, 10000, 10 * time.Second, 10 * time.Second}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("defaults = %v, want %v", got, want)
	}

	c2, err := New("key", Config{Host: "http://localhost:8080/", FlushAt: 1000, MaxRetries: -1, FlushInterval: -1})
	must(t, err)
	defer c2.Close(context.Background())
	if c2.url != "http://localhost:8080/v1/batch" || c2.flushAt != maxBatchSize || c2.maxRetries != 0 || c2.flushInterval != 0 {
		t.Errorf("url, flushAt, maxRetries, flushInterval = %q, %d, %d, %v", c2.url, c2.flushAt, c2.maxRetries, c2.flushInterval)
	}

	if _, err := New("", Config{}); err == nil {
		t.Error("New accepted an empty API key")
	}
	if _, err := New("key", Config{Host: "api.vexo.co"}); err == nil {
		t.Error("New accepted a Host without a scheme")
	}
}

func TestSendsQueuedMessagesAsOneBatchWithTheAPIKey(t *testing.T) {
	api := newFakeAPI(t)
	c, errs := newClient(t, api.URL, Config{})
	at := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

	must(t, c.Identify(Identify{DistinctID: "user_1", Traits: map[string]any{"plan": "pro"}}))
	must(t, c.Capture(Capture{DistinctID: "user_1", Event: "purchase", Properties: map[string]any{"amount": 42, "at": at}, Timestamp: at}))
	must(t, c.Capture(Capture{DistinctID: "user_1", Event: "refund", MessageID: "my-id"}))
	must(t, c.Identify(Identify{DistinctID: "user_2"}))
	must(t, c.Close(context.Background()))

	requests := api.snapshot()
	if len(requests) != 1 {
		t.Fatalf("got %d requests, want 1", len(requests))
	}
	r := requests[0]
	if r.path != "/v1/batch" || r.header.Get("X-Api-Key") != "test-key" || r.header.Get("Content-Type") != "application/json" ||
		r.header.Get("User-Agent") != "vexo-go/"+version {
		t.Errorf("path %q, headers %v", r.path, r.header)
	}
	identify, capture, refund := r.batch[0], r.batch[1], r.batch[2]
	if fmt.Sprintf("%v %v %v", identify["type"], identify["distinctId"], identify["traits"]) != "identify user_1 map[plan:pro]" {
		t.Errorf("identify = %v", identify)
	}
	if fmt.Sprintf("%v %v %v", capture["type"], capture["event"], capture["properties"]) != "capture purchase map[amount:42 at:2026-11-07T12:00:00Z]" {
		t.Errorf("capture = %v", capture)
	}
	if capture["timestamp"] != json.Number("1794052800000") {
		t.Errorf("timestamp = %v, want integer epoch milliseconds 1794052800000", capture["timestamp"])
	}
	if !uuidV4.MatchString(identify["messageId"].(string)) || !uuidV4.MatchString(capture["messageId"].(string)) ||
		identify["messageId"] == capture["messageId"] {
		t.Errorf("messageIds %v and %v aren't two UUIDv4s", identify["messageId"], capture["messageId"])
	}
	if refund["messageId"] != "my-id" {
		t.Errorf("messageId = %v, want the caller's my-id", refund["messageId"])
	}
	// Absent properties and traits go out as {}, and an identify has no timestamp.
	bare := r.batch[3]
	if fmt.Sprint(refund["properties"], bare["traits"]) != "map[] map[]" || bare["timestamp"] != nil {
		t.Errorf("capture properties %v, identify %v", refund["properties"], bare)
	}
	if len(errs.all()) != 0 {
		t.Errorf("errors: %v", errs.all())
	}
}

func TestDefaultsTheTimestampToNowInIntegerMilliseconds(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{})
	before := time.Now().UnixMilli()
	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	after := time.Now().UnixMilli()
	must(t, c.Flush(context.Background()))

	ms, err := api.snapshot()[0].batch[0]["timestamp"].(json.Number).Int64()
	if err != nil || ms < before || ms > after {
		t.Errorf("timestamp = %d (%v), want an integer in [%d, %d]", ms, err, before, after)
	}
}

func TestRejectsInvalidMessagesWithoutSendingThem(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{})

	for _, err := range []error{
		c.Capture(Capture{Event: "no_distinct_id"}),
		c.Capture(Capture{DistinctID: "u"}),
		c.Identify(Identify{}),
	} {
		if err == nil {
			t.Error("an invalid message was accepted")
		}
	}
	must(t, c.Close(context.Background()))
	if n := len(api.snapshot()); n != 0 {
		t.Errorf("got %d requests, want 0", n)
	}
}

func TestDropsOnlyTheMessageThatCannotBeEncoded(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "ok"}))
	var typeErr *json.UnsupportedTypeError
	if err := c.Capture(Capture{DistinctID: "u", Event: "chan", Properties: map[string]any{"c": make(chan int)}}); !errors.As(err, &typeErr) {
		t.Errorf("chan property: err = %v, want a *json.UnsupportedTypeError", err)
	}
	var valueErr *json.UnsupportedValueError
	if err := c.Identify(Identify{DistinctID: "u", Traits: map[string]any{"score": math.NaN()}}); !errors.As(err, &valueErr) {
		t.Errorf("NaN trait: err = %v, want a *json.UnsupportedValueError", err)
	}
	// The API refuses NaN and infinity anywhere in a batch, so a nested one drops its message too.
	nested := map[string]any{"cart": []any{map[string]any{"price": math.Inf(1)}}}
	if err := c.Capture(Capture{DistinctID: "u", Event: "inf", Properties: nested}); !errors.As(err, &valueErr) {
		t.Errorf("nested infinity: err = %v, want a *json.UnsupportedValueError", err)
	}
	must(t, c.Close(context.Background()))

	if got := api.events(); fmt.Sprint(got) != "[ok]" {
		t.Errorf("sent %v, want [ok]", got)
	}
}

func TestCaptureDoesNotWaitForTheNetwork(t *testing.T) {
	api := newFakeAPI(t)
	release := api.hold(t)
	c, _ := newClient(t, api.URL, Config{FlushAt: 1})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "first"}))
	waitFor(t, "the first request", func() bool { return len(api.snapshot()) == 1 })
	started := time.Now()
	for i := 0; i < 100; i++ {
		must(t, c.Capture(Capture{DistinctID: "u", Event: "queued"}))
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("100 captures took %v while a request was in flight", elapsed)
	}
	release()
}

func TestDropsNewMessagesOnceTheQueueIsFull(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{MaxQueueSize: 1})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	if err := c.Capture(Capture{DistinctID: "u", Event: "b"}); !errors.Is(err, ErrQueueFull) {
		t.Errorf("err = %v, want ErrQueueFull", err)
	}
	must(t, c.Flush(context.Background()))
	if got := api.events(); fmt.Sprint(got) != "[a]" {
		t.Errorf("sent %v, want [a]", got)
	}
}

func TestFlushesWhenTheQueueReachesFlushAt(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{FlushAt: 2})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	must(t, c.Capture(Capture{DistinctID: "u", Event: "b"}))
	waitFor(t, "a batch of 2", func() bool { r := api.snapshot(); return len(r) == 1 && len(r[0].batch) == 2 })
}

func TestFlushesOnTheInterval(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{FlushInterval: 20 * time.Millisecond})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	waitFor(t, "a request", func() bool { return len(api.snapshot()) == 1 })
}

func TestANegativeFlushIntervalTurnsTheTimerOff(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{FlushInterval: -1})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	time.Sleep(100 * time.Millisecond)
	if n := len(api.snapshot()); n != 0 {
		t.Fatalf("got %d requests before Flush, want 0", n)
	}
	must(t, c.Flush(context.Background()))
	if got := api.events(); fmt.Sprint(got) != "[a]" {
		t.Errorf("sent %v, want [a]", got)
	}
}

func TestFlushSendsEverythingQueuedInBatchesOfFlushAt(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{})

	for i := 0; i < 250; i++ {
		must(t, c.Capture(Capture{DistinctID: "u", Event: "e"}))
	}
	must(t, c.Flush(context.Background()))

	total := 0
	for _, r := range api.snapshot() {
		if len(r.batch) > 100 {
			t.Errorf("a batch held %d messages, want at most 100", len(r.batch))
		}
		total += len(r.batch)
	}
	if total != 250 {
		t.Errorf("sent %d messages when Flush returned, want 250", total)
	}
}

func TestFlushReturnsWhenItsContextIsDone(t *testing.T) {
	api := newFakeAPI(t)
	release := api.hold(t)
	c, _ := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	release()
}

func TestRetries503And429WithTheSameMessagesAndHonorsRetryAfter(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	api := newFakeAPI(t,
		apiResponse{status: 503, header: map[string]string{"Retry-After": "0"}, body: `{"success":false}`},
		apiResponse{status: 429, header: map[string]string{"Retry-After": past}, body: `{"success":false}`},
	)
	c, errs := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	started := time.Now()
	must(t, c.Flush(context.Background()))

	requests := api.snapshot()
	if len(requests) != 3 {
		t.Fatalf("got %d requests, want 3", len(requests))
	}
	if requests[2].batch[0]["messageId"] != requests[0].batch[0]["messageId"] {
		t.Error("the retry has a new messageId")
	}
	// Each backoff is at least 500 ms, so a fast flush proves both Retry-After values were used.
	if elapsed := time.Since(started); elapsed > 400*time.Millisecond {
		t.Errorf("flush took %v; Retry-After 0 and a past HTTP date mean no wait", elapsed)
	}
	if len(errs.all()) != 0 {
		t.Errorf("errors: %v", errs.all())
	}
}

func TestParsesRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	for header, want := range map[string]time.Duration{
		"0":                             0,
		"2":                             2 * time.Second,
		"1.5":                           1500 * time.Millisecond,
		"120":                           60 * time.Second,
		"Sun, 08 Nov 2026 12:00:30 GMT": 30 * time.Second,
		"Sun, 08 Nov 2026 13:00:00 GMT": 60 * time.Second,
		"Sun, 08 Nov 2026 11:00:00 GMT": 0,
	} {
		if got, ok := retryAfter(header, now); !ok || got != want {
			t.Errorf("retryAfter(%q) = %v, %v; want %v", header, got, ok, want)
		}
	}
	for _, header := range []string{"", "-1", "NaN", "soon"} {
		if got, ok := retryAfter(header, now); ok {
			t.Errorf("retryAfter(%q) = %v, want no value", header, got)
		}
	}
}

func TestRetriesANetworkError(t *testing.T) {
	api := newFakeAPI(t, apiResponse{hangUp: true})
	c, errs := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	must(t, c.Flush(context.Background()))

	requests := api.snapshot()
	if len(requests) != 2 || requests[1].batch[0]["messageId"] != requests[0].batch[0]["messageId"] {
		t.Errorf("got %d requests, want 2 with the same messageId", len(requests))
	}
	if len(errs.all()) != 0 {
		t.Errorf("errors: %v", errs.all())
	}
}

func TestDoesNotRetryA4xxAndReportsIt(t *testing.T) {
	api := newFakeAPI(t, apiResponse{status: 401, body: `{"success":false,"message":"Permission denied"}`})
	c, errs := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	must(t, c.Flush(context.Background()))

	if n := len(api.snapshot()); n != 1 {
		t.Errorf("got %d requests, want 1", n)
	}
	got := errs.vexoErrors(t)
	if len(got) != 1 || got[0].Status != 401 || len(got[0].Batch) != 1 {
		t.Errorf("errors = %v, want one with status 401 and the batch", got)
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	retryNow := map[string]string{"Retry-After": "0"}
	for _, tc := range []struct {
		maxRetries, requests int
	}{{2, 3}, {-1, 1}} {
		api := newFakeAPI(t, apiResponse{500, retryNow, "", false}, apiResponse{500, retryNow, "", false},
			apiResponse{500, retryNow, "", false}, apiResponse{500, retryNow, "", false})
		c, errs := newClient(t, api.URL, Config{MaxRetries: tc.maxRetries})

		must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
		must(t, c.Flush(context.Background()))

		if n := len(api.snapshot()); n != tc.requests {
			t.Errorf("MaxRetries %d: got %d requests, want %d", tc.maxRetries, n, tc.requests)
		}
		if got := errs.vexoErrors(t); len(got) != 1 || got[0].Status != 500 {
			t.Errorf("MaxRetries %d: errors = %v, want one with status 500", tc.maxRetries, got)
		}
	}
}

func TestReportsTheMessagesTheAPIDropped(t *testing.T) {
	api := newFakeAPI(t, apiResponse{status: 200,
		body: `{"success":true,"code":200,"data":{"accepted":1,"dropped":[{"index":1,"reason":"Json node cannot exceed 65536 length"}],"failed":[]}}`})
	c, errs := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "ok"}))
	must(t, c.Capture(Capture{DistinctID: "u", Event: "too_big"}))
	must(t, c.Flush(context.Background()))

	if n := len(api.snapshot()); n != 1 {
		t.Errorf("got %d requests, want 1: dropped messages aren't resent", n)
	}
	got := errs.vexoErrors(t)
	if len(got) != 1 || len(got[0].Dropped) != 1 {
		t.Fatalf("errors = %v, want one with one dropped message", got)
	}
	var dropped captureMessage
	must(t, json.Unmarshal(got[0].Dropped[0].Message, &dropped))
	if dropped.Event != "too_big" || !strings.Contains(got[0].Dropped[0].Reason, "65536") {
		t.Errorf("dropped = %+v", got[0].Dropped[0])
	}
}

func TestCountsAn2xxBodyItCannotReadAsSuccess(t *testing.T) {
	api := newFakeAPI(t, apiResponse{status: 200, body: "OK"})
	c, errs := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	must(t, c.Flush(context.Background()))

	if n := len(api.snapshot()); n != 1 || len(errs.all()) != 0 {
		t.Errorf("got %d requests and errors %v, want 1 request and no errors", n, errs.all())
	}
}

func TestSendsAgainOnlyTheMessagesTheAPIReportsAsFailed(t *testing.T) {
	api := newFakeAPI(t, apiResponse{status: 200,
		body: `{"success":true,"code":200,"data":{"accepted":1,"dropped":[],"failed":[{"index":1,"reason":"Storing failed; retry this message"}]}}`})
	c, errs := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "a", Event: "stored"}))
	must(t, c.Capture(Capture{DistinctID: "b", Event: "failed_once"}))
	must(t, c.Flush(context.Background()))

	requests := api.snapshot()
	if len(requests) != 2 || len(requests[1].batch) != 1 || requests[1].batch[0]["event"] != "failed_once" {
		t.Fatalf("events = %v, want [stored failed_once failed_once]", api.events())
	}
	if requests[1].batch[0]["messageId"] != requests[0].batch[1]["messageId"] {
		t.Error("the resent message has a new messageId")
	}
	if len(errs.all()) != 0 {
		t.Errorf("errors: %v", errs.all())
	}
}

func TestReportsMessagesStillFailedAfterTheLastRetry(t *testing.T) {
	api := newFakeAPI(t, apiResponse{status: 200,
		body: `{"success":true,"code":200,"data":{"accepted":0,"dropped":[],"failed":[{"index":0,"reason":"Storing failed; retry this message"}]}}`})
	c, errs := newClient(t, api.URL, Config{MaxRetries: -1})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	must(t, c.Flush(context.Background()))

	got := errs.vexoErrors(t)
	if len(api.snapshot()) != 1 || len(got) != 1 || len(got[0].Dropped) != 1 || got[0].Dropped[0].Reason != "Storing failed; retry this message" {
		t.Errorf("requests %d, errors %v", len(api.snapshot()), got)
	}
}

func TestOneAttemptPerBatchAfterABatchUsedUpItsRetriesUntilARequestSucceeds(t *testing.T) {
	fail := apiResponse{status: 500, header: map[string]string{"Retry-After": "0"}}
	ok := apiResponse{status: 200}
	api := newFakeAPI(t,
		fail, fail, fail, // batch 1 uses up its 2 retries and opens the circuit
		fail,     // batch 2: one attempt
		ok,       // batch 3: one attempt, which succeeds and closes the circuit
		fail, ok, // batch 4: retried again
	)
	c, errs := newClient(t, api.URL, Config{FlushAt: 1, MaxRetries: 2})

	for i := 1; i <= 4; i++ {
		must(t, c.Capture(Capture{DistinctID: "u", Event: fmt.Sprint("e", i)}))
	}
	must(t, c.Flush(context.Background()))

	if got := api.events(); fmt.Sprint(got) != "[e1 e1 e1 e2 e3 e4 e4]" {
		t.Errorf("requests = %v", got)
	}
	if n := len(errs.all()); n != 2 {
		t.Errorf("got %d errors, want 2 (batches 1 and 2)", n)
	}
}

func TestSplitsBatchesToKeepEachBodyUnder5MB(t *testing.T) {
	api := newFakeAPI(t)
	c, errs := newClient(t, api.URL, Config{})
	blob := strings.Repeat("x", 2*1024*1024)

	for i := 0; i < 3; i++ {
		must(t, c.Capture(Capture{DistinctID: "u", Event: fmt.Sprint("e", i), Properties: map[string]any{"blob": blob}}))
	}
	if err := c.Capture(Capture{DistinctID: "u", Event: "huge", Properties: map[string]any{"blob": blob + blob + blob}}); err == nil {
		t.Error("a message over 5 MB was accepted")
	}
	must(t, c.Flush(context.Background()))

	var sizes []int
	for _, r := range api.snapshot() {
		sizes = append(sizes, len(r.batch))
		if r.size > maxBodyBytes {
			t.Errorf("a request body has %d bytes", r.size)
		}
	}
	if fmt.Sprint(sizes) != "[2 1]" {
		t.Errorf("batch sizes = %v, want [2 1]", sizes)
	}
	if len(errs.all()) != 0 {
		t.Errorf("errors: %v", errs.all())
	}
}

func TestCloseSendsTheQueueAndRejectsLaterMessages(t *testing.T) {
	api := newFakeAPI(t)
	c, _ := newClient(t, api.URL, Config{})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
	must(t, c.Close(context.Background()))
	if got := api.events(); fmt.Sprint(got) != "[a]" {
		t.Errorf("sent %v, want [a]", got)
	}
	if err := c.Capture(Capture{DistinctID: "u", Event: "late"}); !errors.Is(err, ErrClosed) {
		t.Errorf("Capture after Close: %v, want ErrClosed", err)
	}
	if err := c.Identify(Identify{DistinctID: "u"}); !errors.Is(err, ErrClosed) {
		t.Errorf("Identify after Close: %v, want ErrClosed", err)
	}
	if err := c.Flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Flush after Close: %v, want ErrClosed", err)
	}
	must(t, c.Close(context.Background()))
}

func TestCloseGivesUpAtShutdownTimeoutWhenTheAPIIsUnreachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	host := "http://" + listener.Addr().String()
	listener.Close() // connections are refused from now on
	c, errs := newClient(t, host, Config{FlushAt: 1, Timeout: 500 * time.Millisecond, ShutdownTimeout: time.Second})

	for i := 0; i < 5; i++ {
		must(t, c.Capture(Capture{DistinctID: "u", Event: fmt.Sprint("e", i)}))
	}
	started := time.Now()
	err = c.Close(context.Background())

	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Errorf("Close took %v, want about 1s", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close returned %v, want context.DeadlineExceeded", err)
	}
	reported := 0
	deadline := false
	for _, e := range errs.vexoErrors(t) {
		reported += len(e.Batch)
		deadline = deadline || strings.Contains(e.Error(), "deadline")
	}
	if reported != 5 || !deadline {
		t.Errorf("reported %d messages (deadline error: %v), want all 5", reported, deadline)
	}
}

func TestCloseHonorsItsContextDeadline(t *testing.T) {
	api := newFakeAPI(t)
	api.hold(t)
	c, errs := newClient(t, api.URL, Config{FlushAt: 1})

	must(t, c.Capture(Capture{DistinctID: "u", Event: "in_flight"}))
	waitFor(t, "the request", func() bool { return len(api.snapshot()) == 1 })
	must(t, c.Capture(Capture{DistinctID: "u", Event: "queued"}))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := c.Close(ctx)

	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("Close took %v with a 200ms context", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close returned %v, want context.DeadlineExceeded", err)
	}
	reported := 0
	for _, e := range errs.vexoErrors(t) {
		reported += len(e.Batch)
	}
	if reported != 2 {
		t.Errorf("reported %d messages, want the one in flight and the one queued", reported)
	}
}

func TestCloseLeavesNoGoroutineBehind(t *testing.T) {
	api := newFakeAPI(t)
	before := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		c, err := New("test-key", Config{Host: api.URL, OnError: func(error) {}})
		must(t, err)
		must(t, c.Capture(Capture{DistinctID: "u", Event: "a"}))
		must(t, c.Flush(context.Background()))
		must(t, c.Close(context.Background()))
	}
	api.Close() // also closes the idle client connections, which belong to http.DefaultTransport

	waitFor(t, fmt.Sprintf("the goroutine count to return to %d", before), func() bool { return runtime.NumGoroutine() <= before })
}

func TestIsSafeForConcurrentCallers(t *testing.T) {
	api := newFakeAPI(t)
	c, errs := newClient(t, api.URL, Config{FlushAt: 50})

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := c.Capture(Capture{DistinctID: fmt.Sprint("user_", g), Event: "e"}); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			c.Flush(context.Background())
		}
	}()
	wg.Wait()
	must(t, c.Close(context.Background()))

	ids := map[any]bool{}
	for _, r := range api.snapshot() {
		for _, m := range r.batch {
			ids[m["messageId"]] = true
		}
	}
	if len(ids) != 2000 || len(api.events()) != 2000 {
		t.Errorf("sent %d messages with %d distinct messageIds, want 2000 of each", len(api.events()), len(ids))
	}
	if len(errs.all()) != 0 {
		t.Errorf("errors: %v", errs.all())
	}
}
