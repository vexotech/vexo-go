# vexo-go

Send events and identifies to [Vexo](https://vexo.co) from your Go backend: purchases, subscription changes, background jobs, anything your app's SDK can't see. Server events land in the same app as your mobile and web events, bill as events, and join the person who has the same distinct ID.

- No dependencies outside the standard library (Go 1.21 or later).
- Batches messages on one goroutine, sends on size and on an interval, and sends what's left when you call `Close`, within a deadline.
- Retries network errors, `429`, and `5xx` responses (and the messages the API reports as `failed`) with exponential backoff, and honors `Retry-After`.
- Keeps each request under the API's 5 MB body limit, and is safe for concurrent use.

## Install

```bash
go get github.com/vexotech/vexo-go
```

While the `vexotech/vexo-go` repository is private, `go get` needs access to it: set `GOPRIVATE=github.com/vexotech/*` and give Git credentials for GitHub (for example, a `~/.netrc` entry or `git config url."git@github.com:".insteadOf "https://github.com/"`).

## Usage

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/vexotech/vexo-go"
)

func main() {
	client, err := vexo.New(os.Getenv("VEXO_API_KEY"), vexo.Config{})
	if err != nil {
		log.Fatal(err)
	}
	// Send everything still queued before main returns.
	defer client.Close(context.Background())

	// Attach traits to the person. Use the ID you pass to identifyDevice() in the app SDK.
	if err := client.Identify(vexo.Identify{
		DistinctID: "user@example.com",
		Traits:     map[string]any{"plan": "pro", "company": "Acme"},
	}); err != nil {
		log.Print(err)
	}

	// Record an event.
	if err := client.Capture(vexo.Capture{
		DistinctID: "user@example.com",
		Event:      "purchase_completed",
		Properties: map[string]any{"amount": 49.9, "currency": "USD"},
	}); err != nil {
		log.Print(err)
	}
}
```

Create one client when your program starts and share it: it's safe for concurrent use. `Capture` and `Identify` only queue the message, so they never block a request handler.

Copy your app's API key from the app's [settings](https://docs.vexo.co/settings/app-settings/app-general-information) in the Vexo dashboard. It's the same key your app SDK uses, so it isn't a secret; see [Security](https://docs.vexo.co/server-side/capture#security).

## Shut down

Go has no exit hook, so call `Close` before your program exits; otherwise, the messages still queued are lost. `Close` sends everything queued and stops the client's goroutine. It gives up at `ShutdownTimeout` (10 seconds by default) or when its context is done, whichever comes first: it aborts the request in flight, reports what's still queued to `OnError`, and returns the context error.

A server that stops on a signal closes the client after it stops serving:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
// ... start the server, then wait for the signal:
<-ctx.Done()
server.Shutdown(context.Background())
// Not ctx: it's already done, so Close would give up at once.
closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
client.Close(closeCtx)
```

## Serverless functions

A serverless platform can freeze or stop your process as soon as a handler returns, so the client's goroutine might never send what you queued. Call `Flush` before your handler returns.

### AWS Lambda

Create the client once, in `main`, so warm invocations reuse it, and flush at the end of each invocation. Lambda freezes the process between invocations, so the interval timer doesn't run while you aren't handling a request.

```go
var client *vexo.Client

func main() {
	var err error
	client, err = vexo.New(os.Getenv("VEXO_API_KEY"), vexo.Config{})
	if err != nil {
		log.Fatal(err)
	}
	lambda.Start(handler)
}

func handler(ctx context.Context, order Order) error {
	client.Capture(vexo.Capture{DistinctID: order.UserID, Event: "order_paid"})
	// Leave time for the response: flush for at most 2 seconds.
	flushCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	client.Flush(flushCtx)
	return nil
}
```

`Flush` returns the context error if its context is done first; the messages stay queued, and the next invocation sends them.

### Google Cloud Run

Cloud Run with request-based billing gives your container CPU only while it handles a request, so call `Flush` before you write the response, as on Lambda. Cloud Run sends `SIGTERM` and waits 10 seconds before it stops an instance; call `Close` with a deadline shorter than that, as in [Shut down](#shut-down). With instance-based billing (CPU always allocated), the interval timer runs between requests, and closing on `SIGTERM` is enough.

## API

### `vexo.New(apiKey string, config vexo.Config) (*vexo.Client, error)`

Returns a client and starts its goroutine. It returns an error when `apiKey` is empty or `Host` isn't an `http` or `https` URL. A zero field of `Config` selects its default.

| Field | Default | Description |
| --- | --- | --- |
| `Host` | `https://api.vexo.co` | API base URL. |
| `FlushAt` | `100` | Send a batch when this many messages are queued (up to 500). |
| `FlushInterval` | `10 * time.Second` | Send whatever is queued this often. |
| `MaxRetries` | `3` | Retries for network errors, `429`, and `5xx` responses. A negative value disables retries. |
| `MaxQueueSize` | `10000` | Messages kept in memory; new messages are dropped once it's full. |
| `Timeout` | `10 * time.Second` | Timeout of each HTTP request. |
| `ShutdownTimeout` | `10 * time.Second` | How long `Close` keeps sending. |
| `OnError` | `log.Print` | Called with send failures and with the messages the API dropped. |

### `client.Capture(vexo.Capture) error`

Queues an event. `Capture` has these fields:

| Field | Description |
| --- | --- |
| `DistinctID` | The person's ID. Required. |
| `Event` | The event name. Required. |
| `Properties` | A `map[string]any`, encoded with `encoding/json`. |
| `Timestamp` | When the event happened, as a `time.Time`. Defaults to the time you call `Capture`. It's sent as integer epoch milliseconds. |
| `MessageID` | A unique ID that lets the API store a resent message once. Defaults to a new UUIDv4. |

### `client.Identify(vexo.Identify) error`

Queues an identify. `Traits` merge into the person's existing traits; a trait set to `nil` is removed.

### `client.Flush(ctx context.Context) error`

Sends everything queued and returns when the queue is empty. It returns `ctx.Err()` when the context is done first, and `vexo.ErrClosed` after `Close`. Send failures go to `OnError`.

### `client.Close(ctx context.Context) error`

Sends everything queued, stops the client's goroutine, and returns. See [Shut down](#shut-down). You can call it more than once.

## Errors and dropped messages

`Capture` and `Identify` return an error, and drop that one message, when the message itself can't be queued:

- `DistinctID` or `Event` is empty.
- A property or trait can't be encoded as JSON, such as a channel, a function, or `math.NaN()`. The error wraps the `encoding/json` error. Values that `encoding/json` encodes as strings, such as a `time.Time` or a type that implements `encoding.TextMarshaler` (most UUID and decimal types), are sent as strings.
- The message is over 5 MB.
- The queue is full (`vexo.ErrQueueFull`) or the client is closed (`vexo.ErrClosed`).

Problems that happen later, on the client's goroutine, go to `OnError`, usually as a `*vexo.Error`:

- A batch that still fails after the retries, or that the API rejects with a `4xx`, arrives with `Status` (the HTTP status, or `0` after a network error) and `Batch` (the JSON of the messages it held). After that, the next batches get a single attempt until a request succeeds, so an unreachable API doesn't hold up your program.
- A message the API dropped (for example, properties over 64 KB) arrives in `Dropped`, a list of messages, each with the API's `Reason`. Messages the API reports as `failed` are retried, and reported the same way if they still fail.
- Messages still queued at the shutdown deadline arrive as one error with `Batch`.

`OnError` runs on the client's goroutine: return quickly, and don't call `Flush` or `Close` from it.

Every message carries a `messageId`, so a retried batch is stored once.

## Development

```bash
go vet ./...
go test -race ./...
```

To run the shared conformance scenarios from a `vexo-sdk-conformance` checkout next to this one:

```bash
python3 ../vexo-sdk-conformance/run.py --driver "go run ./conformance/driver" --skip exit_hook
```

Skip `exit_hook`: Go has no exit hook, so you call `Close` yourself.

## License

MIT
