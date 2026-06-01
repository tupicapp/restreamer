# Pipe Stream Design

## Goal

Add a new in-memory stream bridge in `core/pipe.go` that allows cascading two `Streamer` instances.

The pipe is not a protocol destination. It is a local bridge that exposes:

- one `Stream` view for writing into the pipe: `AsOutput()`
- one `Stream` view for reading from the pipe: `AsInput()`

This allows:

1. `streamerA` to use a normal input.
2. `streamerA` to use `pipe.AsOutput()` as one of its outputs.
3. `streamerB` to use `pipe.AsInput()` as one of its inputs.
4. `streamerB` to route that media into any normal destination.

## Why `AsInput` / `AsOutput`

`Streamer` only works with the `Stream` interface.

Because of that, the pipe should not be passed into a streamer directly as a custom object. Instead, the pipe owns shared channels internally and returns two `Stream` adapters:

- input-side adapter
- output-side adapter

Each adapter implements `Stream`.

## Ownership Model

The writing side owns the pipe lifecycle.

Rules:

- `AsOutput()` is the owner side.
- `AsInput()` is the consumer side.
- only one output-side adapter is allowed
- only one input-side adapter is allowed
- if the output side closes, the shared media channels are closed
- when the shared channels are closed, the input side naturally observes end-of-stream
- this is a normal condition, not an error

## Stop Semantics

`Stop()` on a pipe side does not destroy the pipe.

It only disables forwarding for that side.

Operational meaning:

- if the output side is stopped, frames sent to its exposed channels are drained and dropped instead of being forwarded into the shared bridge
- if the input side is stopped, it should not emit frames to its consumer-facing channels
- `Start()` re-enables forwarding on that side
- `Close()` is final for that side

This keeps `Stop()` aligned with existing stream lifecycle expectations while preserving pipe ownership rules.

## Data Flow

The pipe owns shared bridge channels for:

- video
- audio

### Output side

The output-side `Stream` exposes writable channels through:

- `GetVideoChan()`
- `GetAudioChan()`

`streamerA` writes frames into those channels exactly like any other output.

The output-side adapter runs forwarding loops:

- read from output adapter input channels
- if started, forward into the pipe shared channels
- if stopped, drop frames
- if closed, close the shared channels exactly once

### Input side

The input-side `Stream` exposes readable channels through:

- `GetVideoChan()`
- `GetAudioChan()`

The input-side adapter runs forwarding loops:

- read from the pipe shared channels
- if started, forward into the input adapter output channels
- if stopped, consume and drop frames
- if the shared channels close, close the input adapter channels

This allows `streamerB` to treat the pipe as a normal input stream.

## Stream Interface Expectations

Both adapters must implement `shared.Stream`.

### `Type()`

Use stable explicit values:

- output side: `pipe_output`
- input side: `pipe_input`

### `GetID()`

Base pipe id is user-defined, for example `pipe-1`.

Recommended derived ids:

- input side: `<pipe-id>:input`
- output side: `<pipe-id>:output`

### `IsRestartable()`

Return `false`.

The pipe is an in-memory bridge, not a reconnectable external resource.

### `RestartInterval()`

Return a harmless default value to satisfy the interface.

### `Clone()`

Return an error.

Reason:

- the pipe has strict single-owner and single-consumer semantics
- cloning a side would violate ownership and close behavior

### `WaitForStart(ctx)`

Each side should report started when its own `Start()` has been called.

This is local side state only.

It should not wait for the opposite side.

## Concurrency Rules

The implementation must be safe for concurrent use from both streamers.

Requirements:

- protect one-time side creation with mutexes
- protect started/stopped/closed flags
- close shared channels exactly once
- close side-facing channels exactly once
- avoid goroutine leaks when one side closes before the other

## Buffering

Use bounded buffered channels.

Initial expectation:

- shared pipe video/audio channels should use a reasonable default buffer
- side-facing channels should also be buffered

The exact size can follow existing project defaults unless implementation evidence suggests otherwise.

## Event and State Model

Each side should maintain its own minimal lifecycle state and events.

Expected events:

- started
- stopped
- closed

State should include at least:

- `IsStarted`
- `StreamID`
- `Type`
- lightweight `LastIO` updates when frames pass through the side

No URL is required because the pipe is in-memory.

## Example Usage

```go
pipe := core.NewPipe("cascade-1")

upstream := core.NewStreamer()
downstream := core.NewStreamer()

upstream.UpdateStreams(
	[]core.Stream{normalInput},
	[]core.Stream{pipe.AsOutput()},
)

downstream.UpdateStreams(
	[]core.Stream{pipe.AsInput()},
	[]core.Stream{normalOutput},
)
```

Then:

- upstream selected input produces frames
- upstream multicaster writes into `pipe.AsOutput()`
- pipe forwards frames across the bridge
- downstream reads from `pipe.AsInput()`
- downstream writes to its own outputs

## Non-Goals

Not in scope for the first version:

- multiple readers
- multiple writers
- fan-out inside the pipe
- backpressure negotiation between streamers
- protocol conversion
- persistence or recording
- restart cloning support

## Implementation Shape

Recommended files:

- `core/pipe.go`
- tests in `core/test` or targeted unit tests near `core`

Recommended exported API:

```go
type Pipe struct { ... }

func NewPipe(id string) *Pipe
func (p *Pipe) AsInput() Stream
func (p *Pipe) AsOutput() Stream
```

Internally:

- one shared `pipeCore`
- one `pipeInputStream`
- one `pipeOutputStream`

## Acceptance Criteria

Implementation is correct when all of the following are true:

1. Two streamers can be chained through one pipe.
2. Upstream output frames become downstream input frames.
3. Closing the output side closes the shared bridge and downstream sees closed input channels.
4. Stopping a side pauses forwarding but does not destroy the pipe immediately.
5. Only one input-side adapter and one output-side adapter can exist for a given pipe.
6. The implementation does not panic on repeated `Start`, `Stop`, or `Close`.
