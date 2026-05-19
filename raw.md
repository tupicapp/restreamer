# Raw Processing

This document describes the raw-domain pipeline in Restreamer.

## Overview

`RawStreamer` is the generic stream node that works in raw frame space.

It:

- consumes one or more existing input streams
- inspects each input codec and creates the needed decoder
- converts input streams into raw frames
- passes raw frames to a pluggable raw processor
- receives processed raw frames back
- encodes the processed raw frame into a normal stream again
- exposes a normal `Stream` output that the rest of the system can use

That means raw-domain work is an internal specialization, not a separate top-level
pipeline beside inputs and outputs.

## High-Level Flow

```text
Inputs
  -> decoder selection per input codec
  -> raw video frames
  -> RawProcessor
  -> raw output frame
  -> encoder selection per output encoding
  -> normal stream output
```

## Stream Graph Position

`RawStreamer` stays compatible with the normal stream graph:

```text
Inputs -> RawStreamer -> Streamer/Switcher -> MultiCaster -> Outputs
```

This keeps `inputs` and `outputs` packages unchanged.

## Core Rule

`RawStreamer` owns orchestration.

It is responsible for:

- input stream consumption
- decoder creation
- latest-frame buffering
- raw processor invocation
- output encoder creation
- audio passthrough or audio mix strategy
- normal stream lifecycle and events

`RawProcessor` owns raw-domain logic only.

It must not know:

- input URL types
- output destinations
- switcher behavior
- stream graph wiring

## Processor Contract

The extension point is the raw processor interface in `core/raw`.

Conceptually:

```go
type Processor interface {
    Process(ProcessRequest) (*VideoFrame, error)
}
```

The request contains:

- output canvas
- raw input placements

The processor returns one raw output frame.

`RawStreamer` applies stable stream metadata such as:

- output stream identity
- output PTS/DTS
- output duration
- output timestamp

## Default Processor: Composer

The default raw processor is `Composer`.

It composes multiple raw inputs into a single raw output frame using layout data:

- x/y placement
- width/height
- z-index
- transparency

`RawStreamer` carries those layout fields through with each placement, but the
actual layer ordering and alpha blending live in the raw processor. For the
default scene implementation, `Composer` sorts by `z-index` and blends each
placement using `transparency`.

This is the current scene implementation.

## Scenes Are A Usage Of RawStreamer

Scenes are no longer the primitive.

Scenes are one usage of `RawStreamer` with:

- `Composer` as the raw processor
- scene layouts as raw processor input
- scene output type set to `scene`

Conceptually:

```text
Scene = RawStreamer + Composer + Layout
```

So creating scenes is one application of the generic raw pipeline, not a separate
special-case engine.

## Decode Stage

For each input:

- if the stream already provides `rawvideo`, `RawStreamer` consumes it directly
- otherwise `RawStreamer` inspects the codec and creates the related decoder
- decoder creation is local to that input runtime

Current supported decode path in the implementation:

- `rawvideo`
- `h264`

The design stays open to adding more decoders without changing input or output
packages.

## Raw Processing Stage

After decoding, `RawStreamer` operates on raw frames.

This stage is where raw processors can do work such as:

- scene composition
- overlays
- AI inference results
- segmentation
- algorithmic transforms
- future filtergraph-style steps

`RawStreamer` does not encode raw logic itself. It delegates to the configured
processor.

## Encode Stage

After raw processing, `RawStreamer` selects the encoder based on requested output
encoding.

Current supported output video encoding in the implementation:

- `h264`

The encoded result becomes a normal stream again so the rest of the system can
route, switch, and fan out without knowing anything about raw processing internals.

## Audio

In the current implementation, raw processing is video-focused.

Audio is handled separately by `RawStreamer` using one of these strategies:

- passthrough from one selected input
- mix multiple AAC inputs after decode-to-PCM and re-encode to AAC

This keeps audio decoupled from the video raw processor interface.

## Timing

`RawStreamer` runs on an output clock.

For each output tick:

- it snapshots the latest raw frame from every ready input
- it calls the raw processor
- it stamps output timing metadata
- it pushes the frame into the encoder
- if an input session has died, or has gone stale past its restart interval, it keeps using that input's last decoded frame until a replacement session produces fresh frames again

This matches the intended scene behavior and fits future processor types too.

## Open/Closed Direction

This architecture is meant to stay open for extension and closed for modification.

Adding a new raw-domain behavior should usually mean:

- implement a new raw processor in `core/raw`
- instantiate a `RawStreamer` with that processor

It should not require:

- changing `core/inputs`
- changing `core/outputs`
- rewriting the switcher

## Current Assumptions

- `RawStreamer` exposes one normal output stream.
- Fan-out still happens later through `Streamer` and `MultiCaster`.
- Raw processing is video-only in the processor contract for now.
- Audio remains a separate strategy inside `RawStreamer`.
- The default scene implementation is `Composer`.
- Current codec coverage is intentionally narrow and can be extended incrementally.
