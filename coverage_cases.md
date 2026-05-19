# Coverage Cases

This file lists the runtime and command scenarios that the current implementation
is expected to handle and should be covered by tests.

It is intentionally short and grouped by behavior, not by file.

## CLI And Spec Parsing

- `rawscene` builds one `RawStreamer` backed by the default `Composer`.
- `scene` and `rawscene` accept layout values as `x,y,width,height[,z[,transparency]]`.
- Single input plus `--canvas` and no explicit layout defaults to a full-canvas layout.
- Canvas can be derived from one or more layouts.
- Mismatched `--input` and `--layout` counts are rejected.
- `--audio-from` is validated against input count.
- `--audio-ratio` count and sum are validated and normalized.
- Layout flags preserve comma-separated values and input order.
- Audio-ratio flags preserve input order.

## Raw Composition

- `RawStreamer` is the generic raw-domain node.
- Scene composition is one usage of `RawStreamer` through `Composer`.
- `Composer` applies:
- x/y placement
- scaling to target width/height
- z-index ordering
- transparency blending
- Inputs and outputs packages remain unchanged by scene composition.

## Video Ingest And Decode

- Raw video input can be consumed directly.
- H.264 input can be decoded to raw video.
- H.264 SPS/PPS received before the first keyframe is cached and reused so decoding can start correctly.
- Decoder creation is driven by input codec, per input.
- Output video encoding is driven by output requirements.

## Placement Snapshot And Partial Readiness

- `RawStreamer` can start composing as soon as at least one input has a decoded frame.
- Inputs that have never produced a frame yet render as background / missing placement state.
- One missing input must not block the entire composed output.
- If all inputs are missing, composition for that tick is rejected.

## Stable Output Policy

- Output cadence is driven by the shared pre-encoder timeline, not by per-input arrival timing.
- If an input stops delivering video frames, the last decoded frame for that input is reused.
- Input jitter must not stall the composed video encoder.
- Missing audio samples are padded with silence so audio cadence stays stable.
- Input instability must not stop the output stream.

## Per-Input Lifecycle And Recovery

- Each input is supervised independently.
- Logical input lifecycle is handled as `initial`, `live`, `dead`.
- If an input session closes, a replacement session is attached automatically.
- If an input stays open but stops producing IO past `RestartInterval`, it is treated as stale and replaced automatically.
- While a dead/stale input is being replaced, its last decoded video frame remains visible.
- When the upstream source starts again later, the same logical input resumes live updates automatically.
- Old decoder workers from a previous input generation cannot overwrite revived input state.

## Audio Handling

- Audio processing is separate from the raw video processor contract.
- Selected-input audio mode is supported.
- Multi-input audio mix mode is supported with `--audio-ratio`.
- AAC input can be decoded to PCM when needed.
- Mixed/processed audio can be re-encoded to AAC.
- Audio buffering keeps output audio cadence stable.
- Silence padding is used when buffered audio runs short.

## Audio Resample And Quality

- Resampling is done with a persistent/stateful FFmpeg resampler, not a fresh resampler per chunk.
- The resample path is CGO-safe and does not pass invalid Go pointer structures into C.
- 44.1 kHz to 48 kHz conversion is supported for the buffered/mix path.
- Resampler state is preserved across audio chunks to avoid boundary artifacts.

## A/V Timing And Sync Ownership

- Audio and video timing come from the shared `core/avsync` timeline.
- Sync/timeline logic is not owned by codec-specific encoders.
- Sync/timeline logic is not duplicated per output codec.
- The same timing layer is intended to support future multi-encoder fanout such as H.264 and H.265.

## RTMP Output Path

- `rawscene` can publish the composed output to RTMP.
- RTMP audio config prefers the real AAC config from the upstream provider/encoder.
- Video and audio can both be present in the published RTMP output.

## Scene Runtime Boundary

- `RawStreamer` currently exposes one normal encoded stream output.
- Higher-level switching and fan-out still happen later in `Streamer` / `MultiCaster`.

## Explicit Non-Goal

- Repeating the same input URL multiple times is not treated as internal fanout.
- Each repeated URL is treated as a separate real input session.

## Suggested Priority For Tests

- Command parsing and layout parsing.
- Raw composition correctness: placement, z-index, transparency.
- Partial readiness and freeze-last-frame behavior.
- Input restart on channel close.
- Input restart on stale session without channel close.
- Generation guard against stale decoder workers.
- Audio ratio validation and stable audio buffering.
- Resample correctness and chunk continuity.
- Shared timeline monotonicity and stable output cadence.
- RTMP output metadata correctness for AAC/H.264.
