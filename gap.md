# Runtime Gaps Compared To FFmpeg

This file records runtime behaviors handled by FFmpeg that are not yet fully
modeled in this project, or are only handled partially.

The purpose is to keep the missing cases explicit before more streaming
features are built on top of `RawStreamer`, `Streamer`, and future multi-output
encoder fan-out.

## Context

The comparison here is based on the local FFmpeg mirror in:

- `fftools/ffplay.c`
- `fftools/ffmpeg_dec.c`
- `fftools/ffmpeg_enc.c`
- `fftools/ffmpeg_mux.c`

## Already Covered Reasonably

These areas are already reflected in the current design at a basic level:

- shared pre-encoder timeline in `core/avsync`
- stable output cadence independent from single-input stalls
- freeze-last-video-frame behavior for missing inputs
- silence-fill / stable buffered audio cadence
- per-input stale-session restart for restartable inputs
- generation guard to prevent old decoder workers from updating revived inputs

## Gaps

## 1. Stream Generation Flush Semantics

FFmpeg uses packet serials and flush behavior so old packets and frames from a
previous stream generation are ignored after restart, seek, or queue reset.

Relevant FFmpeg areas:

- `ffplay.c`: packet queue serial changes and decoder flush behavior
- `ffplay.c`: decoder resets `next_pts`, `finished`, and codec buffers on serial change

What we have:

- `RawStreamer` has a generation guard for decoded outputs

What is still missing:

- explicit flush semantics for buffered per-input packet/audio/video state
- formal reset behavior for all intermediate buffers when a session generation changes
- a single consistent “new generation began” transition across all per-input subsystems

Why it matters:

- stale buffered audio or packets can leak across a restart boundary
- restart correctness becomes harder when more buffering layers are added later

## 2. Mid-Stream Format Change Reconfiguration

FFmpeg reconfigures runtime graphs when decoded media properties change.

Relevant FFmpeg areas:

- `ffplay.c`: audio filter graph rebuild on sample rate / sample format / channel layout change
- `ffplay.c`: video filter graph rebuild on width / height / pixel format change

What we have:

- decoders and resamplers are configured once per session

What is still missing:

- input-side detection for:
  - audio sample rate changes
  - audio channel layout changes
  - audio sample format changes
  - video resolution changes
  - video pixel format changes
- controlled teardown and rebuild of dependent runtime components

Why it matters:

- revived or replaced inputs may come back with different media properties
- adaptive/live pipelines can change encoding profile or resolution at runtime

## 3. Timestamp Gap And Discontinuity Reset Handling

FFmpeg explicitly resets timestamp conversion state when it detects a gap or
timestamp discontinuity.

Relevant FFmpeg areas:

- `ffmpeg_dec.c`: audio timestamp processing resets delta conversion state on gaps

What we have:

- a stable shared output timeline

What is still missing:

- explicit discontinuity detection at input/decode boundaries
- reset of audio resample/timestamp conversion state after large gaps
- explicit handling for large source timestamp jumps, not only source silence

Why it matters:

- gap recovery after reconnect can still carry stale timing assumptions
- long pauses or source restarts can corrupt sync if conversion state is preserved blindly

## 4. Missing-PTS Recovery With Duration History

FFmpeg uses `best_effort_timestamp` when available and extrapolates timestamps
from the previous duration when PTS is missing.

Relevant FFmpeg areas:

- `ffmpeg_dec.c`: `frame->best_effort_timestamp`
- `ffmpeg_dec.c`: video duration estimation and missing-PTS extrapolation

What we have:

- output-side rebasing through `core/avsync`

What is still missing:

- richer source-side timing history before rebasing
- a consistent strategy for weak or partially missing source timestamps
- source timing quality metadata that later stages can use

Why it matters:

- not every input restart or source type has clean timestamps
- decoder-side timing quality still affects sync stability before rebasing

## 5. Mux-Side Timestamp Repair

FFmpeg repairs invalid timestamp conditions at mux time.

Relevant FFmpeg areas:

- `ffmpeg_mux.c`: invalid `DTS > PTS` repair
- `ffmpeg_mux.c`: non-monotonic DTS clamp/fixup
- `ffmpeg_mux.c`: audio rescale precision handling during streamcopy

What we have:

- shared timing generation before encode

What is still missing:

- output-layer monotonicity enforcement
- last-muxed timestamp tracking with correction policy
- explicit repair of bad packet ordering before final write

Why it matters:

- mux boundaries are the last safe place to catch timing regressions
- future multi-encoder / multi-output fan-out increases timestamp repair importance

## 6. Runtime Frame Drop / Frame Duplication Policy

FFmpeg actively drops or duplicates frames when timing drift requires it.

Relevant FFmpeg areas:

- `ffplay.c`: `compute_target_delay()`
- `ffplay.c`: early/late frame drop accounting

What we have:

- freeze-last-frame on missing input
- fixed output cadence

What is still missing:

- explicit late-frame drop policy when processing or encode falls behind
- explicit duplicate policy when a cadence correction is better than waiting
- metrics that distinguish:
  - source stall
  - compositor lag
  - encoder lag

Why it matters:

- stable cadence alone does not solve overload behavior
- live systems need predictable degradation under pressure

## 7. Audio Sample Count Sync Correction

FFmpeg does not only rely on clocks. It can slightly adjust the number of audio
samples used for playback to pull A/V sync back toward the master clock.

Relevant FFmpeg areas:

- `ffplay.c`: `synchronize_audio()`

What we have:

- buffered audio cadence
- silence-fill on missing audio

What is still missing:

- bounded audio sample count correction
- a policy for when to stretch/shrink audio slightly versus keep strict cadence

Why it matters:

- small drift corrections are often cleaner than coarse discontinuous fixes
- this becomes more important under long-running live operation

## 8. Queue Fullness And Backpressure Policy

FFmpeg has explicit queue-size-based backpressure and reading behavior.

Relevant FFmpeg areas:

- `ffplay.c`: queue fullness checks
- `ffplay.c`: conditional wait when enough packets are already buffered

What we have:

- local bounded channels and some drop behavior

What is still missing:

- a whole-pipeline backpressure policy
- queue watermarks and coordinated producer throttling
- explicit distinction between:
  - drop because downstream is slow
  - hold because buffering is healthy

Why it matters:

- multi-input and future multi-output graphs need predictable buffering rules
- otherwise latency and frame dropping become accidental rather than designed

## 9. EOF / Drain Signaling

FFmpeg uses explicit null-packet / EOF signaling so downstream decode and mux
stages can drain correctly.

Relevant FFmpeg areas:

- `ffplay.c`: null packets on EOF
- `ffmpeg_dec.c`: downstream EOF timestamp signaling

What we have:

- channel closure and session replacement

What is still missing:

- formal end-of-stream signaling between pipeline stages
- explicit drain behavior for encoders and future mux fan-out stages

Why it matters:

- correct draining becomes important for file outputs, HLS, records, and clean teardown

## 10. Realtime External Clock Adaptation

FFmpeg adapts external clock speed for realtime streams based on queue fullness.

Relevant FFmpeg areas:

- `ffplay.c`: external clock speed adjustment for realtime playback

What we have:

- fixed shared timeline for output generation

What is still missing:

- adaptive clock policy tied to buffer health
- queue-aware timing adjustment for realtime ingest/output pressure

Why it matters:

- a purely fixed clock is simple, but not always optimal under realtime backlog changes

## Prioritized Next Gaps

If these are addressed incrementally, the recommended order is:

1. mux-side timestamp repair
2. explicit discontinuity detection and generation-boundary flush/reset
3. mid-stream format change reconfiguration
4. queue/backpressure policy
5. frame drop / duplication policy

## Assumptions

- This gap list is focused on runtime behavior, not UI/player-only behavior.
- Some FFmpeg logic is playback-oriented (`ffplay`) rather than restreamer-oriented,
  but the clocking, queueing, discontinuity, and serial-generation ideas still map
  directly to a live stream graph.
- We are intentionally not copying FFmpeg structure directly; the goal is to keep
  the architecture modular while borrowing the runtime ideas that matter.
