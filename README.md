# Irajstreamer CLI

This directory now contains a standalone CLI entrypoint for `irajstreamer`.

## Commands

- `switch`: route multiple live inputs into one or more outputs and switch active input live from terminal UI.
- `scene`: existing scene command is also available in the standalone CLI.

## Run

```bash
go run main.go switch \
  -i rtmp://127.0.0.1:1938/live/cam1 \
  -i rtmp://127.0.0.1:1938/live/cam2 \
  -o rtmp://127.0.0.1:1938/live/out1
```

## Smooth Switch (CLI)

Use this pattern to switch between mixed inputs (for example HLS + RTMP) and write to multiple outputs:

```bash
go run main.go switch \
  -i http://localhost:8091/milad-nob/milad.m3u8 \
  -i rtmp://127.0.0.1:1938/live/1 \
  -o ./record/stream.m3u8 \
  -o rtmp://localhost:1938/live/out
```

How it behaves:

- `switch` starts all inputs/outputs, activates input 1 first, then opens an interactive switcher when more than one input exists.
- Controls: `↑`/`↓` to move, `Enter` to switch, `q` to quit.
- The selected input is routed to all outputs at the same time.

For smoother switching in production:

- Keep all inputs codec-compatible (same video/audio codecs, stable GOP/keyframe cadence, similar FPS/sample-rate).
- Prefer RTMP publishers that send both audio and video tracks.
- If an RTMP source is audio-only/video-only or temporarily stalls one track, the input compatibility layer fills missing/stalled media with synthetic frames to keep output decodable.
- Keep HLS output in live mode when you need a sliding window:

```bash
go run main.go switch \
  -i http://localhost:8091/milad-nob/milad.m3u8 \
  -i rtmp://127.0.0.1:1938/live/1 \
  --live --segment-duration 2s --playlist-size 6 --clean-interval 10s -o ./record/stream.m3u8 \
  -o rtmp://localhost:1938/live/out
```

## Smooth Switch (Library)

If users integrate as a Go library, the switch flow is:

1. Build input streams with `streamfactory.NewInput`.
2. Build output streams with `streamfactory.NewOutput` and/or `streamfactory.NewHLSOutput`.
3. Call `streamer.UpdateStreams(inputs, outputs)`.
4. Call `streamer.Switch(<input-id>)` for initial input, then call `Switch` again whenever you need to cut.
5. Start with `streamer.StartLife()` + `streamer.Start()`.

## Assumptions

- The module is intended to be consumed as a library at `github.com/tupicapp/restreamer`, and downstream code should import packages from that module path.
- `switch` accepts one or more inputs. With one input it behaves as a straight passthrough route; with multiple inputs it also opens the interactive switcher UI.
- Outputs are real destination URLs and are passed by repeating `-o/--output`.
- A selected input is routed to all configured outputs (single active route at a time).
- Stream protocol/type detection remains delegated to existing `streamfactory` behavior.
- The live terminal switcher mechanism is intentionally copied into `switch.go` and not imported from the scene command helpers.
- CI only validates the test suite. Version tags and GitHub releases are created manually when needed.
- CI fixture bootstrap assumes an RTMP server is reachable on `127.0.0.1:1938`; when `testdata/rtmp` is absent, `make -f MakeFile streaming-infra-up` publishes a synthetic H.264/AAC stream to `rtmp://127.0.0.1:1938/live/1`.
- Integration tests use `HLS_SERVER_URL` when provided and otherwise fall back to the checked-in `testdata/stream.m3u8` fixture served from `http://127.0.0.1:8091`.
- Test fixture filesystem resolution is anchored to the repository root derived from `core/test` source location (`runtime.Caller`), and local fixture checks use relative-path constants such as `testHLSFixtureRelativePath` instead of current-working-directory discovery.
- `TestAll` startup auto-publishes compatibility RTMP fixtures `audio-less` and `video-less` from `testdata/minion.mp4` using dedicated ffmpeg loops when those streams are not already reachable.
- HLS live-oriented integration tests use deterministic local fixtures generated from `testdata/stream.m3u8` instead of relying on an external live playlist.
- The switch integration test only validates switching behavior when at least two inputs actually start; if fixture availability collapses it to a single input, the test skips instead of asserting on a non-switching timeline.
- Live HLS passthrough/window tests treat video payload identity as strict, but compare audio windows by stable overlap, frame coverage, and timeline coverage instead of requiring near-perfect AAC packet identity across remuxing on every CI runner.
- RTMP timing tests normalize away isolated timestamp outliers before checking elapsed-time windows, because some RTMP readers can surface a stray zero-based frame alongside the live timeline on slower runners.
- `switch` wraps RTMP inputs in a compatibility adapter after stream construction. Track presence is learned only from RTMP `initTracks()`, and missing or stalled tracks are filled with synthetic media at the input boundary so existing destinations can keep their current assumptions.
- HLS output now marks explicit switch boundaries (`InputID` changes) with `#EXT-X-DISCONTINUITY` to improve mixed-source (for example HLS<->RTMP) player stability during live switching.
- HLS output now caches H.264 SPS/PPS per input and only commits a switch on a decodable keyframe from the new source. It marks the next segment with `#EXT-X-DISCONTINUITY`, but it does not reject a source locally after the upstream switch has already happened; mixed-format normalization still needs to happen before HLS output if seamless playback across incompatible sources is required.
- `core/test/state_test.go` is intentionally a deterministic state-transition test using mock streams; it validates `State()` add/switch/remove/close behavior and URL presence for configured program-live/program-record folders without depending on external RTMP/HLS runtime availability.
- Per-input background program live/record generation is enabled only for non-pausing inputs (currently compat-wrapped RTMP readers) and starts when that input has program live and/or record folders configured.
