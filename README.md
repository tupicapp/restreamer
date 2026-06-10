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
- Assumption: in live HLS mode, `#EXT-X-TARGETDURATION` should not shrink across playlist updates; the destination keeps it monotonic (ceiling of the max segment duration seen so far in the current run) to reduce client-side reload/decoder instability.
- Assumption: live HLS cleanup must never delete playlist files such as `stream.m3u8`; only stale segment/object files are eligible for time-based cleanup, otherwise clients like `ffplay` will fail on playlist reload with HTTP 404.
- Assumption: live HLS `.ts` segment writes carry a fixed storage expiration hint of 1 minute from write time; playlists are written without an expiration hint.
- `core/test/state_test.go` is intentionally a deterministic state-transition test using mock streams; it validates `State()` add/switch/remove/close behavior and URL presence for configured input live/input record folders without depending on external RTMP/HLS runtime availability.
- Per-input background live/record generation is enabled only for non-pausing inputs (currently compat-wrapped RTMP readers) and starts when that input has input live and/or input record folders configured.
- Assumption: served stream URLs are owned by the stream implementation and its injected storage, not by `Streamer`. HLS playlists are written with relative segment URIs, storage can expose a full public base URL, and serve metadata is reported through each stream `State()` (`url`, `local_path`, `serve_type`, `serve_mode`).
- Assumption: `streamfactory.WithStreamServer(...)` still passes sidecar streams through generic construction options, but output implementations in `core/outputs` currently ignore them. Sidecars are effectively input-only for now.
- Assumption: sidecars receive cloned `Frame` objects, not shared pointers. HLS/RTMP writer internals can normalize timestamps, payload headers, and keyframe metadata in place, so sharing a single frame instance across parent and sidecars can freeze or corrupt playlists.
- Assumption: a stream can have multiple sidecars. In that case `State().Served` contains all served endpoints, while the parent stream’s own top-level fields (`url`, `local_path`, `serve_type`, `serve_mode`) remain the parent stream’s own identity.
- Assumption: input sidecars are observers of the underlying input, not of the currently selected route. For non-pausing inputs such as compat-wrapped RTMP readers, sidecars keep receiving real and synthetic media even when that input is not the active switched source.
- Assumption: VOD/file `hlsInput` streams become removable as soon as `#EXT-X-ENDLIST` has been observed and the final known segment queue plus buffered frames have been fully drained. HLS playlists without `#EXT-X-ENDLIST` still fall back to the generic stale-IO removable path.
- Assumption: all streams are wrapped by the internal manager. For non-restartable streams, the manager marks `State().IsRemovable = true` after 5 seconds without IO, and `Streamer` runs a lightweight cleanup ticker that prunes removable streams while preserving the current active input.
- Assumption: if the current active input has no IO for more than 1 second and another healthy input exists, `Streamer` automatically switches to the freshest alternative input before stale outputs are pruned.
- Assumption: the `switch` TUI renders its input list from current `Streamer.State()` instead of the startup spec, so deleted/auto-removed inputs disappear from the terminal view immediately.
- Assumption: live add/remove in `cmd/switch` is CLI-owned. Slash commands such as `/ai`, `/ao`, `/di`, and `/do` build or remove streams through direct `Streamer` APIs (`AddInput`, `AddOutput`, `RemoveInput`, `RemoveOutput`) instead of rebuilding the whole route with `UpdateStreams`.
- Assumption: the `switch` CLI owns sidecar storage layout. Per-stream `--live` / `--record` flags create local HLS sidecars under `.irajstreamer/switch/<route>/<inputs|outputs>/<stream-id>/<live|record>/stream.m3u8`.
- Assumption: the `switch` CLI can start a lightweight local file server for local HLS served paths. This server exists only in the CLI layer and is not part of `core`; it is derived from `Streamer.State()` so library users can implement the same behavior themselves with minimal glue.
- Assumption: `Pipe` is a single-writer, single-reader in-memory bridge. The writer side returned by `AsOutput()` owns closure of the shared bridge, and the reader side returned by `AsInput()` observes end-of-stream by receiving closed channels.
- HLS live output is now real-audio-only at destination level: it does not synthesize repeated AAC packets, does not clamp audio PTS to video PTS, and does not drop audio solely for temporary audio-ahead drift.
- Assumption: live HLS cross-input switch commit is guarded by destination-side video config readiness; the switch is deferred until a compatible H.264 keyframe is available.
- Assumption: `inputs.NewHLS(..., inputs.WithLoop())` replays `#EXT-X-ENDLIST` playlists from the beginning instead of becoming removable, and leaves emitted timestamps unchanged so downstream layers can decide how to handle loop boundaries.
