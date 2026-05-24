# Irajstreamer CLI

This directory now contains a standalone CLI entrypoint for `irajstreamer`.

## Commands

- `switch`: route multiple live inputs into one or more outputs and switch active input live from terminal UI.
- `scene`: existing scene command is also available in the standalone CLI.

## Run

```bash
go run irajstreamer/main.go switch \
  -i rtmp://127.0.0.1:1938/live/cam1 \
  -i rtmp://127.0.0.1:1938/live/cam2 \
  rtmp://127.0.0.1:1938/live/out1
```

## Assumptions

- The module is intended to be consumed as a library at `github.com/tupicapp/restreamer`, and downstream code should import packages from that module path.
- `switch` accepts one or more inputs. With one input it behaves as a straight passthrough route; with multiple inputs it also opens the interactive switcher UI.
- Outputs are real destination URLs and can be passed by `-o/--output` and/or as positional args.
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
