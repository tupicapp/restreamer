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
- HLS live-oriented integration tests use deterministic local fixtures generated from `testdata/stream.m3u8` instead of relying on an external live playlist.
- The switch integration test only validates switching behavior when at least two inputs actually start; if fixture availability collapses it to a single input, the test skips instead of asserting on a non-switching timeline.
