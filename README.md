# Irajstreamer CLI

This directory now contains a standalone CLI entrypoint for `irajstreamer`.

## Commands

- `switch`: route multiple live inputs into one or more outputs and switch active input live from terminal UI.
- `scene`: existing scene command is also available in the standalone CLI.
- `rawscene`: run one `RawStreamer` backed by the default `Composer` raw processor.

## Run

```bash
go run irajstreamer/main.go switch \
  -i rtmp://127.0.0.1:1938/live/cam1 \
  -i rtmp://127.0.0.1:1938/live/cam2 \
  rtmp://127.0.0.1:1938/live/out1
```

```bash
CGO_CFLAGS= CGO_CPPFLAGS= CGO_CXXFLAGS= CGO_LDFLAGS= CGO_FFLAGS= \
go run -tags 'cgo media' irajstreamer/main.go rawscene \
  --stream-id raw-scene-1 \
  -i rtmp://127.0.0.1:1938/live/cam1 --layout 0,0,640,360 \
  -i rtmp://127.0.0.1:1938/live/cam2 --layout 640,0,640,360 \
  -i rtmp://127.0.0.1:1938/live/cam3 --layout 0,360,640,360 \
  -i rtmp://127.0.0.1:1938/live/cam4 --layout 640,360,640,360 \
  --canvas 1280x720 \
  -o rtmp://127.0.0.1:1938/live/out
```

If your shell exports unrelated CGO flags, prefer:

```bash
make go-media-run ARGS="rawscene --stream-id raw-scene-1 -i rtmp://127.0.0.1:1938/live/1 --layout 0,0,640,360 -i rtmp://127.0.0.1:1938/live/1 --layout 640,0,640,360 -i rtmp://127.0.0.1:1938/live/1 --layout 0,360,640,360 -i rtmp://127.0.0.1:1938/live/1 --layout 640,360,640,360 --canvas 1280x720 -o rtmp://127.0.0.1:1938/live/out --audio-ratio 40 --audio-ratio 20 --audio-ratio 20 --audio-ratio 20"
```

## Assumptions

- `switch` accepts one or more inputs. With one input it behaves as a straight passthrough route; with multiple inputs it also opens the interactive switcher UI.
- Outputs are real destination URLs and can be passed by `-o/--output` and/or as positional args.
- A selected input is routed to all configured outputs (single active route at a time).
- Stream protocol/type detection remains delegated to existing `streamfactory` behavior.
- The live terminal switcher mechanism is intentionally copied into `switch.go` and not imported from the scene command helpers.
- CI only validates the test suite. Version tags and GitHub releases are created manually when needed.
- `RawStreamer` is now the generic raw-domain stream node. Scene composition is one usage of it through the default `Composer` raw processor.
- `RawStreamer` currently exposes one normal encoded stream output; switching and fan-out still happen later in `Streamer`/`MultiCaster`.
- The first raw processor contract is video-focused. Audio remains a separate strategy inside `RawStreamer` with passthrough or AAC mix behavior.
- `RawStreamer` starts composing as soon as at least one input has a decoded frame; missing inputs render as background until they become ready.
- `RawStreamer` currently normalizes composed audio to AAC-LC stereo at `48000 Hz` to match the repo's default RTMP/HLS output expectations.
- Current raw-streamer codec coverage is intentionally narrow: `rawvideo` and `h264` for video ingest, `h264` for video output, and AAC for audio passthrough/mix flows.
