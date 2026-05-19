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
  -i rtmp://127.0.0.1:1938/live/cam1 --layout 0,0,1280,720,0,0 \
  -i rtmp://127.0.0.1:1938/live/cam2 --layout 880,40,360,200,10,0.20 \
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
- A shared pre-encoder timeline in `core/avsync` owns composed stream timing so audio/video synchronization does not depend on any single encoder implementation. This is intended to stay reusable when one raw stream later fans out to multiple encoded variants such as H.264 and H.265.
- The first raw processor contract is video-focused. Audio remains a separate strategy inside `RawStreamer` with passthrough or AAC mix behavior.
- `RawStreamer` starts composing as soon as at least one input has a decoded frame; missing inputs render as background until they become ready.
- `RawStreamer` keeps the output clock stable even when inputs stall: video reuses each input's last decoded frame, and the buffered AAC path pads missing audio samples with silence instead of letting input timing stall encoder output.
- Each `RawStreamer` input is supervised independently with a minimal `initial/live/dead` lifecycle. If an input session ends or stops producing IO past its restart interval, the slot keeps its last decoded video frame while a replacement session is attached; when fresh frames arrive again for that logical input, the slot resumes live updates automatically without disturbing the output encoder clock.
- `RawStreamer` now keeps audio output stable through a buffered AAC encode path for both selected-input audio and mixed audio. When resampling is required, it normalizes to AAC-LC stereo at `48000 Hz` using a stateful per-input FFmpeg `swresample` pipeline with an explicit high-quality profile instead of per-frame default conversion.
- Current raw-streamer codec coverage is intentionally narrow: `rawvideo` and `h264` for video ingest, `h264` for video output, and AAC for audio passthrough/mix flows.
- Scene layouts support optional `z-index` and transparency in both `scene` and `rawscene`: `x,y,width,height[,z[,transparency]]`. `RawStreamer` passes those layout values through unchanged, and the default `Composer` applies the layer order and blending.
