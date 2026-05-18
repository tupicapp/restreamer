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

- Switching requires at least two inputs, so `switch` validates `-i` count is `>= 2`.
- Outputs are real destination URLs and can be passed by `-o/--output` and/or as positional args.
- A selected input is routed to all configured outputs (single active route at a time).
- Stream protocol/type detection remains delegated to existing `streamfactory` behavior.
- The live terminal switcher mechanism is intentionally copied into `switch.go` and not imported from the scene command helpers.
