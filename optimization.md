# Optimization and Simplification Priorities

1. Split `Streamer` responsibilities
- `core/streamer.go` currently owns routing, switching, playlist state, recording state, folders, and events.
- Make it a thin orchestrator and move each concern into small services (`router`, `switcher`, `playlist`, `recorder`, `eventbus`).

2. Reduce `Stream` interface size
- `core/shared/models.go` `Stream` mixes media I/O + lifecycle + restart policy + events.
- Split into smaller interfaces (`MediaIO`, `Lifecycle`, `Restartable`, `EventSource`) so implementations are simpler and easier to test.

3. Replace string-based protocol detection with registry
- `core/streamfactory/factory.go` has protocol logic in `detect*Kind` + switches.
- Use a protocol registry (matcher + constructor). This removes branching and makes adding RTSP/SRT/etc predictable.

4. Break up `helpers.go` “god file”
- `core/helpers.go` contains many unrelated utilities and `//nolint:all`.
- Split by concern (`ffmpeg_probe.go`, `codec_validation.go`, `frame_builders.go`, `media_info.go`).

5. Simplify multicaster concurrency model
- `core/multicast.go` starts goroutines per output per frame.
- Move to fixed worker loops per output or non-blocking fanout with bounded queues. Easier reasoning, fewer race/perf edge cases.

6. Unify input/output adapter duplication
- `core/inputs/*` and `core/outputs/*` have many protocol-specific files.
- Introduce a shared adapter pattern with a common base for start/stop/state/event handling to reduce repeated plumbing.

7. Simplify test layers
- Keep fast unit tests in module dirs and a small, explicit integration suite in `core/test`.
- This gives clearer guarantees and lowers maintenance cost.
