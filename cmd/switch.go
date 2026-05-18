package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	core "restreamer/core"
	"restreamer/core/streamfactory"

	"github.com/spf13/cobra"
)

const defaultHLSCleanInterval = 10 * time.Second

type switchCommandOptions struct {
	routeID        string
	inputs         []string
	outputs        []string
	startupTimeout time.Duration
}

type switchBuildResult struct {
	routeID        string
	inputURLs      []string
	outputURLs     []string
	startupTimeout time.Duration
}

func buildSwitchSpec(opts switchCommandOptions, extraOutputs []string) (switchBuildResult, error) {
	allOutputs := append(opts.outputs, extraOutputs...)
	if len(opts.inputs) < 2 {
		return switchBuildResult{}, fmt.Errorf("at least two -i inputs are required for switching")
	}
	if len(allOutputs) == 0 {
		return switchBuildResult{}, fmt.Errorf("at least one -o output is required")
	}
	return switchBuildResult{
		routeID:        opts.routeID,
		inputURLs:      opts.inputs,
		outputURLs:     allOutputs,
		startupTimeout: opts.startupTimeout,
	}, nil
}

func shouldShowSwitchHelp(opts switchCommandOptions, extraArgs []string) bool {
	return len(opts.inputs) == 0 && len(extraArgs) == 0
}

type inputSpec struct {
	url string
}

type outputSpec struct {
	url                string
	hlsLive            bool
	hlsSegmentDuration time.Duration
	hlsPlaylistSize    int
	hlsCleanInterval   time.Duration
}

type switchSpec struct {
	routeID        string
	startupTimeout time.Duration
	inputs         []inputSpec
	outputs        []outputSpec
}

type switchEntry struct {
	id   string
	name string
}

func NewSwitchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "switch [global-flags] [stream-flags -i <url>]... [stream-flags -o <url>]...",
		Short:              "Route multiple live inputs to one or more outputs with interactive switching",
		DisableFlagParsing: true,
		Long: "Start a passthrough router with multiple inputs and one or more outputs, then switch the active input live.\n\n" +
			"Global flags (apply to the whole command):\n" +
			"  --route-id <id>              Route stream ID prefix (default: route-1)\n" +
			"  --startup-timeout <duration> Max time to wait for streams to start (default: 30s)\n\n" +
			"Per-output flags (place immediately before -o):\n" +
			"  -l, --live                   HLS live/sliding-window mode\n" +
			"  --segment-duration <dur>     HLS segment duration, e.g. 4s (default 2s)\n" +
			"  --playlist-size <n>          Sliding-window segment count, live mode only (default 6)\n" +
			"  --clean-interval <dur>       How often stale segments are removed (default 10s)\n\n" +
			"Example:\n" +
			"  irajstreamer switch \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam1 \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam2 \\\n" +
			"    --live --segment-duration 4s -o ./hls/stream.m3u8 \\\n" +
			"    -o rtmp://127.0.0.1:1938/live/out",
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, a := range args {
				if a == "-h" || a == "--help" {
					return cmd.Help()
				}
			}
			spec, err := parseSwitchArgs(args)
			if err != nil {
				return err
			}
			if len(spec.inputs) == 0 && len(spec.outputs) == 0 {
				return cmd.Help()
			}
			return runSwitchCommand(cmd.Context(), spec)
		},
	}
	return cmd
}

func parseSwitchArgs(args []string) (switchSpec, error) {
	spec := switchSpec{
		routeID:        "route-1",
		startupTimeout: 30 * time.Second,
	}

	var pendingHLSLive bool
	var pendingSegmentDuration time.Duration
	var pendingPlaylistSize int
	var pendingCleanInterval time.Duration

	resetPending := func() {
		pendingHLSLive = false
		pendingSegmentDuration = 0
		pendingPlaylistSize = 0
		pendingCleanInterval = 0
	}

	consume := func(i int, flag string) (string, int, error) {
		if i+1 >= len(args) {
			return "", i, fmt.Errorf("%s requires a value", flag)
		}
		return args[i+1], i + 1, nil
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		switch {
		// ── global flags ────────────────────────────────────────────────────
		case arg == "--route-id":
			v, next, err := consume(i, arg)
			if err != nil {
				return switchSpec{}, err
			}
			spec.routeID = v
			i = next

		case strings.HasPrefix(arg, "--route-id="):
			spec.routeID = strings.TrimPrefix(arg, "--route-id=")

		case arg == "--startup-timeout":
			v, next, err := consume(i, arg)
			if err != nil {
				return switchSpec{}, err
			}
			d, err := time.ParseDuration(v)
			if err != nil {
				return switchSpec{}, fmt.Errorf("--startup-timeout: %w", err)
			}
			spec.startupTimeout = d
			i = next

		case strings.HasPrefix(arg, "--startup-timeout="):
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--startup-timeout="))
			if err != nil {
				return switchSpec{}, fmt.Errorf("--startup-timeout: %w", err)
			}
			spec.startupTimeout = d

		// ── per-stream flags (HLS output) ────────────────────────────────────
		case arg == "-l" || arg == "--live":
			pendingHLSLive = true

		case arg == "--segment-duration":
			v, next, err := consume(i, arg)
			if err != nil {
				return switchSpec{}, err
			}
			d, err := time.ParseDuration(v)
			if err != nil {
				return switchSpec{}, fmt.Errorf("--segment-duration: %w", err)
			}
			pendingSegmentDuration = d
			i = next

		case strings.HasPrefix(arg, "--segment-duration="):
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--segment-duration="))
			if err != nil {
				return switchSpec{}, fmt.Errorf("--segment-duration: %w", err)
			}
			pendingSegmentDuration = d

		case arg == "--playlist-size":
			v, next, err := consume(i, arg)
			if err != nil {
				return switchSpec{}, err
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				return switchSpec{}, fmt.Errorf("--playlist-size: %w", err)
			}
			pendingPlaylistSize = n
			i = next

		case strings.HasPrefix(arg, "--playlist-size="):
			n, err := strconv.Atoi(strings.TrimPrefix(arg, "--playlist-size="))
			if err != nil {
				return switchSpec{}, fmt.Errorf("--playlist-size: %w", err)
			}
			pendingPlaylistSize = n

		case arg == "--clean-interval":
			v, next, err := consume(i, arg)
			if err != nil {
				return switchSpec{}, err
			}
			d, err := time.ParseDuration(v)
			if err != nil {
				return switchSpec{}, fmt.Errorf("--clean-interval: %w", err)
			}
			pendingCleanInterval = d
			i = next

		case strings.HasPrefix(arg, "--clean-interval="):
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--clean-interval="))
			if err != nil {
				return switchSpec{}, fmt.Errorf("--clean-interval: %w", err)
			}
			pendingCleanInterval = d

		// ── stream anchors ───────────────────────────────────────────────────
		case arg == "-i" || arg == "--input":
			url, next, err := consume(i, arg)
			if err != nil {
				return switchSpec{}, err
			}
			url = strings.TrimSpace(url)
			if url == "" {
				return switchSpec{}, fmt.Errorf("-i URL cannot be empty")
			}
			spec.inputs = append(spec.inputs, inputSpec{url: url})
			resetPending()
			i = next

		case arg == "-o" || arg == "--output":
			url, next, err := consume(i, arg)
			if err != nil {
				return switchSpec{}, err
			}
			url = strings.TrimSpace(url)
			if url == "" {
				return switchSpec{}, fmt.Errorf("-o URL cannot be empty")
			}
			cleanInterval := pendingCleanInterval
			if cleanInterval <= 0 && pendingHLSLive {
				cleanInterval = defaultHLSCleanInterval
			}
			spec.outputs = append(spec.outputs, outputSpec{
				url:                url,
				hlsLive:            pendingHLSLive,
				hlsSegmentDuration: pendingSegmentDuration,
				hlsPlaylistSize:    pendingPlaylistSize,
				hlsCleanInterval:   cleanInterval,
			})
			resetPending()
			i = next

		default:
			return switchSpec{}, fmt.Errorf("unknown argument: %q", arg)
		}
	}

	if err := validateSwitchSpec(spec); err != nil {
		return switchSpec{}, err
	}
	return spec, nil
}

func validateSwitchSpec(spec switchSpec) error {
	if spec.startupTimeout <= 0 {
		return fmt.Errorf("--startup-timeout must be greater than zero")
	}
	if len(spec.inputs) < 2 {
		return fmt.Errorf("at least two -i inputs are required for switching")
	}
	if len(spec.outputs) == 0 {
		return fmt.Errorf("at least one -o output is required")
	}
	return nil
}

func runSwitchCommand(parent context.Context, spec switchSpec) error {
	if parent == nil {
		parent = context.Background()
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	created := make([]core.Stream, 0, len(spec.inputs)+len(spec.outputs))
	cleanup := func() {
		for _, stream := range created {
			if stream != nil {
				stream.Close()
			}
		}
	}
	defer cleanup()

	inputStreams := make([]core.Stream, 0, len(spec.inputs))
	entries := make([]switchEntry, 0, len(spec.inputs))
	for idx, in := range spec.inputs {
		streamID := fmt.Sprintf("%s-in-%d", spec.routeID, idx+1)
		stream, err := streamfactory.NewInput(streamID, in.url)
		if err != nil {
			return fmt.Errorf("create input %d: %w", idx+1, err)
		}
		created = append(created, stream)
		inputStreams = append(inputStreams, stream)
		entries = append(entries, switchEntry{id: streamID, name: fmt.Sprintf("Input %d", idx+1)})
	}

	outputStreams := make([]core.Stream, 0, len(spec.outputs))
	for idx, out := range spec.outputs {
		streamID := fmt.Sprintf("%s-out-%d", spec.routeID, idx+1)
		var stream core.Stream
		var err error
		if streamfactory.IsHLSOutputPath(out.url) {
			opts := streamfactory.HLSOutputOptions{
				IsLive:          out.hlsLive,
				SegmentDuration: out.hlsSegmentDuration,
				PlaylistSize:    out.hlsPlaylistSize,
				CleanInterval:   out.hlsCleanInterval,
			}
			stream, err = streamfactory.NewHLSOutput(streamID, out.url, opts)
		} else {
			stream, err = streamfactory.NewOutput(streamID, out.url)
		}
		if err != nil {
			return fmt.Errorf("create output %d: %w", idx+1, err)
		}
		created = append(created, stream)
		outputStreams = append(outputStreams, stream)
	}

	streamer := core.NewStreamer(true, true, true)
	streamer.StartLife()
	defer streamer.Close()

	if err := streamer.UpdateStreams(inputStreams, outputStreams); err != nil {
		return fmt.Errorf("update streams: %w", err)
	}
	if ok := streamer.Switch(entries[0].id); !ok {
		return fmt.Errorf("failed to activate initial input %q", entries[0].name)
	}

	streamer.Start()

	waitCtx, waitCancel := context.WithTimeout(ctx, spec.startupTimeout)
	defer waitCancel()

	for idx, input := range inputStreams {
		if err := input.WaitForStart(waitCtx); err != nil {
			return fmt.Errorf("input %d failed to start: %w", idx+1, err)
		}
	}
	for idx, output := range outputStreams {
		if err := output.WaitForStart(waitCtx); err != nil {
			return fmt.Errorf("output %d failed to start: %w", idx+1, err)
		}
	}

	return runRouteSwitcherTUI(ctx, entries, streamer, len(outputStreams))
}

func runRouteSwitcherTUI(ctx context.Context, entries []switchEntry, streamer *core.Streamer, outputCount int) error {
	if err := switchSetTermRaw(); err != nil {
		<-ctx.Done()
		return nil
	}
	defer switchSetTermNormal()

	activeIdx := 0
	selectedIdx := 0
	firstRender := true
	totalLines := len(entries) + 6

	render := func() {
		if !firstRender {
			fmt.Printf("\x1b[%dA\x1b[J", totalLines)
		}
		firstRender = false

		fmt.Print("\r\n")
		fmt.Print("\x1b[1;36m  ┌──────────────────────────────────────┐\r\n")
		fmt.Print("  │          ROUTE  SWITCHER             │\r\n")
		fmt.Print("  └──────────────────────────────────────┘\x1b[0m\r\n")
		fmt.Printf("  Inputs: %d   Outputs: %d\r\n", len(entries), outputCount)
		for i, e := range entries {
			cursor := "   "
			nameStyle := "\x1b[0m"
			liveTag := ""
			if i == selectedIdx {
				cursor = "\x1b[1;33m ▶ \x1b[0m"
			}
			if i == activeIdx {
				nameStyle = "\x1b[1m"
				liveTag = "  \x1b[1;32m● LIVE\x1b[0m"
			}
			fmt.Printf("%s%s%d.  %s%s\r\n", cursor, nameStyle, i+1, e.name, liveTag)
		}
		fmt.Print("\x1b[90m  ↑ ↓  navigate    Enter  switch    q  quit\x1b[0m\r\n")
	}

	render()

	inputCh := make(chan []byte, 16)
	go func() {
		buf := make([]byte, 16)
		for {
			n, err := os.Stdin.Read(buf)
			if err != nil {
				close(inputCh)
				return
			}
			if n > 0 {
				b := make([]byte, n)
				copy(b, buf[:n])
				inputCh <- b
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\x1b[%dA\x1b[J", totalLines)
			return nil
		case b, ok := <-inputCh:
			if !ok {
				return nil
			}
			redraw := false
			switch {
			case len(b) == 1 && (b[0] == 3 || b[0] == 'q' || b[0] == 'Q'):
				fmt.Printf("\x1b[%dA\x1b[J", totalLines)
				return nil
			case len(b) == 3 && b[0] == 27 && b[1] == '[' && b[2] == 'A':
				if selectedIdx > 0 {
					selectedIdx--
					redraw = true
				}
			case len(b) == 3 && b[0] == 27 && b[1] == '[' && b[2] == 'B':
				if selectedIdx < len(entries)-1 {
					selectedIdx++
					redraw = true
				}
			case len(b) == 1 && (b[0] == 13 || b[0] == 10):
				if selectedIdx != activeIdx && streamer.Switch(entries[selectedIdx].id) {
					activeIdx = selectedIdx
				}
				redraw = true
			}
			if redraw {
				render()
			}
		}
	}
}

func switchSetTermRaw() error {
	cmd := exec.Command("stty", "raw", "-echo")
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func switchSetTermNormal() {
	cmd := exec.Command("stty", "sane")
	cmd.Stdin = os.Stdin
	_ = cmd.Run()
}
