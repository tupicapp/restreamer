package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	core "restreamer/irajstreamer/core"
	"restreamer/irajstreamer/core/streamfactory"

	"github.com/spf13/cobra"
)

const defaultHLSCleanInterval = 10 * time.Second

type switchCommandOptions struct {
	routeID             string
	inputs              []string
	outputs             []string
	startupTimeout      time.Duration
	hlsLive             bool
	hlsSegmentDuration  time.Duration
	hlsPlaylistSize     int
	hlsCleanInterval    time.Duration
}

type switchSpec struct {
	routeID             string
	inputURLs           []string
	outputURLs          []string
	startupTimeout      time.Duration
	hlsLive             bool
	hlsSegmentDuration  time.Duration
	hlsPlaylistSize     int
	hlsCleanInterval    time.Duration
}

type switchEntry struct {
	id   string
	name string
}

func NewSwitchCommand() *cobra.Command {
	opts := switchCommandOptions{}

	cmd := &cobra.Command{
		Use:   "switch -i <input> [-i <input> ...] <output> [output ...]",
		Short: "Route multiple live inputs to one or more outputs with interactive switching",
		Long: "Start a passthrough router with multiple inputs and one or more outputs, then switch the active input live from an interactive terminal UI.\n\n" +
			"Example:\n" +
			"  go run irajstreamer/main.go switch \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam1 \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam2 \\\n" +
			"    rtmp://127.0.0.1:1938/live/out1 rtmp://127.0.0.1:1938/live/out2",
		RunE: func(cmd *cobra.Command, args []string) error {
			if shouldShowSwitchHelp(opts, args) {
				return cmd.Help()
			}

			spec, err := buildSwitchSpec(opts, args)
			if err != nil {
				return err
			}

			return runSwitchCommand(cmd.Context(), spec)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.routeID, "route-id", "route-1", "Route stream ID prefix")
	flags.StringSliceVarP(&opts.inputs, "input", "i", nil, "Input URL. Repeat for each input")
	flags.StringSliceVarP(&opts.outputs, "output", "o", nil, "Output URL. Repeat for each output (or pass outputs as positional args)")
	flags.DurationVar(&opts.startupTimeout, "startup-timeout", 30*time.Second, "Maximum time to wait for all streams to start")
	flags.BoolVarP(&opts.hlsLive, "live", "l", false, "HLS live mode: sliding window playlist with segment cleanup (default is record/VOD)")
	flags.DurationVar(&opts.hlsSegmentDuration, "segment-duration", 0, "HLS segment duration, e.g. 4s (default 2s)")
	flags.IntVar(&opts.hlsPlaylistSize, "playlist-size", 0, "HLS sliding window size in segments, live mode only (default 6)")
	flags.DurationVar(&opts.hlsCleanInterval, "clean-interval", defaultHLSCleanInterval, "How often stale HLS segments are removed from disk, live mode only")

	return cmd
}

func shouldShowSwitchHelp(opts switchCommandOptions, args []string) bool {
	return len(opts.inputs) == 0 && len(opts.outputs) == 0 && len(args) == 0
}

func buildSwitchSpec(opts switchCommandOptions, args []string) (switchSpec, error) {
	if opts.startupTimeout <= 0 {
		return switchSpec{}, fmt.Errorf("--startup-timeout must be greater than zero")
	}

	cleanInputs := make([]string, 0, len(opts.inputs))
	for idx, input := range opts.inputs {
		trimmed := strings.TrimSpace(input)
		if trimmed == "" {
			return switchSpec{}, fmt.Errorf("--input %d cannot be empty", idx+1)
		}
		cleanInputs = append(cleanInputs, trimmed)
	}
	if len(cleanInputs) < 2 {
		return switchSpec{}, fmt.Errorf("at least two --input values are required for switching")
	}

	outputCandidates := make([]string, 0, len(opts.outputs)+len(args))
	outputCandidates = append(outputCandidates, opts.outputs...)
	outputCandidates = append(outputCandidates, args...)

	cleanOutputs := make([]string, 0, len(outputCandidates))
	for idx, output := range outputCandidates {
		trimmed := strings.TrimSpace(output)
		if trimmed == "" {
			return switchSpec{}, fmt.Errorf("output %d cannot be empty", idx+1)
		}
		cleanOutputs = append(cleanOutputs, trimmed)
	}
	if len(cleanOutputs) == 0 {
		return switchSpec{}, fmt.Errorf("at least one output URL is required")
	}

	routeID := strings.TrimSpace(opts.routeID)
	if routeID == "" {
		routeID = "route-1"
	}

	return switchSpec{
		routeID:            routeID,
		inputURLs:          cleanInputs,
		outputURLs:         cleanOutputs,
		startupTimeout:     opts.startupTimeout,
		hlsLive:            opts.hlsLive,
		hlsSegmentDuration: opts.hlsSegmentDuration,
		hlsPlaylistSize:    opts.hlsPlaylistSize,
		hlsCleanInterval:   opts.hlsCleanInterval,
	}, nil
}

// switchHLSOptions returns non-nil HLS options only when the user explicitly
// set any HLS flag. A nil return means "use auto-detection only".
func switchHLSOptions(spec switchSpec) *streamfactory.HLSOutputOptions {
	if !spec.hlsLive && spec.hlsSegmentDuration == 0 && spec.hlsPlaylistSize == 0 {
		return nil
	}
	cleanInterval := spec.hlsCleanInterval
	if cleanInterval <= 0 {
		cleanInterval = defaultHLSCleanInterval
	}
	return &streamfactory.HLSOutputOptions{
		IsLive:          spec.hlsLive,
		SegmentDuration: spec.hlsSegmentDuration,
		PlaylistSize:    spec.hlsPlaylistSize,
		CleanInterval:   cleanInterval,
	}
}

func runSwitchCommand(parent context.Context, spec switchSpec) error {
	if parent == nil {
		parent = context.Background()
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	created := make([]core.Stream, 0, len(spec.inputURLs)+len(spec.outputURLs))
	cleanup := func() {
		for _, stream := range created {
			if stream != nil {
				stream.Close()
			}
		}
	}
	defer cleanup()

	inputStreams := make([]core.Stream, 0, len(spec.inputURLs))
	entries := make([]switchEntry, 0, len(spec.inputURLs))
	for idx, inputURL := range spec.inputURLs {
		streamID := fmt.Sprintf("%s-in-%d", spec.routeID, idx+1)
		stream, err := streamfactory.NewInput(streamID, inputURL)
		if err != nil {
			return fmt.Errorf("create input %d: %w", idx+1, err)
		}
		created = append(created, stream)
		inputStreams = append(inputStreams, stream)
		entries = append(entries, switchEntry{id: streamID, name: fmt.Sprintf("Input %d", idx+1)})
	}

	hlsOpts := switchHLSOptions(spec)
	outputStreams := make([]core.Stream, 0, len(spec.outputURLs))
	for idx, outputURL := range spec.outputURLs {
		streamID := fmt.Sprintf("%s-out-%d", spec.routeID, idx+1)
		var stream core.Stream
		var err error
		if streamfactory.IsHLSOutputPath(outputURL) {
			opts := streamfactory.HLSOutputOptions{}
			if hlsOpts != nil {
				opts = *hlsOpts
			}
			stream, err = streamfactory.NewHLSOutput(streamID, outputURL, opts)
		} else {
			stream, err = streamfactory.NewOutput(streamID, outputURL)
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
