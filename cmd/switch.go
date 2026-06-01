package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	core "github.com/tupicapp/restreamer/core"
	"github.com/tupicapp/restreamer/core/streamfactory"

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
	if len(opts.inputs) == 0 {
		return switchBuildResult{}, fmt.Errorf("at least one -i input is required")
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
	url      string
	sidecars []switchSidecarSpec
}

type outputSpec struct {
	url                string
	hlsLive            bool
	hlsSegmentDuration time.Duration
	hlsPlaylistSize    int
	hlsCleanInterval   time.Duration
	sidecars           []switchSidecarSpec
}

type switchSidecarSpec struct {
	mode            string
	segmentDuration time.Duration
	playlistSize    int
	cleanInterval   time.Duration
}

type switchFileServer struct {
	server  *http.Server
	baseURL string
	rootDir string
}

type switchServedPath struct {
	Role         string
	OwnerID      string
	ServedID     string
	ServeType    string
	ServeMode    string
	PlaylistPath string
	Directory    string
	HTTPURL      string
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

type switchRuntime struct {
	routeID         string
	startupTimeout  time.Duration
	streamer        *core.Streamer
	fileServer      *switchFileServer
	namesByID       map[string]string
	inputSpecs      map[string]inputSpec
	outputSpecs     map[string]outputSpec
	nextInputIndex  int
	nextOutputIndex int
}

func NewSwitchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "switch [global-flags] [stream-flags -i <url>]... [stream-flags -o <url>]...",
		Short:              "Route one or more live inputs to one or more outputs with optional switching",
		DisableFlagParsing: true,
		Long: "Start a passthrough router with one or more inputs and one or more outputs. When multiple inputs are provided, the active input can be switched live.\n\n" +
			"Global flags (apply to the whole command):\n" +
			"  --route-id <id>              Route stream ID prefix (default: route-1)\n" +
			"  --startup-timeout <duration> Max time to wait for streams to start (default: 30s)\n\n" +
			"Per-stream sidecar flags (place immediately before -i or -o):\n" +
			"  -l, --live                   Attach a live HLS sidecar\n" +
			"  -r, --record                 Attach a non-live HLS sidecar\n" +
			"  --segment-duration <dur>     Sidecar HLS segment duration, e.g. 4s (default 2s)\n" +
			"  --playlist-size <n>          Sidecar sliding-window segment count, live mode only (default 6)\n" +
			"  --clean-interval <dur>       Sidecar stale-segment cleanup interval (default 10s for live)\n\n" +
			"Example:\n" +
			"  irajstreamer switch \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam1 \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam2 \\\n" +
			"    --live --segment-duration 4s -o ./hls/stream.m3u8 \\\n" +
			"    -o rtmp://127.0.0.1:1938/live/out\n\n" +
			"With a single input, the command routes that input to all outputs until interrupted.",
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

	var pendingLiveSidecar bool
	var pendingRecordSidecar bool
	var pendingSegmentDuration time.Duration
	var pendingPlaylistSize int
	var pendingCleanInterval time.Duration

	resetPending := func() {
		pendingLiveSidecar = false
		pendingRecordSidecar = false
		pendingSegmentDuration = 0
		pendingPlaylistSize = 0
		pendingCleanInterval = 0
	}

	buildPendingSidecars := func() []switchSidecarSpec {
		sidecars := make([]switchSidecarSpec, 0, 2)
		if pendingLiveSidecar {
			cleanInterval := pendingCleanInterval
			if cleanInterval <= 0 {
				cleanInterval = defaultHLSCleanInterval
			}
			sidecars = append(sidecars, switchSidecarSpec{
				mode:            "live",
				segmentDuration: pendingSegmentDuration,
				playlistSize:    pendingPlaylistSize,
				cleanInterval:   cleanInterval,
			})
		}
		if pendingRecordSidecar {
			sidecars = append(sidecars, switchSidecarSpec{
				mode:            "record",
				segmentDuration: pendingSegmentDuration,
			})
		}
		return sidecars
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

		// ── per-stream flags (sidecar HLS serving) ───────────────────────────
		case arg == "-l" || arg == "--live":
			pendingLiveSidecar = true

		case arg == "-r" || arg == "--record":
			pendingRecordSidecar = true

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
			spec.inputs = append(spec.inputs, inputSpec{url: url, sidecars: buildPendingSidecars()})
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
			spec.outputs = append(spec.outputs, outputSpec{
				url:                url,
				hlsLive:            pendingLiveSidecar,
				hlsSegmentDuration: pendingSegmentDuration,
				hlsPlaylistSize:    pendingPlaylistSize,
				hlsCleanInterval:   pendingCleanInterval,
				sidecars:           buildPendingSidecars(),
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
	if len(spec.inputs) == 0 {
		return fmt.Errorf("at least one -i input is required")
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
	namesByID := make(map[string]string, len(spec.inputs))
	inputSpecsByID := make(map[string]inputSpec, len(spec.inputs))
	for idx, in := range spec.inputs {
		streamID := fmt.Sprintf("%s-in-%d", spec.routeID, idx+1)
		stream, sidecars, err := createSwitchInputStream(spec.routeID, streamID, in)
		if err != nil {
			for _, sidecar := range sidecars {
				sidecar.Close()
			}
			return fmt.Errorf("create input %d: %w", idx+1, err)
		}
		created = append(created, stream)
		inputStreams = append(inputStreams, stream)
		namesByID[streamID] = fmt.Sprintf("Input %d", idx+1)
		inputSpecsByID[streamID] = in
	}

	outputStreams := make([]core.Stream, 0, len(spec.outputs))
	outputSpecsByID := make(map[string]outputSpec, len(spec.outputs))
	for idx, out := range spec.outputs {
		streamID := fmt.Sprintf("%s-out-%d", spec.routeID, idx+1)
		stream, sidecars, err := createSwitchOutputStream(spec.routeID, streamID, out)
		if err != nil {
			for _, sidecar := range sidecars {
				sidecar.Close()
			}
			return fmt.Errorf("create output %d: %w", idx+1, err)
		}
		created = append(created, stream)
		outputStreams = append(outputStreams, stream)
		outputSpecsByID[streamID] = out
	}

	streamer := core.NewStreamer()
	streamer.StartLife()
	defer streamer.Close()

	if err := streamer.UpdateStreams(inputStreams, outputStreams); err != nil {
		return fmt.Errorf("update streams: %w", err)
	}
	initialInputID := fmt.Sprintf("%s-in-%d", spec.routeID, 1)
	if ok := streamer.Switch(initialInputID); !ok {
		return fmt.Errorf("failed to activate initial input %q", namesByID[initialInputID])
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

	fileServer, err := startSwitchFileServer(streamer.State())
	if err != nil {
		return fmt.Errorf("start local hls file server: %w", err)
	}
	if fileServer != nil {
		defer fileServer.Close()
	}

	runtime := &switchRuntime{
		routeID:         spec.routeID,
		startupTimeout:  spec.startupTimeout,
		streamer:        streamer,
		fileServer:      fileServer,
		namesByID:       namesByID,
		inputSpecs:      inputSpecsByID,
		outputSpecs:     outputSpecsByID,
		nextInputIndex:  len(spec.inputs) + 1,
		nextOutputIndex: len(spec.outputs) + 1,
	}

	if len(inputSpecsByID) == 1 {
		<-ctx.Done()
		return nil
	}

	return runRouteSwitcherTUI(ctx, runtime)
}

func createSwitchInputStream(routeID, streamID string, spec inputSpec) (core.Stream, []core.Stream, error) {
	streamOpts, sidecars, err := buildSwitchSidecarOptions(routeID, "inputs", streamID, spec.sidecars)
	if err != nil {
		return nil, nil, err
	}
	stream, err := streamfactory.NewInput(streamID, spec.url, streamOpts...)
	if err != nil {
		for _, sidecar := range sidecars {
			sidecar.Close()
		}
		return nil, nil, err
	}
	return stream, sidecars, nil
}

func createSwitchOutputStream(routeID, streamID string, spec outputSpec) (core.Stream, []core.Stream, error) {
	streamOpts, sidecars, err := buildSwitchSidecarOptions(routeID, "outputs", streamID, spec.sidecars)
	if err != nil {
		return nil, nil, err
	}
	var stream core.Stream
	if streamfactory.IsHLSOutputPath(spec.url) {
		opts := streamfactory.HLSOutputOptions{
			IsLive:          spec.hlsLive,
			SegmentDuration: spec.hlsSegmentDuration,
			PlaylistSize:    spec.hlsPlaylistSize,
			CleanInterval:   spec.hlsCleanInterval,
		}
		stream, err = streamfactory.NewHLSOutput(streamID, spec.url, opts, streamOpts...)
	} else {
		stream, err = streamfactory.NewOutput(streamID, spec.url, streamOpts...)
	}
	if err != nil {
		for _, sidecar := range sidecars {
			sidecar.Close()
		}
		return nil, nil, err
	}
	return stream, sidecars, nil
}

func buildSwitchSidecarOptions(routeID, role, streamID string, specs []switchSidecarSpec) ([]streamfactory.StreamOption, []core.Stream, error) {
	if len(specs) == 0 {
		return nil, nil, nil
	}

	opts := make([]streamfactory.StreamOption, 0, len(specs))
	sidecars := make([]core.Stream, 0, len(specs))
	for _, spec := range specs {
		sidecarID := fmt.Sprintf("%s-%s-%s", streamID, role, spec.mode)
		sidecarPath := switchSidecarPlaylistPath(routeID, role, streamID, spec.mode)
		hlsOpts := streamfactory.HLSOutputOptions{
			IsLive:          spec.mode == "live",
			SegmentDuration: spec.segmentDuration,
			PlaylistSize:    spec.playlistSize,
			CleanInterval:   spec.cleanInterval,
		}
		sidecar, err := streamfactory.NewHLSOutput(sidecarID, sidecarPath, hlsOpts)
		if err != nil {
			for _, created := range sidecars {
				created.Close()
			}
			return nil, nil, err
		}
		opts = append(opts, streamfactory.WithStreamServer(sidecar))
		sidecars = append(sidecars, sidecar)
	}
	return opts, sidecars, nil
}

func switchSidecarPlaylistPath(routeID, role, streamID, mode string) string {
	baseDir := filepath.Join(".irajstreamer", "switch", routeID, role, streamID, mode)
	return filepath.Join(baseDir, "stream.m3u8")
}

func runRouteSwitcherTUI(ctx context.Context, runtime *switchRuntime) error {
	if err := switchSetTermRaw(); err != nil {
		<-ctx.Done()
		return nil
	}
	defer switchSetTermNormal()

	selectedID := ""
	firstRender := true
	commandMode := false
	commandBuffer := ""
	statusMessage := ""

	render := func() {
		state := runtime.streamer.State()
		liveEntries := buildSwitchEntries(state, runtime.namesByID)
		if len(liveEntries) > 0 {
			if selectedID == "" || findEntryIndexByID(liveEntries, selectedID) == -1 {
				selectedID = liveEntries[0].id
			}
		} else {
			selectedID = ""
		}
		uiLines := buildSwitchTUILines(liveEntries, state, selectedID, runtime.fileServer, commandBuffer, statusMessage)
		totalLines := len(uiLines)
		if !firstRender {
			fmt.Printf("\x1b[%dA\x1b[J", totalLines)
		}
		firstRender = false

		for _, line := range uiLines {
			fmt.Print(line)
			fmt.Print("\r\n")
		}
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
			state := runtime.streamer.State()
			totalLines := len(buildSwitchTUILines(buildSwitchEntries(state, runtime.namesByID), state, selectedID, runtime.fileServer, commandBuffer, statusMessage))
			fmt.Printf("\x1b[%dA\x1b[J", totalLines)
			return nil
		case b, ok := <-inputCh:
			if !ok {
				return nil
			}
			redraw := false
			if commandMode {
				for _, ch := range b {
					switch ch {
					case 27:
						commandMode = false
						commandBuffer = ""
						statusMessage = ""
						redraw = true
					case 127, 8:
						if len(commandBuffer) > 0 {
							commandBuffer = commandBuffer[:len(commandBuffer)-1]
						}
						redraw = true
					case 13, 10:
						statusMessage = ""
						if strings.TrimSpace(commandBuffer) != "" {
							if msg, err := runtime.executeCommand(commandBuffer); err != nil {
								statusMessage = "error: " + err.Error()
							} else {
								statusMessage = msg
							}
						}
						commandMode = false
						commandBuffer = ""
						redraw = true
					default:
						if ch >= 32 && ch <= 126 {
							commandBuffer += string(ch)
							redraw = true
						}
					}
				}
				if redraw {
					render()
				}
				continue
			}

			state := runtime.streamer.State()
			liveEntries := buildSwitchEntries(state, runtime.namesByID)
			selectedIdx := findEntryIndexByID(liveEntries, selectedID)
			if selectedIdx == -1 && len(liveEntries) > 0 {
				selectedIdx = 0
				selectedID = liveEntries[0].id
			}
			switch {
			case len(b) == 1 && (b[0] == 3 || b[0] == 'q' || b[0] == 'Q'):
				totalLines := len(buildSwitchTUILines(liveEntries, state, selectedID, runtime.fileServer, commandBuffer, statusMessage))
				fmt.Printf("\x1b[%dA\x1b[J", totalLines)
				return nil
			case len(b) == 1 && b[0] == '/':
				commandMode = true
				commandBuffer = "/"
				statusMessage = ""
				redraw = true
			case len(b) == 3 && b[0] == 27 && b[1] == '[' && b[2] == 'A':
				if selectedIdx > 0 {
					selectedID = liveEntries[selectedIdx-1].id
					redraw = true
				}
			case len(b) == 3 && b[0] == 27 && b[1] == '[' && b[2] == 'B':
				if selectedIdx >= 0 && selectedIdx < len(liveEntries)-1 {
					selectedID = liveEntries[selectedIdx+1].id
					redraw = true
				}
			case len(b) == 1 && (b[0] == 13 || b[0] == 10):
				if selectedIdx >= 0 && selectedIdx < len(liveEntries) && runtime.streamer.Switch(liveEntries[selectedIdx].id) {
					selectedID = liveEntries[selectedIdx].id
				}
				redraw = true
			}
			if redraw {
				render()
			}
		}
	}
}

func (r *switchRuntime) executeCommand(line string) (string, error) {
	req, err := parseSwitchRuntimeCommand(line)
	if err != nil {
		return "", err
	}

	switch req.kind {
	case "ai":
		return r.addInput(*req.input)
	case "ao":
		return r.addOutput(*req.output)
	case "di":
		return r.removeInput(req.targetID)
	case "do":
		return r.removeOutput(req.targetID)
	default:
		return "", fmt.Errorf("unsupported command %q", req.kind)
	}
}

func (r *switchRuntime) addInput(spec inputSpec) (string, error) {
	streamID := fmt.Sprintf("%s-in-%d", r.routeID, r.nextInputIndex)
	r.nextInputIndex++

	stream, _, err := createSwitchInputStream(r.routeID, streamID, spec)
	if err != nil {
		return "", err
	}
	if err := r.streamer.AddInput(stream); err != nil {
		stream.Close()
		return "", err
	}
	r.inputSpecs[streamID] = spec
	r.namesByID[streamID] = fmt.Sprintf("Input %d", r.nextInputIndex-1)
	if strings.TrimSpace(r.streamer.State().CurrentInputID) == "" {
		_ = r.streamer.Switch(streamID)
	}
	return "added input " + streamID, nil
}

func (r *switchRuntime) addOutput(spec outputSpec) (string, error) {
	streamID := fmt.Sprintf("%s-out-%d", r.routeID, r.nextOutputIndex)
	r.nextOutputIndex++

	stream, _, err := createSwitchOutputStream(r.routeID, streamID, spec)
	if err != nil {
		return "", err
	}
	if err := r.streamer.AddOutput(stream); err != nil {
		stream.Close()
		return "", err
	}
	r.outputSpecs[streamID] = spec
	r.namesByID[streamID] = fmt.Sprintf("Output %d", r.nextOutputIndex-1)
	return "added output " + streamID, nil
}

func (r *switchRuntime) removeInput(streamID string) (string, error) {
	state := r.streamer.State()
	if state.CurrentInputID == streamID {
		return "", fmt.Errorf("cannot remove active input %s", streamID)
	}
	if _, ok := r.inputSpecs[streamID]; !ok {
		return "", fmt.Errorf("input %s not found", streamID)
	}
	r.streamer.RemoveInput(streamID)
	delete(r.inputSpecs, streamID)
	delete(r.namesByID, streamID)
	return "removed input " + streamID, nil
}

func (r *switchRuntime) removeOutput(streamID string) (string, error) {
	if _, ok := r.outputSpecs[streamID]; !ok {
		return "", fmt.Errorf("output %s not found", streamID)
	}
	r.streamer.RemoveOutput(streamID)
	delete(r.outputSpecs, streamID)
	delete(r.namesByID, streamID)
	return "removed output " + streamID, nil
}

type switchCommandRequest struct {
	kind     string
	targetID string
	input    *inputSpec
	output   *outputSpec
}

func parseSwitchRuntimeCommand(line string) (switchCommandRequest, error) {
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) == 0 {
		return switchCommandRequest{}, fmt.Errorf("empty command")
	}

	switch fields[0] {
	case "/di", "/do":
		if len(fields) != 2 {
			return switchCommandRequest{}, fmt.Errorf("%s requires exactly one stream id", fields[0])
		}
		return switchCommandRequest{
			kind:     strings.TrimPrefix(fields[0], "/"),
			targetID: strings.TrimSpace(fields[1]),
		}, nil
	case "/ai", "/ao":
		spec, err := parseSwitchAddCommand(fields[1:], fields[0] == "/ao")
		if err != nil {
			return switchCommandRequest{}, err
		}
		req := switchCommandRequest{kind: strings.TrimPrefix(fields[0], "/")}
		if fields[0] == "/ai" {
			req.input = &inputSpec{url: spec.url, sidecars: spec.sidecars}
		} else {
			req.output = &spec
		}
		return req, nil
	default:
		return switchCommandRequest{}, fmt.Errorf("unknown command %q", fields[0])
	}
}

func parseSwitchAddCommand(args []string, output bool) (outputSpec, error) {
	var spec outputSpec
	var pendingLive bool
	var pendingRecord bool

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--live":
			pendingLive = true
		case arg == "--record":
			pendingRecord = true
		case arg == "--segment-duration":
			if i+1 >= len(args) {
				return outputSpec{}, fmt.Errorf("--segment-duration requires a value")
			}
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				return outputSpec{}, fmt.Errorf("--segment-duration: %w", err)
			}
			spec.hlsSegmentDuration = d
			i++
		case strings.HasPrefix(arg, "--segment-duration="):
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--segment-duration="))
			if err != nil {
				return outputSpec{}, fmt.Errorf("--segment-duration: %w", err)
			}
			spec.hlsSegmentDuration = d
		case arg == "--playlist-size":
			if i+1 >= len(args) {
				return outputSpec{}, fmt.Errorf("--playlist-size requires a value")
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil {
				return outputSpec{}, fmt.Errorf("--playlist-size: %w", err)
			}
			spec.hlsPlaylistSize = n
			i++
		case strings.HasPrefix(arg, "--playlist-size="):
			n, err := strconv.Atoi(strings.TrimPrefix(arg, "--playlist-size="))
			if err != nil {
				return outputSpec{}, fmt.Errorf("--playlist-size: %w", err)
			}
			spec.hlsPlaylistSize = n
		case arg == "--clean-interval":
			if i+1 >= len(args) {
				return outputSpec{}, fmt.Errorf("--clean-interval requires a value")
			}
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				return outputSpec{}, fmt.Errorf("--clean-interval: %w", err)
			}
			spec.hlsCleanInterval = d
			i++
		case strings.HasPrefix(arg, "--clean-interval="):
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--clean-interval="))
			if err != nil {
				return outputSpec{}, fmt.Errorf("--clean-interval: %w", err)
			}
			spec.hlsCleanInterval = d
		default:
			if i != len(args)-1 {
				return outputSpec{}, fmt.Errorf("unexpected argument %q", arg)
			}
			spec.url = strings.TrimSpace(arg)
		}
	}

	if strings.TrimSpace(spec.url) == "" {
		return outputSpec{}, fmt.Errorf("missing url")
	}

	if pendingLive {
		cleanInterval := spec.hlsCleanInterval
		if cleanInterval <= 0 {
			cleanInterval = defaultHLSCleanInterval
		}
		spec.sidecars = append(spec.sidecars, switchSidecarSpec{
			mode:            "live",
			segmentDuration: spec.hlsSegmentDuration,
			playlistSize:    spec.hlsPlaylistSize,
			cleanInterval:   cleanInterval,
		})
	}
	if pendingRecord {
		spec.sidecars = append(spec.sidecars, switchSidecarSpec{
			mode:            "record",
			segmentDuration: spec.hlsSegmentDuration,
		})
	}

	if output {
		spec.hlsLive = pendingLive
	}

	return spec, nil
}

func buildSwitchEntries(state core.StreamerState, namesByID map[string]string) []switchEntry {
	entries := make([]switchEntry, 0, len(state.StreamInputs))
	for i, input := range state.StreamInputs {
		if input == nil {
			continue
		}
		name := namesByID[input.StreamID]
		if strings.TrimSpace(name) == "" {
			name = fmt.Sprintf("Input %d", i+1)
		}
		entries = append(entries, switchEntry{id: input.StreamID, name: name})
	}
	return entries
}

func buildSwitchTUILines(entries []switchEntry, state core.StreamerState, selectedID string, fileServer *switchFileServer, commandBuffer string, statusMessage string) []string {
	lines := make([]string, 0, len(entries)+24)
	activeIdx := findActiveEntryIndex(entries, state.CurrentInputID)
	selectedIdx := findEntryIndexByID(entries, selectedID)
	servedPaths := collectServedPaths(state, fileServer)

	lines = append(lines, "")
	if len(servedPaths) > 0 {
		lines = append(lines, "  Served Paths:")
		for _, served := range servedPaths {
			lines = append(lines, formatServedPathLine(served))
		}
		lines = append(lines, "  ")
	}

	lines = append(lines,
		"\x1b[1;36m  ┌──────────────────────────────────────┐",
		"  │          ROUTE  SWITCHER             │",
		"  └──────────────────────────────────────┘\x1b[0m",
		fmt.Sprintf("  Inputs: %d   Outputs: %d", len(entries), len(state.StreamOutputs)),
	)

	lines = append(lines, "  Input Sources:")
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
		sourceURL := findStateURL(state.StreamInputs, e.id)
		if sourceURL != "" {
			lines = append(lines, fmt.Sprintf("%s%s%d.  %s%s  \x1b[90m%s\x1b[0m", cursor, nameStyle, i+1, e.name, liveTag, sourceURL))
		} else {
			lines = append(lines, fmt.Sprintf("%s%s%d.  %s%s", cursor, nameStyle, i+1, e.name, liveTag))
		}
	}

	outputLines := collectOutputLines(state)
	if len(outputLines) > 0 {
		lines = append(lines, "  ", "  Output Targets:")
		lines = append(lines, outputLines...)
	}

	if strings.TrimSpace(commandBuffer) != "" {
		lines = append(lines, "  ", "  Command: "+commandBuffer)
	}
	if strings.TrimSpace(statusMessage) != "" {
		lines = append(lines, "  ", "  Status: "+statusMessage)
	}

	lines = append(lines, "\x1b[90m  ↑ ↓ navigate    Enter switch    / command    q quit\x1b[0m")
	return lines
}

func findActiveEntryIndex(entries []switchEntry, activeID string) int {
	if activeID == "" {
		return -1
	}
	for i, entry := range entries {
		if entry.id == activeID {
			return i
		}
	}
	return -1
}

func findEntryIndexByID(entries []switchEntry, id string) int {
	for i, entry := range entries {
		if entry.id == id {
			return i
		}
	}
	return -1
}

func collectOutputLines(state core.StreamerState) []string {
	lines := make([]string, 0, len(state.StreamOutputs))
	for _, outputState := range state.StreamOutputs {
		if outputState == nil {
			continue
		}
		target := strings.TrimSpace(outputState.Url)
		if target == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("    - %s: %s", fallbackLabel(outputState.StreamID), target))
	}
	sort.Strings(lines)
	return lines
}

func findStateURL(states []*core.State, streamID string) string {
	for _, state := range states {
		if state != nil && state.StreamID == streamID {
			return strings.TrimSpace(state.Url)
		}
	}
	return ""
}

func collectServedPaths(state core.StreamerState, fileServer *switchFileServer) []switchServedPath {
	type sortable struct {
		key  string
		path switchServedPath
	}

	items := make([]sortable, 0, len(state.StreamInputs)+len(state.StreamOutputs))
	appendFromStates := func(role string, states []*core.State) {
		for _, streamState := range states {
			if streamState == nil {
				continue
			}
			for _, servedState := range servedStatesForRender(streamState) {
				entry, ok := servedPathFromState(role, streamState.StreamID, servedState, fileServer)
				if !ok {
					continue
				}
				items = append(items, sortable{
					key:  role + ":" + entry.OwnerID + ":" + entry.ServedID + ":" + entry.PlaylistPath,
					path: entry,
				})
			}
		}
	}
	appendFromStates("input", state.StreamInputs)
	appendFromStates("output", state.StreamOutputs)

	sort.Slice(items, func(i, j int) bool {
		return items[i].key < items[j].key
	})

	out := make([]switchServedPath, 0, len(items))
	for _, item := range items {
		out = append(out, item.path)
	}
	return out
}

func servedStatesForRender(state *core.State) []core.ServedState {
	if state == nil {
		return nil
	}
	if len(state.Served) > 0 {
		return append([]core.ServedState(nil), state.Served...)
	}
	if state.LocalPath == "" && state.ServeType == "" && state.ServeMode == "" {
		return nil
	}
	return []core.ServedState{{
		StreamID:  state.StreamID,
		Url:       state.Url,
		LocalPath: state.LocalPath,
		ServeType: state.ServeType,
		ServeMode: state.ServeMode,
	}}
}

func servedPathFromState(role, ownerID string, state core.ServedState, fileServer *switchFileServer) (switchServedPath, bool) {
	if strings.TrimSpace(state.ServeType) != "hls" {
		return switchServedPath{}, false
	}

	playlistPath := resolveLocalPlaylistPath(state)
	if playlistPath == "" {
		return switchServedPath{}, false
	}

	entry := switchServedPath{
		Role:         role,
		OwnerID:      ownerID,
		ServedID:     fallbackLabel(state.StreamID),
		ServeType:    state.ServeType,
		ServeMode:    state.ServeMode,
		PlaylistPath: playlistPath,
		Directory:    filepath.Dir(playlistPath),
	}
	if fileServer != nil {
		entry.HTTPURL = fileServer.PlaylistURL(playlistPath)
	}
	return entry, true
}

func resolveLocalPlaylistPath(state core.ServedState) string {
	if rawURL := strings.TrimSpace(state.Url); rawURL != "" {
		if parsed, err := neturl.Parse(rawURL); err == nil && parsed.Scheme == "file" {
			if parsed.Path != "" {
				return filepath.Clean(parsed.Path)
			}
		}
	}

	if localPath := strings.TrimSpace(state.LocalPath); localPath != "" {
		return filepath.Join(localPath, "stream.m3u8")
	}
	return ""
}

func formatServedPathLine(path switchServedPath) string {
	mode := strings.TrimSpace(path.ServeMode)
	if mode == "" {
		mode = "serve"
	}
	parts := []string{
		fmt.Sprintf("%s %s", path.ServeType, mode),
		"playlist=" + path.PlaylistPath,
	}
	if path.HTTPURL != "" {
		parts = append(parts, "http="+path.HTTPURL)
	}
	return fmt.Sprintf("    - %s %s: %s", path.Role, fallbackLabel(path.ServedID), strings.Join(parts, "   "))
}

func fallbackLabel(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return "-"
	}
	return label
}

func startSwitchFileServer(state core.StreamerState) (*switchFileServer, error) {
	served := collectServedPaths(state, nil)
	playlistPaths := make([]string, 0, len(served))
	for _, entry := range served {
		if entry.PlaylistPath != "" {
			playlistPaths = append(playlistPaths, entry.PlaylistPath)
		}
	}
	rootDir := commonAncestorDir(playlistPaths)
	if rootDir == "" {
		rootDir = localFilesystemRoot()
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &http.Server{
		Handler: http.FileServer(http.Dir(rootDir)),
	}

	go func() {
		_ = server.Serve(listener)
	}()

	return &switchFileServer{
		server:  server,
		baseURL: "http://" + listener.Addr().String(),
		rootDir: rootDir,
	}, nil
}

func localFilesystemRoot() string {
	wd, err := os.Getwd()
	if err == nil {
		if vol := filepath.VolumeName(wd); vol != "" {
			return vol + string(filepath.Separator)
		}
	}
	return string(filepath.Separator)
}

func (s *switchFileServer) Close() {
	if s == nil || s.server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.server.Shutdown(ctx)
}

func (s *switchFileServer) PlaylistURL(playlistPath string) string {
	if s == nil || s.baseURL == "" || s.rootDir == "" || playlistPath == "" {
		return ""
	}
	rel, err := filepath.Rel(s.rootDir, playlistPath)
	if err != nil {
		return ""
	}
	if strings.HasPrefix(rel, "..") {
		return ""
	}
	return strings.TrimRight(s.baseURL, "/") + "/" + filepath.ToSlash(rel)
}

func commonAncestorDir(paths []string) string {
	if len(paths) == 0 {
		return ""
	}

	root := filepath.Dir(filepath.Clean(paths[0]))
	for _, p := range paths[1:] {
		root = commonPathPrefix(root, filepath.Dir(filepath.Clean(p)))
		if root == string(filepath.Separator) || root == "." {
			return root
		}
	}
	return root
}

func commonPathPrefix(a, b string) string {
	aParts := splitPathParts(a)
	bParts := splitPathParts(b)
	limit := len(aParts)
	if len(bParts) < limit {
		limit = len(bParts)
	}
	if limit == 0 {
		if filepath.IsAbs(a) || filepath.IsAbs(b) {
			return string(filepath.Separator)
		}
		return "."
	}

	common := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		if aParts[i] != bParts[i] {
			break
		}
		common = append(common, aParts[i])
	}
	if len(common) == 0 {
		if filepath.IsAbs(a) || filepath.IsAbs(b) {
			return string(filepath.Separator)
		}
		return "."
	}

	if filepath.IsAbs(a) {
		return string(filepath.Separator) + filepath.Join(common...)
	}
	return filepath.Join(common...)
}

func splitPathParts(p string) []string {
	cleaned := filepath.Clean(p)
	if cleaned == string(filepath.Separator) || cleaned == "." {
		return nil
	}
	if filepath.IsAbs(cleaned) {
		cleaned = strings.TrimPrefix(cleaned, string(filepath.Separator))
	}
	parts := strings.Split(cleaned, string(filepath.Separator))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" && part != "." {
			out = append(out, part)
		}
	}
	return out
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
