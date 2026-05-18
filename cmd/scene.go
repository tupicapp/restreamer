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

	irajstreamer "restreamer/core"
	"restreamer/core/raw"
	scenes "restreamer/core/scenes"
	"restreamer/core/streamfactory"

	"github.com/spf13/cobra"
)

// ─── single-scene command ─────────────────────────────────────────────────────

type sceneCommandOptions struct {
	sceneID             string
	inputs              []string
	layouts             []string
	audioRatios         []int
	canvas              string
	output              string
	audioFrom           int
	outputFPS           int
	startupTimeout      time.Duration
	hlsLive             bool
	hlsSegmentDuration  time.Duration
	hlsPlaylistSize     int
	hlsCleanInterval    time.Duration

	// multi-scene mode: each entry is "Name|inputURL|layout[|inputURL|layout...]"
	sceneDefs []string
}

type sceneMode string

const (
	sceneModeCompose     sceneMode = "compose"
	sceneModePassthrough sceneMode = "passthrough"
)

type sceneSpec struct {
	mode                sceneMode
	sceneID             string
	inputURLs           []string
	layouts             []raw.VideoLayout
	canvas              raw.CanvasSpec
	outputURL           string
	hlsOptions          *streamfactory.HLSOutputOptions
	audioFrom           int
	audioRatios         []int
	outputFPS           int
	startupTimeout      time.Duration
	hlsSegmentDuration  time.Duration
	hlsPlaylistSize     int
	hlsCleanInterval    time.Duration
}

type sceneEntry struct {
	id   string
	name string
}

func NewSceneCommand() *cobra.Command {
	opts := sceneCommandOptions{}

	cmd := &cobra.Command{
		Use:   "scene -i <input> [--layout <x,y,width,height[,z[,transparency]]> ...] [--output <dest>|<dest>]",
		Short: "Pass through one or more inputs, or compose multiple inputs into one output",
		Long: "For one or more -i/--input values with no layout and no canvas, forward the selected input directly " +
			"to the output without scene composition or re-encoding. With multiple passthrough inputs, the command starts the interactive switcher UI.\n\n" +
			"When layouts are provided, build a scene from one or more inputs, place each input using a video layout, " +
			"encode the merged result, and push it to one output destination.\n\n" +
			"Multi-scene mode: pass --scene flags instead of --input/--layout to define several scenes\n" +
			"and switch between them live with an interactive TUI.\n\n" +
			"  --scene format:  \"Name|input_url|x,y,w,h[|input_url|x,y,w,h...]\"\n\n" +
			"  Passthrough example:\n" +
			"    restreamer scene -i rtmp://srv/cam -o rtmp://srv/out\n\n" +
			"  Passthrough switcher example:\n" +
			"    restreamer scene -i rtmp://srv/cam -i rtmp://srv/backup -o rtmp://srv/out\n\n" +
			"  Example:\n" +
			"    restreamer scene \\\n" +
			"      --scene \"Camera|rtmp://srv/cam|0,0,1280,720\" \\\n" +
			"      --scene \"Screen|rtmp://srv/screen|0,0,1280,720\" \\\n" +
			"      --canvas 1280x720 -o rtmp://srv/out",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Multi-scene mode when --scene flags are present.
			if len(opts.sceneDefs) > 0 {
				return runMultiSceneCommand(cmd.Context(), opts, args)
			}

			if shouldShowSceneHelp(opts, args) {
				return cmd.Help()
			}

			spec, err := buildSceneSpec(opts, args)
			if err != nil {
				return err
			}
			return runSceneCommand(cmd.Context(), spec)
		},
	}

	flags := cmd.Flags()
	// single-scene flags
	flags.StringVar(&opts.sceneID, "scene-id", "scene-1", "Scene stream ID")
	flags.StringSliceVarP(&opts.inputs, "input", "i", nil, "Input URL. Repeat for each scene input")
	flags.StringArrayVar(&opts.layouts, "layout", nil, "Input layout as x,y,width,height[,z[,transparency]]. Repeat in input order")
	flags.StringVar(&opts.canvas, "canvas", "", "Scene canvas size as WIDTHxHEIGHT. If omitted, it is derived from layouts")
	flags.StringVarP(&opts.output, "output", "o", "", "Output destination URL or directory path")
	flags.IntVar(&opts.audioFrom, "audio-from", 0, "Zero-based input index used for audio passthrough")
	flags.IntSliceVar(&opts.audioRatios, "audio-ratio", nil, "Per-input audio mix percentage in input order; values must be 0-100 and sum to 100. Overrides --audio-from")
	flags.IntVar(&opts.outputFPS, "fps", 25, "Encoded scene output FPS")
	flags.DurationVar(&opts.startupTimeout, "startup-timeout", 30*time.Second, "Maximum time to wait for the scene pipeline to produce output")
	flags.BoolVarP(&opts.hlsLive, "live", "l", false, "HLS live mode: sliding window playlist with segment cleanup (default is record/VOD)")
	flags.DurationVar(&opts.hlsSegmentDuration, "segment-duration", 0, "HLS segment duration, e.g. 4s (default 2s)")
	flags.IntVar(&opts.hlsPlaylistSize, "playlist-size", 0, "HLS sliding window size in segments, live mode only (default 6)")
	flags.DurationVar(&opts.hlsCleanInterval, "clean-interval", defaultHLSCleanInterval, "How often stale HLS segments are removed from disk, live mode only")
	// multi-scene flag
	flags.StringArrayVar(&opts.sceneDefs, "scene", nil,
		`Multi-scene mode. Format: "Name|input_url|x,y,w,h[|input_url|x,y,w,h...]". Repeat for each scene.`)

	return cmd
}

func shouldShowSceneHelp(opts sceneCommandOptions, args []string) bool {
	return len(opts.inputs) == 0 &&
		len(opts.layouts) == 0 &&
		strings.TrimSpace(opts.canvas) == "" &&
		strings.TrimSpace(opts.output) == "" &&
		len(args) == 0
}

func toServiceSceneMode(mode sceneMode) scenes.SceneMode {
	if mode == sceneModePassthrough {
		return scenes.SceneModePassthrough
	}
	return scenes.SceneModeCompose
}

// ─── single-scene run ─────────────────────────────────────────────────────────

func runSceneCommand(parent context.Context, spec sceneSpec) error {
	if parent == nil {
		parent = context.Background()
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	sceneService := scenes.NewService()
	return sceneService.RunScene(
		ctx,
		scenes.SceneSpec{
			Mode:           toServiceSceneMode(spec.mode),
			SceneID:        spec.sceneID,
			InputURLs:      append([]string(nil), spec.inputURLs...),
			Layouts:        append([]raw.VideoLayout(nil), spec.layouts...),
			Canvas:         spec.canvas,
			OutputURL:      spec.outputURL,
			HLSOptions:     spec.hlsOptions,
			AudioFrom:      spec.audioFrom,
			AudioRatios:    append([]int(nil), spec.audioRatios...),
			OutputFPS:      spec.outputFPS,
			StartupTimeout: spec.startupTimeout,
		},
		func(ctx context.Context, entries []scenes.SceneEntry, streamer *irajstreamer.Streamer) error {
			uiEntries := make([]sceneEntry, 0, len(entries))
			for _, entry := range entries {
				uiEntries = append(uiEntries, sceneEntry{id: entry.ID, name: entry.Name})
			}
			return runSceneSwitcherTUI(ctx, uiEntries, streamer)
		},
	)
}

func buildSceneSpec(opts sceneCommandOptions, args []string) (sceneSpec, error) {
	outputURL, err := resolveSceneOutput(opts.output, args)
	if err != nil {
		return sceneSpec{}, err
	}
	if len(opts.inputs) == 0 {
		return sceneSpec{}, fmt.Errorf("at least one --input is required")
	}
	if opts.outputFPS <= 0 {
		return sceneSpec{}, fmt.Errorf("--fps must be greater than zero")
	}
	if opts.startupTimeout <= 0 {
		return sceneSpec{}, fmt.Errorf("--startup-timeout must be greater than zero")
	}
	if opts.audioFrom < 0 || opts.audioFrom >= len(opts.inputs) {
		return sceneSpec{}, fmt.Errorf("--audio-from must be between 0 and %d", len(opts.inputs)-1)
	}
	audioRatios, err := scenes.NormalizeAudioMixRatiosForCLI(len(opts.inputs), opts.audioRatios)
	if err != nil {
		return sceneSpec{}, err
	}

	canvas, hasCanvas, err := parseCanvasSpec(opts.canvas)
	if err != nil {
		return sceneSpec{}, err
	}

	layouts := make([]raw.VideoLayout, 0, len(opts.layouts))
	for idx, layoutValue := range opts.layouts {
		layout, err := parseVideoLayout(layoutValue)
		if err != nil {
			return sceneSpec{}, fmt.Errorf("parse --layout %d: %w", idx+1, err)
		}
		layouts = append(layouts, layout)
	}

	mode := sceneModeCompose
	if len(opts.inputs) > 0 && len(layouts) == 0 && !hasCanvas {
		mode = sceneModePassthrough
	}
	if mode == sceneModePassthrough && len(audioRatios) > 0 {
		return sceneSpec{}, fmt.Errorf("--audio-ratio requires scene composition; add --layout values or --canvas")
	}

	if mode == sceneModeCompose {
		switch {
		case len(layouts) == 0 && len(opts.inputs) == 1 && hasCanvas:
			layouts = []raw.VideoLayout{{
				Width:  canvas.Width,
				Height: canvas.Height,
			}}
		case len(layouts) != len(opts.inputs):
			return sceneSpec{}, fmt.Errorf("--layout count must match --input count")
		}
	}

	if mode == sceneModeCompose && !hasCanvas {
		canvas, err = deriveCanvas(layouts)
		if err != nil {
			return sceneSpec{}, err
		}
	}

	return sceneSpec{
		mode:           mode,
		sceneID:        strings.TrimSpace(opts.sceneID),
		inputURLs:      append([]string(nil), opts.inputs...),
		layouts:        layouts,
		canvas:         canvas,
		outputURL:      outputURL,
		hlsOptions:     sceneHLSOptions(opts),
		audioFrom:      opts.audioFrom,
		audioRatios:    audioRatios,
		outputFPS:      opts.outputFPS,
		startupTimeout: opts.startupTimeout,
	}, nil
}

func sceneHLSOptions(opts sceneCommandOptions) *streamfactory.HLSOutputOptions {
	if !opts.hlsLive && opts.hlsSegmentDuration == 0 && opts.hlsPlaylistSize == 0 {
		return nil
	}
	cleanInterval := opts.hlsCleanInterval
	if cleanInterval <= 0 {
		cleanInterval = defaultHLSCleanInterval
	}
	return &streamfactory.HLSOutputOptions{
		IsLive:          opts.hlsLive,
		SegmentDuration: opts.hlsSegmentDuration,
		PlaylistSize:    opts.hlsPlaylistSize,
		CleanInterval:   cleanInterval,
	}
}

// ─── multi-scene run ──────────────────────────────────────────────────────────

func runMultiSceneCommand(parent context.Context, opts sceneCommandOptions, args []string) error {
	if parent == nil {
		parent = context.Background()
	}

	outputURL, err := resolveSceneOutput(opts.output, args)
	if err != nil {
		return err
	}

	canvas, hasCanvas, err := parseCanvasSpec(opts.canvas)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	definitions := make([]scenes.MultiSceneDefinition, 0, len(opts.sceneDefs))

	for idx, def := range opts.sceneDefs {
		name, inputURLs, layoutStrs, parseErr := parseSceneDef(def)
		if parseErr != nil {
			return fmt.Errorf("--scene %d: %w", idx+1, parseErr)
		}

		layouts := make([]raw.VideoLayout, 0, len(layoutStrs))
		for li, ls := range layoutStrs {
			layout, lerr := parseVideoLayout(ls)
			if lerr != nil {
				return fmt.Errorf("--scene %d layout %d: %w", idx+1, li+1, lerr)
			}
			layouts = append(layouts, layout)
		}

		definitions = append(definitions, scenes.MultiSceneDefinition{
			Name:     name,
			InputURL: append([]string(nil), inputURLs...),
			Layouts:  append([]raw.VideoLayout(nil), layouts...),
		})
	}

	sceneService := scenes.NewService()
	return sceneService.RunMultiScene(
		ctx,
		scenes.MultiSceneSpec{
			OutputURL:      outputURL,
			HLSOptions:     sceneHLSOptions(opts),
			HasCanvas:      hasCanvas,
			Canvas:         canvas,
			AudioFrom:      opts.audioFrom,
			AudioRatios:    append([]int(nil), opts.audioRatios...),
			OutputFPS:      opts.outputFPS,
			StartupTimeout: opts.startupTimeout,
			Definitions:    definitions,
		},
		func(ctx context.Context, entries []scenes.SceneEntry, streamer *irajstreamer.Streamer) error {
			uiEntries := make([]sceneEntry, 0, len(entries))
			for _, entry := range entries {
				uiEntries = append(uiEntries, sceneEntry{id: entry.ID, name: entry.Name})
			}
			return runSceneSwitcherTUI(ctx, uiEntries, streamer)
		},
	)
}

// parseSceneDef parses "Name|inputURL|layout[|inputURL|layout...]".
func parseSceneDef(s string) (name string, inputs []string, layouts []string, err error) {
	parts := strings.Split(s, "|")
	if len(parts) < 3 {
		err = fmt.Errorf("must be \"Name|input_url|x,y,w,h[|input_url|x,y,w,h...]\"")
		return
	}
	name = strings.TrimSpace(parts[0])
	if name == "" {
		err = fmt.Errorf("scene name cannot be empty")
		return
	}
	rest := parts[1:]
	if len(rest)%2 != 0 {
		err = fmt.Errorf("each input_url must be paired with a layout (%d values after name)", len(rest))
		return
	}
	for i := 0; i < len(rest); i += 2 {
		inputs = append(inputs, strings.TrimSpace(rest[i]))
		layouts = append(layouts, strings.TrimSpace(rest[i+1]))
	}
	return
}

// ─── interactive TUI ─────────────────────────────────────────────────────────

func runSceneSwitcherTUI(ctx context.Context, entries []sceneEntry, streamer *irajstreamer.Streamer) error {
	if err := setTermRaw(); err != nil {
		// No TTY — just block until context is cancelled.
		<-ctx.Done()
		return nil
	}
	defer setTermNormal()

	activeIdx := 0
	selectedIdx := 0
	firstRender := true
	// header(1 blank + 3 box lines) + one line per scene + footer(1) = len+5
	totalLines := len(entries) + 5

	render := func() {
		if !firstRender {
			fmt.Printf("\x1b[%dA\x1b[J", totalLines)
		}
		firstRender = false

		fmt.Print("\r\n")
		fmt.Print("\x1b[1;36m  ┌──────────────────────────────────────┐\r\n")
		fmt.Print("  │          SCENE  SWITCHER             │\r\n")
		fmt.Print("  └──────────────────────────────────────┘\x1b[0m\r\n")
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
			case len(b) == 1 && (b[0] == 3 || b[0] == 'q' || b[0] == 'Q'): // Ctrl-C / q
				fmt.Printf("\x1b[%dA\x1b[J", totalLines)
				return nil

			case len(b) == 3 && b[0] == 27 && b[1] == '[' && b[2] == 'A': // ↑
				if selectedIdx > 0 {
					selectedIdx--
					redraw = true
				}

			case len(b) == 3 && b[0] == 27 && b[1] == '[' && b[2] == 'B': // ↓
				if selectedIdx < len(entries)-1 {
					selectedIdx++
					redraw = true
				}

			case len(b) == 1 && (b[0] == 13 || b[0] == 10): // Enter
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

func setTermRaw() error {
	cmd := exec.Command("stty", "raw", "-echo")
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func setTermNormal() {
	cmd := exec.Command("stty", "sane")
	cmd.Stdin = os.Stdin
	_ = cmd.Run()
}

// ─── shared helpers ───────────────────────────────────────────────────────────

func resolveSceneOutput(flagValue string, args []string) (string, error) {
	outputURL := strings.TrimSpace(flagValue)
	if outputURL != "" {
		if len(args) > 0 {
			return "", fmt.Errorf("output cannot be provided both as argument and --output")
		}
		return outputURL, nil
	}
	if len(args) != 1 {
		return "", fmt.Errorf("exactly one output destination is required")
	}

	outputURL = strings.TrimSpace(args[0])
	if outputURL == "" {
		return "", fmt.Errorf("output destination cannot be empty")
	}
	return outputURL, nil
}

func parseCanvasSpec(value string) (raw.CanvasSpec, bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return raw.CanvasSpec{}, false, nil
	}

	parts := strings.Split(strings.ToLower(value), "x")
	if len(parts) != 2 {
		return raw.CanvasSpec{}, false, fmt.Errorf("--canvas must be WIDTHxHEIGHT")
	}

	width, err := parsePositiveInt(parts[0])
	if err != nil {
		return raw.CanvasSpec{}, false, fmt.Errorf("invalid canvas width: %w", err)
	}
	height, err := parsePositiveInt(parts[1])
	if err != nil {
		return raw.CanvasSpec{}, false, fmt.Errorf("invalid canvas height: %w", err)
	}

	if _, err := raw.ExpectedYUV420PSize(width, height); err != nil {
		return raw.CanvasSpec{}, false, fmt.Errorf("invalid canvas size: %w", err)
	}

	return raw.NewBlackCanvasSpec(width, height), true, nil
}

func parseVideoLayout(value string) (raw.VideoLayout, error) {
	parts := strings.Split(strings.TrimSpace(value), ",")
	if len(parts) != 4 && len(parts) != 5 && len(parts) != 6 {
		return raw.VideoLayout{}, fmt.Errorf("layout must be x,y,width,height[,z[,transparency]]")
	}

	x, err := parseInteger(parts[0])
	if err != nil {
		return raw.VideoLayout{}, fmt.Errorf("invalid x: %w", err)
	}
	y, err := parseInteger(parts[1])
	if err != nil {
		return raw.VideoLayout{}, fmt.Errorf("invalid y: %w", err)
	}
	width, err := parsePositiveInt(parts[2])
	if err != nil {
		return raw.VideoLayout{}, fmt.Errorf("invalid width: %w", err)
	}
	height, err := parsePositiveInt(parts[3])
	if err != nil {
		return raw.VideoLayout{}, fmt.Errorf("invalid height: %w", err)
	}

	layout := raw.VideoLayout{
		X:      x,
		Y:      y,
		Width:  width,
		Height: height,
	}

	if len(parts) >= 5 {
		zIndex, err := parseInteger(parts[4])
		if err != nil {
			return raw.VideoLayout{}, fmt.Errorf("invalid z-index: %w", err)
		}
		layout.ZIndex = zIndex
	}

	if len(parts) == 6 {
		transparency, err := strconv.ParseFloat(strings.TrimSpace(parts[5]), 64)
		if err != nil {
			return raw.VideoLayout{}, fmt.Errorf("invalid transparency: %w", err)
		}
		layout.Transparency = transparency
	}

	if err := layout.Validate(); err != nil {
		return raw.VideoLayout{}, err
	}

	return layout, nil
}

func deriveCanvas(layouts []raw.VideoLayout) (raw.CanvasSpec, error) {
	if len(layouts) == 0 {
		return raw.CanvasSpec{}, fmt.Errorf("either --canvas or at least one --layout is required")
	}

	maxWidth := 0
	maxHeight := 0
	for _, layout := range layouts {
		right := layout.X + layout.Width
		bottom := layout.Y + layout.Height
		if right > maxWidth {
			maxWidth = right
		}
		if bottom > maxHeight {
			maxHeight = bottom
		}
	}

	if _, err := raw.ExpectedYUV420PSize(maxWidth, maxHeight); err != nil {
		return raw.CanvasSpec{}, fmt.Errorf("derived canvas is invalid: %w", err)
	}

	return raw.NewBlackCanvasSpec(maxWidth, maxHeight), nil
}

func parsePositiveInt(value string) (int, error) {
	parsed, err := parseInteger(value)
	if err != nil {
		return 0, err
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("must be greater than zero")
	}
	return parsed, nil
}

func parseInteger(value string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, err
	}
	return parsed, nil
}
