package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"restreamer/core/raw"
	"restreamer/core/rawstreamer"
	"restreamer/core/streamfactory"

	"github.com/spf13/cobra"
)

type rawSceneCommandOptions struct {
	streamID           string
	inputs             []string
	layouts            []string
	audioRatios        []int
	canvas             string
	output             string
	audioFrom          int
	outputFPS          int
	startupTimeout     time.Duration
	hlsLive            bool
	hlsSegmentDuration time.Duration
	hlsPlaylistSize    int
	hlsCleanInterval   time.Duration
}

type rawSceneSpec struct {
	streamID       string
	inputURLs      []string
	layouts        []raw.VideoLayout
	canvas         raw.CanvasSpec
	outputURL      string
	hlsOptions     *streamfactory.HLSOutputOptions
	audioFrom      int
	audioRatios    []int
	outputFPS      int
	startupTimeout time.Duration
}

func NewRawSceneCommand() *cobra.Command {
	opts := rawSceneCommandOptions{}

	cmd := &cobra.Command{
		Use:   "rawscene -i <input> --layout <x,y,width,height[,z[,transparency]]> ... [--output <dest>|<dest>]",
		Short: "Run one RawStreamer with the default Composer raw processor",
		Long: "Build a single RawStreamer, decode inputs into raw frames, compose them with the default " +
			"Composer processor, encode the result, and push it to one output destination.\n\n" +
			"Example:\n" +
			"  irajstreamer rawscene \\\n" +
			"    --stream-id raw-scene-1 \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam1 --layout 0,0,640,360 \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam2 --layout 640,0,640,360 \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam3 --layout 0,360,640,360 \\\n" +
			"    -i rtmp://127.0.0.1:1938/live/cam4 --layout 640,360,640,360 \\\n" +
			"    --canvas 1280x720 \\\n" +
			"    -o rtmp://127.0.0.1:1938/live/out",
		RunE: func(cmd *cobra.Command, args []string) error {
			if shouldShowRawSceneHelp(opts, args) {
				return cmd.Help()
			}

			spec, err := buildRawSceneSpec(opts, args)
			if err != nil {
				return err
			}
			return runRawSceneCommand(cmd.Context(), spec)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.streamID, "stream-id", "raw-scene-1", "RawStreamer ID")
	flags.StringSliceVarP(&opts.inputs, "input", "i", nil, "Input URL. Repeat for each raw-scene input")
	flags.StringArrayVar(&opts.layouts, "layout", nil, "Input layout as x,y,width,height[,z[,transparency]]. Repeat in input order")
	flags.StringVar(&opts.canvas, "canvas", "", "Canvas size as WIDTHxHEIGHT. If omitted, it is derived from layouts")
	flags.StringVarP(&opts.output, "output", "o", "", "Output destination URL or directory path")
	flags.IntVar(&opts.audioFrom, "audio-from", 0, "Zero-based input index used for audio passthrough")
	flags.IntSliceVar(&opts.audioRatios, "audio-ratio", nil, "Per-input audio mix percentage in input order; values must be 0-100 and sum to 100. Overrides --audio-from")
	flags.IntVar(&opts.outputFPS, "fps", 25, "Encoded raw-scene output FPS")
	flags.DurationVar(&opts.startupTimeout, "startup-timeout", 30*time.Second, "Maximum time to wait for the raw-scene pipeline to produce output")
	flags.BoolVarP(&opts.hlsLive, "live", "l", false, "HLS live mode: sliding window playlist with segment cleanup (default is record/VOD)")
	flags.DurationVar(&opts.hlsSegmentDuration, "segment-duration", 0, "HLS segment duration, e.g. 4s (default 2s)")
	flags.IntVar(&opts.hlsPlaylistSize, "playlist-size", 0, "HLS sliding window size in segments, live mode only (default 6)")
	flags.DurationVar(&opts.hlsCleanInterval, "clean-interval", defaultHLSCleanInterval, "How often stale HLS segments are removed from disk, live mode only")

	return cmd
}

func shouldShowRawSceneHelp(opts rawSceneCommandOptions, args []string) bool {
	return len(opts.inputs) == 0 &&
		len(opts.layouts) == 0 &&
		strings.TrimSpace(opts.canvas) == "" &&
		strings.TrimSpace(opts.output) == "" &&
		len(args) == 0
}

func buildRawSceneSpec(opts rawSceneCommandOptions, args []string) (rawSceneSpec, error) {
	outputURL, err := resolveSceneOutput(opts.output, args)
	if err != nil {
		return rawSceneSpec{}, err
	}
	if strings.TrimSpace(opts.streamID) == "" {
		return rawSceneSpec{}, fmt.Errorf("--stream-id is required")
	}
	if len(opts.inputs) == 0 {
		return rawSceneSpec{}, fmt.Errorf("at least one --input is required")
	}
	if opts.outputFPS <= 0 {
		return rawSceneSpec{}, fmt.Errorf("--fps must be greater than zero")
	}
	if opts.startupTimeout <= 0 {
		return rawSceneSpec{}, fmt.Errorf("--startup-timeout must be greater than zero")
	}
	if opts.audioFrom < 0 || opts.audioFrom >= len(opts.inputs) {
		return rawSceneSpec{}, fmt.Errorf("--audio-from must be between 0 and %d", len(opts.inputs)-1)
	}

	audioRatios, err := rawstreamer.NormalizeAudioMixRatios(len(opts.inputs), opts.audioRatios)
	if err != nil {
		return rawSceneSpec{}, err
	}

	canvas, hasCanvas, err := parseCanvasSpec(opts.canvas)
	if err != nil {
		return rawSceneSpec{}, err
	}

	layouts := make([]raw.VideoLayout, 0, len(opts.layouts))
	for idx, layoutValue := range opts.layouts {
		layout, err := parseVideoLayout(layoutValue)
		if err != nil {
			return rawSceneSpec{}, fmt.Errorf("parse --layout %d: %w", idx+1, err)
		}
		layouts = append(layouts, layout)
	}

	switch {
	case len(layouts) == 0 && len(opts.inputs) == 1 && hasCanvas:
		layouts = []raw.VideoLayout{{
			Width:  canvas.Width,
			Height: canvas.Height,
		}}
	case len(layouts) != len(opts.inputs):
		return rawSceneSpec{}, fmt.Errorf("--layout count must match --input count")
	}

	if !hasCanvas {
		canvas, err = deriveCanvas(layouts)
		if err != nil {
			return rawSceneSpec{}, err
		}
	}

	return rawSceneSpec{
		streamID:       strings.TrimSpace(opts.streamID),
		inputURLs:      append([]string(nil), opts.inputs...),
		layouts:        layouts,
		canvas:         canvas,
		outputURL:      outputURL,
		hlsOptions:     rawSceneHLSOptions(opts),
		audioFrom:      opts.audioFrom,
		audioRatios:    audioRatios,
		outputFPS:      opts.outputFPS,
		startupTimeout: opts.startupTimeout,
	}, nil
}

func runRawSceneCommand(parent context.Context, spec rawSceneSpec) error {
	if parent == nil {
		parent = context.Background()
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	service := rawstreamer.NewService()
	return service.RunComposer(
		ctx,
		rawstreamer.Spec{
			StreamID:       spec.streamID,
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
	)
}

func rawSceneHLSOptions(opts rawSceneCommandOptions) *streamfactory.HLSOutputOptions {
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
