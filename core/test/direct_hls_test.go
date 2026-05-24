package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	streaminputs "github.com/tupicapp/restreamer/core/inputs"
	"github.com/tupicapp/restreamer/core/outputs"
	"github.com/tupicapp/restreamer/core/storage"
)

// TestDirectHLSPassthrough streams directly from HLS input to HLS output without Streamer,
// and verifies frame count matches using ffprobe.
func TestDirectHLSPassthrough(t *testing.T) {
	testCases := []struct {
		name string
		url  string
	}{
		{"miladNob", miladNobURL},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testDirectHLSPassthroughWithURL(t, tc.url)
		})
	}
}

func testDirectHLSPassthroughWithURL(t *testing.T, sourceURL string) {
	requireHTTPReachable(t, sourceURL, 1*time.Second)

	outDir := "./testdata_direct/"
	os.RemoveAll(outDir)
	os.MkdirAll(outDir, 0755)
	defer os.RemoveAll(outDir)

	outFolder := storage.NewFolder(outDir)

	// Create HLS output destination
	hlsDest, err := outputs.NewHLSLiveDestination("hls-out",
		outFolder,
		outputs.WithHLSLiveMode(),
		outputs.WithHLSSegmentDuration(2*time.Second),
		outputs.WithHLSPlaylistSize(30),
	)
	if err != nil {
		t.Fatalf("NewHLSLiveDestination: %v", err)
	}

	// Create HLS input
	hlsInput := streaminputs.NewHLSLive("hls-input", sourceURL)

	// Get channels
	videoIn := hlsInput.GetVideoChan()
	audioIn := hlsInput.GetAudioChan()
	videoOut := hlsDest.GetVideoChan()
	audioOut := hlsDest.GetAudioChan()

	stopChan := make(chan struct{})
	var videoCount, audioCount int64
	lastFrameTime := time.Now()
	noFrameTimeout := 2 * time.Second

	// Goroutine 1: read video frames and pass to output
	go func() {
		for {
			select {
			case <-stopChan:
				return
			case frame := <-videoIn:
				if frame == nil {
					return
				}
				videoCount++
				lastFrameTime = time.Now()
				select {
				case videoOut <- frame:
				case <-stopChan:
					return
				}
			}
		}
	}()

	// Goroutine 2: read audio frames and pass to output
	go func() {
		for {
			select {
			case <-stopChan:
				return
			case frame := <-audioIn:
				if frame == nil {
					return
				}
				audioCount++
				lastFrameTime = time.Now()
				select {
				case audioOut <- frame:
				case <-stopChan:
					return
				}
			}
		}
	}()

	// Start both input and output
	hlsInput.Start()
	hlsDest.Start()

	// Wait for both to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := hlsInput.(interface{ WaitForStart(context.Context) error }).WaitForStart(ctx); err != nil {
		t.Fatalf("hls input failed to start: %v", err)
	}
	if err := hlsDest.WaitForStart(ctx); err != nil {
		t.Fatalf("hls dest failed to start: %v", err)
	}

	// Monitor for no frames timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if time.Since(lastFrameTime) > noFrameTimeout {
			t.Logf("No frames for %v, stopping", noFrameTimeout)
			break
		}
	}

	close(stopChan)
	time.Sleep(1500 * time.Millisecond)

	// Close everything
	hlsInput.Close()
	hlsDest.Close()

	// Wait for writes to complete
	time.Sleep(1 * time.Second)

	t.Logf("Video frames passed: %d", videoCount)
	t.Logf("Audio frames passed: %d", audioCount)

	// Get frame count from input using ffprobe
	inputPlaylist := sourceURL
	outputPlaylist := outDir + "stream.m3u8"

	hlsErr := EqualHLS(inputPlaylist, outputPlaylist)

	if hlsErr.ProbeError1 != nil {
		t.Errorf("ffprobe failed on input playlist: %v", hlsErr.ProbeError1)
	}
	if hlsErr.ProbeError2 != nil {
		t.Errorf("ffprobe failed on output playlist: %v", hlsErr.ProbeError2)
	}

	if hlsErr.StreamCountMismatch {
		t.Errorf("Stream count mismatch: input=%d, output=%d",
			hlsErr.StreamCount1, hlsErr.StreamCount2)
	}
	for _, sd := range hlsErr.StreamDiffs {
		t.Errorf("Stream[%d] %s mismatch: input=%q output=%q",
			sd.Index, sd.Field, sd.Value1, sd.Value2)
	}

	if hlsErr.PacketCountMismatch {
		t.Errorf("Packet count mismatch: video input=%d output=%d, audio input=%d output=%d",
			hlsErr.VideoPacketCount1, hlsErr.VideoPacketCount2,
			hlsErr.AudioPacketCount1, hlsErr.AudioPacketCount2)
	}

	countErrors := 0
	errorPrints := 100
	for _, pd := range hlsErr.PacketDiffs {
		countErrors++
		if pd.Field == "data" {
			t.Errorf("Packet[%s #%d] %s mismatch",
				pd.CodecType, pd.Index, pd.Field)

			continue
		}

		t.Errorf("Packet[%s #%d] %s mismatch: input=%q output=%q",
			pd.CodecType, pd.Index, pd.Field, pd.Value1, pd.Value2)
		if countErrors >= errorPrints {
			break
		}
	}
}

type HLSErrors struct {
	ProbeError1 error
	ProbeError2 error

	StreamCountMismatch bool
	StreamCount1        int
	StreamCount2        int
	StreamDiffs         []StreamDiff
	PacketCountMismatch bool
	VideoPacketCount1   int
	VideoPacketCount2   int
	AudioPacketCount1   int
	AudioPacketCount2   int
	PacketDiffs         []PacketDiff
}

type StreamDiff struct {
	Index  int
	Field  string
	Value1 string
	Value2 string
}

type PacketDiff struct {
	Index     int
	CodecType string
	Field     string
	Value1    string
	Value2    string
}

func EqualHLS(playlistURL_1 string, playlistURL_2 string) HLSErrors {
	var errs HLSErrors

	streamData_in, err := dumpFrames(playlistURL_1)
	if err != nil {
		errs.ProbeError1 = err
		return errs
	}

	streamData_out, err := dumpFrames(playlistURL_2)
	if err != nil {
		errs.ProbeError2 = err
		return errs
	}

	// Compare streams (codec, dimensions, extradata)
	errs.StreamCount1 = len(streamData_in.Streams)
	errs.StreamCount2 = len(streamData_out.Streams)
	if errs.StreamCount1 != errs.StreamCount2 {
		errs.StreamCountMismatch = true
	}

	minStreams := errs.StreamCount1
	if errs.StreamCount2 < minStreams {
		minStreams = errs.StreamCount2
	}
	for i := 0; i < minStreams; i++ {
		s1 := streamData_in.Streams[i]
		s2 := streamData_out.Streams[i]
		if s1.CodecName != s2.CodecName {
			errs.StreamDiffs = append(errs.StreamDiffs, StreamDiff{i, "codec_name", s1.CodecName, s2.CodecName})
		}
		if s1.CodecType != s2.CodecType {
			errs.StreamDiffs = append(errs.StreamDiffs, StreamDiff{i, "codec_type", s1.CodecType, s2.CodecType})
		}
		if s1.Width != s2.Width {
			errs.StreamDiffs = append(errs.StreamDiffs, StreamDiff{i, "width", strconv.Itoa(s1.Width), strconv.Itoa(s2.Width)})
		}
		if s1.Height != s2.Height {
			errs.StreamDiffs = append(errs.StreamDiffs, StreamDiff{i, "height", strconv.Itoa(s1.Height), strconv.Itoa(s2.Height)})
		}
		if !bytes.Equal(normalizeFFprobeHexDump(s1.Extradata), normalizeFFprobeHexDump(s2.Extradata)) {
			errs.StreamDiffs = append(errs.StreamDiffs, StreamDiff{i, "extradata", s1.Extradata, s2.Extradata})
		}
	}

	// Split packets by codec type
	vIn, aIn := splitPacketsByType(streamData_in.Packets)
	vOut, aOut := splitPacketsByType(streamData_out.Packets)
	vIn = alignInputPackets(vIn, vOut)
	aIn = alignInputPackets(aIn, aOut)

	errs.VideoPacketCount1 = len(vIn)
	errs.VideoPacketCount2 = len(vOut)
	errs.AudioPacketCount1 = len(aIn)
	errs.AudioPacketCount2 = len(aOut)

	if errs.VideoPacketCount1 != errs.VideoPacketCount2 || errs.AudioPacketCount1 != errs.AudioPacketCount2 {
		errs.PacketCountMismatch = true
	}

	errs.PacketDiffs = append(errs.PacketDiffs, comparePackets("video", vIn, vOut)...)
	errs.PacketDiffs = append(errs.PacketDiffs, comparePackets("audio", aIn, aOut)...)

	return errs
}

func splitPacketsByType(pkts []Packet) (video, audio []Packet) {
	for _, p := range pkts {
		switch p.CodecType {
		case "video":
			video = append(video, p)
		case "audio":
			audio = append(audio, p)
		}
	}
	return
}

func comparePackets(codecType string, p1, p2 []Packet) []PacketDiff {
	var diffs []PacketDiff
	basePTSInt1, basePTSInt2 := firstPacketIntBases(p1, p2, func(p Packet) string { return string(p.Pts) })
	basePTSTime1, basePTSTime2 := firstPacketFloatBases(p1, p2, func(p Packet) string { return string(p.PtsTime) })
	baseDTSInt1, baseDTSInt2 := firstPacketIntBases(p1, p2, func(p Packet) string { return string(p.Dts) })
	baseDTSTime1, baseDTSTime2 := firstPacketFloatBases(p1, p2, func(p Packet) string { return string(p.DtsTime) })
	n := len(p1)
	if len(p2) < n {
		n = len(p2)
	}
	for i := 0; i < n; i++ {
		a := p1[i]
		b := p2[i]
		if !equalPacketTimestampInt(a.Pts, b.Pts, basePTSInt1, basePTSInt2) {
			diffs = append(diffs, PacketDiff{i, codecType, "pts", string(a.Pts), string(b.Pts)})
		}
		if !equalPacketTimestampFloat(a.PtsTime, b.PtsTime, basePTSTime1, basePTSTime2) {
			diffs = append(diffs, PacketDiff{i, codecType, "pts_time", string(a.PtsTime), string(b.PtsTime)})
		}
		if !equalPacketTimestampInt(a.Dts, b.Dts, baseDTSInt1, baseDTSInt2) {
			diffs = append(diffs, PacketDiff{i, codecType, "dts", string(a.Dts), string(b.Dts)})
		}
		if !equalPacketTimestampFloat(a.DtsTime, b.DtsTime, baseDTSTime1, baseDTSTime2) {
			diffs = append(diffs, PacketDiff{i, codecType, "dts_time", string(a.DtsTime), string(b.DtsTime)})
		}
		if a.Duration != b.Duration {
			diffs = append(diffs, PacketDiff{i, codecType, "duration", string(a.Duration), string(b.Duration)})
		}
		comparePayload := codecType != "video"
		if comparePayload && a.Size != b.Size && !equalPacketData(a.Data, b.Data) {
			diffs = append(diffs, PacketDiff{i, codecType, "size", string(a.Size), string(b.Size)})
		}
		if a.Flags != b.Flags {
			diffs = append(diffs, PacketDiff{i, codecType, "flags", a.Flags, b.Flags})
		}
		if comparePayload && !equalPacketData(a.Data, b.Data) {
			diffs = append(diffs, PacketDiff{i, codecType, "data", a.Data, b.Data})
		}
	}
	return diffs
}

func alignInputPackets(input, output []Packet) []Packet {
	if len(input) == 0 || len(output) == 0 || len(input) <= len(output) {
		return input
	}

	maxOffset := len(input) - len(output)
	for offset := 0; offset <= maxOffset; offset++ {
		if packetLooksEquivalent(input[offset], output[0]) {
			return input[offset:]
		}
	}

	return input
}

func packetLooksEquivalent(a, b Packet) bool {
	if a.CodecType != b.CodecType {
		return false
	}
	if strings.TrimSpace(a.Flags) != strings.TrimSpace(b.Flags) {
		return false
	}
	if strings.TrimSpace(string(a.Duration)) != strings.TrimSpace(string(b.Duration)) {
		return false
	}
	if equalPacketData(a.Data, b.Data) {
		return true
	}

	return strings.TrimSpace(string(a.Size)) == strings.TrimSpace(string(b.Size))
}

func firstPacketIntBases(p1, p2 []Packet, field func(Packet) string) (int64, int64) {
	return firstPacketIntBase(p1, field), firstPacketIntBase(p2, field)
}

func firstPacketFloatBases(p1, p2 []Packet, field func(Packet) string) (float64, float64) {
	return firstPacketFloatBase(p1, field), firstPacketFloatBase(p2, field)
}

func firstPacketIntBase(pkts []Packet, field func(Packet) string) int64 {
	for _, pkt := range pkts {
		value := strings.TrimSpace(field(pkt))
		if value == "" || value == "N/A" {
			continue
		}
		base, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return base
		}
	}
	return 0
}

func firstPacketFloatBase(pkts []Packet, field func(Packet) string) float64 {
	for _, pkt := range pkts {
		value := strings.TrimSpace(field(pkt))
		if value == "" || value == "N/A" {
			continue
		}
		base, err := strconv.ParseFloat(value, 64)
		if err == nil {
			return base
		}
	}
	return 0
}

func equalPacketTimestampInt(a, b flexString, baseA, baseB int64) bool {
	sa := strings.TrimSpace(string(a))
	sb := strings.TrimSpace(string(b))
	if sa == sb {
		return true
	}
	if sa == "" || sb == "" || sa == "N/A" || sb == "N/A" {
		return sa == sb
	}

	av, errA := strconv.ParseInt(sa, 10, 64)
	bv, errB := strconv.ParseInt(sb, 10, 64)
	if errA != nil || errB != nil {
		return false
	}
	delta := (av - baseA) - (bv - baseB)
	if delta < 0 {
		delta = -delta
	}
	return delta <= 1
}

func equalPacketTimestampFloat(a, b flexString, baseA, baseB float64) bool {
	sa := strings.TrimSpace(string(a))
	sb := strings.TrimSpace(string(b))
	if sa == sb {
		return true
	}
	if sa == "" || sb == "" || sa == "N/A" || sb == "N/A" {
		return sa == sb
	}

	av, errA := strconv.ParseFloat(sa, 64)
	bv, errB := strconv.ParseFloat(sb, 64)
	if errA != nil || errB != nil {
		return false
	}
	return nearlyEqualFloat(av-baseA, bv-baseB, 0.0005)
}

func nearlyEqualFloat(a, b, eps float64) bool {
	if a > b {
		return a-b <= eps
	}
	return b-a <= eps
}

func equalPacketData(a, b string) bool {
	return bytes.Equal(normalizeFFprobeHexDump(a), normalizeFFprobeHexDump(b))
}

func normalizeFFprobeHexDump(dump string) []byte {
	var out []byte
	for _, line := range strings.Split(dump, "\n") {
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		hexPart := line[colon+1:]
		if sep := strings.Index(hexPart, "  "); sep >= 0 {
			hexPart = hexPart[:sep]
		}
		for _, field := range strings.Fields(hexPart) {
			field = strings.TrimSpace(field)
			if len(field)%2 != 0 {
				continue
			}
			for i := 0; i < len(field); i += 2 {
				v, err := strconv.ParseUint(field[i:i+2], 16, 8)
				if err != nil {
					continue
				}
				out = append(out, byte(v))
			}
		}
	}
	return bytes.TrimRight(out, "\x00")
}

// getFFprobeFrameCount returns the total number of video frames in an HLS playlist using ffprobe
func getFFprobeFrameCount(playlistURL string) (int, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-count_frames",
		"-show_entries", "stream=nb_read_frames",
		"-of", "csv=p=0",
		playlistURL,
	)

	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("ffprobe failed: %w", err)
	}

	// ffprobe may output multiple lines, take the first non-empty one
	lines := strings.Split(string(output), "\n")
	var countStr string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			countStr = line
			break
		}
	}

	if countStr == "" {
		return 0, fmt.Errorf("ffprobe returned empty output")
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		return 0, fmt.Errorf("could not parse frame count '%s': %w", countStr, err)
	}

	return count, nil
}

type FFProbeOutput struct {
	Packets []Packet     `json:"packets"`
	Streams []StreamInfo `json:"streams"`
}

type Packet struct {
	CodecType   string `json:"codec_type"`
	StreamIndex int    `json:"stream_index"`

	Pts     flexString `json:"pts"`
	PtsTime flexString `json:"pts_time"`
	Dts     flexString `json:"dts"`
	DtsTime flexString `json:"dts_time"`

	Duration flexString `json:"duration"`
	Size     flexString `json:"size"`
	Flags    string     `json:"flags"`

	Data string `json:"data"`
}

// flexString accepts either a JSON string or a JSON number and stores it as a string.
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		*f = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = flexString(s)
		return nil
	}
	*f = flexString(string(b))
	return nil
}

type StreamInfo struct {
	CodecName string `json:"codec_name"`
	CodecType string `json:"codec_type"`

	Width  int `json:"width"`
	Height int `json:"height"`

	Extradata string `json:"extradata"`
}

func dumpFrames(playlistURL string) (*FFProbeOutput, error) {
	cmd := exec.Command(
		"ffprobe",
		"-v", "error",

		// show packets instead of just frame count
		"-show_packets",

		// show stream info (codec, SPS/PPS extradata)
		"-show_streams",

		// include packet payload
		"-show_data",

		// json easier to parse
		"-print_format", "json",

		playlistURL,
	)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	var result FFProbeOutput

	err = json.Unmarshal(output, &result)
	if err != nil {
		return nil, fmt.Errorf("json parse failed: %w", err)
	}

	return &result, nil
}
