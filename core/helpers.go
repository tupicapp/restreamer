//nolint:all
package irajstreamer

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	mp4codecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mp4/codecs"
)

const (
	// Video
	DefaultWidth             = 1280
	DefaultHeight            = 720
	DefaultFPS2              = 60 // 60 if you want super smooth, but 30 is the safe default
	DefaultFPS               = 30 // 60 if you want super smooth, but 30 is the safe default
	DefaultErrorLoggerBuffer = 1024
	DefaultPixFormat         = "yuv420p"

	// Audio
	DefaultAudioChannels = 2           // Stereo
	DefaultAudioProfile  = 2           // 48 kHz, broadcast standard
	DefaultAudioRate     = 44100       // 48 kHz, broadcast standard
	DefaultAudioFormat   = "s16le"     // PCM
	DefaultAudioCodec    = "aac"       // PCM
	DefaultPreset        = "ultrafast" // PCM
	DefaultVideoFormat   = "libx264"

	DefaultChannelBufferSize  = 100
	DefaultReadDeadline       = time.Millisecond * 100
	DefaultWriteDeadline      = time.Millisecond * 100
	DefaultMaxAcceptedLatancy = time.Millisecond * 50
)

func GenerateSilentAACFrame() *Frame {
	// Below is a valid 2-byte AAC LC silence frame used by many muxers.
	// [33 26 75 255 255 255 255 253 196 2 136 178 165 24 136 55 183 189 239 151 61 31 233 253 107 141 86 181 74 185 226 245 80 19 124 86 0 86 138 20 151 90 130 154 239 173 168 30 10 2 38 77 138 124 172 156 9 57 5 68 90 143 105 35 2 1 26 22 8 216 176 70 198 3 186 115 75 165 43 33 91 107 34 130 130 130 169 254 170 136 43 227 65 73 104 5 5 5 5 5 52 40 111 205 127 226 253 169 217 27 16 83 187 133 5 29 136 40 235 187 184 208 87 10 176 40 40 41 237 5 20 20 21 243 175 249 95 26 10 42 10 203 76 133 5 59 64 180 26 10 27 68 23 254 43 60 87 66 130 130 162 58 249 2 216 21 10 172 58 54 42 127 202 130 255 43 134 14 12 216 226 200 12 204 142 226 116 227 139 54 51 54 61 126 108 88 0 13 9 240 195 7 0 39 115 98 174 74 240 26 113 198 102 102 96 175 249 71 93 225 38 104 238 97 69 65 65 65 126 107 210 157 172 40 40 41 221 196 2 63 75 219 222 247 203 158 143 244 254 181 198 171 90 165 92 241 122 168 9 190 43 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 7]
	// payload := []byte{
	// 	33, 26, 75, 255, 255, 255, 255, 253, 196, 2, 136, 178, 165, 24, 136, 55,
	// 	183, 189, 239, 151, 61, 31, 233, 253, 107, 141, 86, 181, 74, 185, 226, 245,
	// 	80, 19, 124, 86, 0, 86, 138, 20, 151, 90, 130, 154, 239, 173, 168, 30,
	// 	10, 2, 38, 77, 138, 124, 172, 156, 9, 57, 5, 68, 90, 143, 105, 35,
	// 	2, 1, 26, 22, 8, 216, 176, 70, 198, 3, 186, 115, 75, 165, 43, 33,
	// 	91, 107, 34, 130, 130, 130, 169, 254, 170, 136, 43, 227, 65, 73, 104, 5,
	// 	5, 5, 5, 5, 52, 40, 111, 205, 127, 226, 253, 169, 217, 27, 16, 83,
	// 	187, 133, 5, 29, 136, 40, 235, 187, 184, 208, 87, 10, 176, 40, 40, 41,
	// 	237, 5, 20, 20, 21, 243, 175, 249, 95, 26, 10, 42, 10, 203, 76, 133,
	// 	5, 59, 64, 180, 26, 10, 27, 68, 23, 254, 43, 60, 87, 66, 130, 130,
	// 	162, 58, 249, 2, 216, 21, 10, 172, 58, 54, 42, 127, 202, 130, 255, 43,
	// 	134, 14, 12, 216, 226, 200, 12, 204, 142, 226, 116, 227, 139, 54, 51, 54,
	// 	61, 126, 108, 88, 0, 13, 9, 240, 195, 7, 0, 39, 115, 98, 174, 74,
	// 	240, 26, 113, 198, 102, 102, 96, 175, 249, 71, 93, 225, 38, 104, 238, 97,
	// 	69, 65, 65, 65, 126, 107, 210, 157, 172, 40, 40, 41, 221, 196, 2, 63,
	// 	75, 219, 222, 247, 203, 158, 143, 244, 254, 181, 198, 171, 90, 165, 92, 241,
	// 	122, 168, 9, 190, 43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
	// }
	// [33 16 4 96 140 28]
	// these bits are huffman coding dependant bits, dont change it
	payload := []byte{33, 16, 4, 96, 140, 28}

	// Bit stream:
	//  33: 00100001
	//  16: 00010000
	//   4: 00000100
	//  96: 01100000
	// 140: 10001100
	//  28: 00011100
	// Concatenated bitstream:
	// 1 0 10 1111 00000001 00100001 00010000 00000100 01100000 10001100 00011100
	// Is_Streo, Depth, Rate, Codec, AAC-Type,Payload

	// adts := buildADTSHeader2(len(payload), 1, 44100, 2)

	// var SilentAAC44100Stereo = []byte{
	// 	0xFF, 0xF1, 0x50, 0x80, 0x03, 0x1F, 0xFC,
	// 	0x00, 0x20, 0x00, 0x00, 0x00,
	// }

	frame := &Frame{
		PTS:        0,
		DTS:        0,
		Payload:    [][]byte{payload},
		Codec:      "aac-lc",
		Timestamp:  time.Now(),
		InputID:    "silent-audio",
		IsKeyFrame: true,
	}

	return frame
}

// buildADTSHeader generates a 7-byte ADTS header for AAC-LC
func buildADTSHeader(frameLength int, profile int, sampleRate int, channels int) []byte {
	header := make([]byte, 7)

	sampleRateIndex := map[int]int{
		96000: 0, 88200: 1, 64000: 2, 48000: 3, 44100: 4,
		32000: 5, 24000: 6, 22050: 7, 16000: 8, 12000: 9,
		11025: 10, 8000: 11, 7350: 12,
	}

	srIndex := sampleRateIndex[sampleRate]
	fullLength := frameLength + 7 // ADTS header + AAC frame

	header[0] = 0xFF
	header[1] = 0xF1
	header[2] = byte(((profile-1)<<6)&0xC0 | (srIndex<<2)&0x3C | (channels>>2)&0x01)
	header[3] = byte(((channels & 0x3) << 6) | ((fullLength >> 11) & 0x03))
	header[4] = byte((fullLength >> 3) & 0xFF)
	header[5] = byte(((fullLength & 0x7) << 5) | 0x1F)
	header[6] = 0xFC

	return header
}

// IsValidH264Packet is a simple validation for H264 raw frames (NAL unit start code 0x00000001 or 0x000001)
func IsValidH264Packet(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	// Check for H264 NAL unit start codes
	// 0x00 00 00 01 or 0x00 00 01
	if data[0] == 0x00 && data[1] == 0x00 && ((data[2] == 0x01) || (data[2] == 0x00 && data[3] == 0x01)) {
		return true
	}
	return false
}

// IsValidAACMPEG4AudioPacket is a simple check for AAC ADTS header (syncword 0xFFF)
func IsValidAACMPEG4AudioPacket(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	if (data[0] == 0xFF) && ((data[1] & 0xF0) == 0xF0) {
		return true
	}
	return false
}

func CreateBlackH264Frame() *Frame {
	// Pre-encoded SPS + PPS + I-frame for black frame
	// This is a minimal H.264 IDR frame for 1920x1080
	// Normally you can generate it with x264 once and embed the bytes
	sps := []byte{0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, 0xe5, 0x88, 0x68, 0x50, 0x1e, 0xd0} // example
	pps := []byte{0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2}                                     // example
	idr := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00, 0x00, 0x00}                         // minimal black IDR

	now := time.Now()
	return &Frame{
		PTS:        0,
		DTS:        0,
		Payload:    [][]byte{sps, pps, idr},
		Codec:      "h264",
		Timestamp:  now,
		InputID:    "black_frame",
		IsKeyFrame: true,
	}
}

func isFFmpegAlive(cmd *exec.Cmd) bool {
	//  Has Go marked it as exited?
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return false
	}

	//  Check if PID exists in /proc (Linux only)
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", cmd.Process.Pid)); os.IsNotExist(err) {
		return false
	}

	return true
}

func FFmpegReadable(file string) error {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-i", file,
		"-t", "0",
		"-f", "null", "-",
	}

	cmd := exec.Command("ffmpeg", args...)

	ffmpegDone := make(chan error)

	go func() {
		ffmpegDone <- cmd.Run()
	}()

	select {
	case err := <-ffmpegDone:
		if err != nil {
			return err
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("ffmpeg input [%v] is readable timeout ", file)
	}

	return nil
}

func FFmpegWritable(url string) error {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=black:s=1280x720:r=1",
		"-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
		"-t", "0.1",
		"-c:v", "libx264",
		"-pix_fmt", "yuv420p",
		"-c:a", "aac",
		"-frames:v", "1",
		"-f", "flv",
		url,
	}

	cmd := exec.Command("ffmpeg", args...)

	ffmpegDone := make(chan error)

	go func() {
		ffmpegDone <- cmd.Run()
	}()

	select {
	case err := <-ffmpegDone:
		if err != nil {
			return err
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("ffmpeg output [%v] is writable timeout ", url)
	}

	return nil
}

// StreamInfo holds metadata about a media stream
type StreamInfo struct {
	// Video
	Width        int
	Height       int
	FPS          float64
	VideoCodec   string
	PixFmt       string
	VideoBitrate int // in kbps

	// Audio
	AudioChannels int
	AudioRate     int // Hz
	AudioCodec    string
	AudioBitrate  int // in kbps

	// Overall
	Format   string
	Duration float64 // seconds
}

// FFprobeJSON format
type ffprobeOutput struct {
	Format struct {
		FormatName string `json:"format_name"`
		Duration   string `json:"duration"`
	} `json:"format"`
	Streams []struct {
		CodecName  string `json:"codec_name"`
		Width      int    `json:"width,omitempty"`
		Height     int    `json:"height,omitempty"`
		PixFmt     string `json:"pix_fmt,omitempty"`
		BitRate    string `json:"bit_rate,omitempty"`
		RFrameRate string `json:"r_frame_rate,omitempty"`
		Channels   int    `json:"channels,omitempty"`
		SampleRate string `json:"sample_rate,omitempty"`
	} `json:"streams"`
}

// ProbeStream extracts detailed stream info using ffprobe (single call)
func ProbeStream(url string) (*StreamInfo, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-show_entries", "format=format_name,duration",
		"-show_entries", "stream",
		"-of", "json",
		url,
	)

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe error: %w", err)
	}

	var fp ffprobeOutput
	if err := json.Unmarshal(out, &fp); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %w", err)
	}

	info := &StreamInfo{}
	info.Format = fp.Format.FormatName
	info.Duration, _ = strconv.ParseFloat(fp.Format.Duration, 64)

	for _, s := range fp.Streams {
		if s.Width > 0 && s.Height > 0 {
			// Video stream
			info.Width = s.Width
			info.Height = s.Height
			info.VideoCodec = s.CodecName
			info.PixFmt = s.PixFmt
			if s.BitRate != "" {
				if v, err := strconv.Atoi(s.BitRate); err == nil {
					info.VideoBitrate = v / 1000 // convert to kbps
				}
			}
			// FPS
			if s.RFrameRate != "" && s.RFrameRate != "0/0" {
				parts := [2]float64{}
				fmt.Sscanf(s.RFrameRate, "%f/%f", &parts[0], &parts[1])
				if parts[1] != 0 {
					info.FPS = parts[0] / parts[1]
				}
			}
		} else if s.Channels > 0 {
			// Audio stream
			info.AudioChannels = s.Channels
			info.AudioCodec = s.CodecName
			if s.SampleRate != "" {
				if sr, err := strconv.Atoi(s.SampleRate); err == nil {
					info.AudioRate = sr
				}
			}
			if s.BitRate != "" {
				if v, err := strconv.Atoi(s.BitRate); err == nil {
					info.AudioBitrate = v / 1000
				}
			}
		}
	}

	return info, nil
}

func addDefaultRTMPPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	if !strings.Contains(u.Host, ":") {
		u.Host = u.Host + ":1935"
	}

	return u.String()
}

func SanitizeFileName(name string) string {
	replacer := strings.NewReplacer("/", "_", ":", "_", "\\", "_", " ", "_")
	return replacer.Replace(name)
}

func ClassifyVideoPacketType(au [][]byte, codec string) string {
	switch codec {
	case "h264":
		return classifyH264PacketType(au)
	case "h265":
		return classifyH265PacketType(au)
	default:
		return "unknown"
	}
}

func classifyH264PacketType(au [][]byte) string {
	for _, nalu := range au {
		if len(nalu) == 0 {
			continue
		}
		nalType := nalu[0] & 0x1F
		switch nalType {
		case 5:
			return "I"
		case 1:
			if sliceType, err := parseH264SliceType(nalu); err == nil {
				return mapH264SliceType(sliceType)
			}
			return "P"
		default:
			continue
		}
	}
	return "unknown"
}

func classifyH265PacketType(au [][]byte) string {
	for _, nalu := range au {
		if len(nalu) < 2 {
			continue
		}
		nalType := (nalu[0] >> 1) & 0x3F
		if nalType >= 16 && nalType <= 21 {
			return "I"
		}
		if nalType <= 31 {
			return "P"
		}
	}
	return "unknown"
}

func parseH264SliceType(nalu []byte) (int, error) {
	if len(nalu) <= 1 {
		return 0, io.EOF
	}

	rbsp := removeEmulationPreventionBytes(nalu[1:])
	if len(rbsp) == 0 {
		return 0, io.EOF
	}

	br := newBitReader(rbsp)
	if _, err := br.readUE(); err != nil {
		return 0, err
	}

	sliceType, err := br.readUE()
	if err != nil {
		return 0, err
	}

	return int(sliceType), nil
}

func mapH264SliceType(sliceType int) string {
	switch sliceType {
	case 0, 5:
		return "P"
	case 1, 6:
		return "B"
	case 2, 7:
		return "I"
	case 3, 8:
		return "SP"
	case 4, 9:
		return "SI"
	default:
		return "unknown"
	}
}

func removeEmulationPreventionBytes(data []byte) []byte {
	if len(data) < 3 {
		return append([]byte(nil), data...)
	}

	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if i+2 < len(data) && data[i] == 0x00 && data[i+1] == 0x00 && data[i+2] == 0x03 {
			out = append(out, 0x00, 0x00)
			i += 2
			continue
		}
		out = append(out, data[i])
	}

	return out
}

type bitReader struct {
	data    []byte
	bytePos int
	bitPos  uint8
}

func newBitReader(data []byte) *bitReader {
	return &bitReader{data: data}
}

func (b *bitReader) readBits(n int) (uint32, error) {
	var value uint32
	for i := 0; i < n; i++ {
		if b.bytePos >= len(b.data) {
			return 0, io.EOF
		}
		bit := (b.data[b.bytePos] >> (7 - b.bitPos)) & 0x1
		value = (value << 1) | uint32(bit)
		b.bitPos++
		if b.bitPos == 8 {
			b.bitPos = 0
			b.bytePos++
		}
	}
	return value, nil
}

func (b *bitReader) readUE() (uint32, error) {
	leadingZeroBits := 0
	for {
		bit, err := b.readBits(1)
		if err != nil {
			return 0, err
		}
		if bit == 0 {
			leadingZeroBits++
			continue
		}
		break
	}

	if leadingZeroBits == 0 {
		return 0, nil
	}

	suffix, err := b.readBits(leadingZeroBits)
	if err != nil {
		return 0, err
	}

	return ((1 << leadingZeroBits) - 1) + suffix, nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func normalizeHLSURI(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("empty uri")
	}

	u, err := url.Parse(trimmed)
	if err == nil && u.Scheme != "" {
		return trimmed, nil
	}

	absPath, err := filepath.Abs(trimmed)
	if err != nil {
		return "", err
	}

	fileURL := &url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(absPath),
	}

	return fileURL.String(), nil
}

func AddSPStoKeyFrame(frame *Frame) {
	if frame == nil {
		return
	}

	spsPps := []byte{}

	payload := frame.Payload
	if frame.IsKeyFrame && len(spsPps) > 0 {
		payload = append([][]byte{spsPps}, payload...)
	} else if frame.IsKeyFrame {
		// Extract SPS/PPS from the keyframe if we haven't yet
		for _, nalu := range frame.Payload {
			if len(nalu) == 0 {
				continue
			}
			typ := nalu[0] & 0x1F
			if typ == 7 || typ == 8 { // SPS=7, PPS=8
				spsPps = append(spsPps, prependStartCode(nalu)...)
			}
		}
	}

	frame.Payload = payload
}

func IsTsKeyFrame(frame *Frame) bool {
	if frame == nil {
		return false
	}

	switch frame.Codec {
	case "h265":
		for _, nalu := range frame.Payload {
			if len(nalu) == 0 {
				continue
			}
			typ := (nalu[0] >> 1) & 0x3F
			if typ == 19 || typ == 20 || typ == 21 {
				return true
			}
		}
	default:
		for _, nalu := range frame.Payload {
			if len(nalu) == 0 {
				continue
			}
			if nalu[0]&0x1F == 5 {
				return true
			}
		}
	}

	return false
}

func mp4CodecToString(codec mp4codecs.Codec) (string, bool) {
	switch codec.(type) {
	case *mp4codecs.H264:
		return "h264", true
	case *mp4codecs.H265:
		return "h265", true
	case *mp4codecs.MPEG4Audio:
		return "aac", true
	case *mp4codecs.Opus:
		return "opus", true
	case *mp4codecs.MPEG1Audio:
		return "mpeg1audio", true
	case *mp4codecs.AC3:
		return "ac3", true
	default:
		return "", false
	}
}

// BuildADTSHeader exposes ADTS header generation for moved-layout packages.
func BuildADTSHeader(frameLength int, profile int, sampleRate int, channels int) []byte {
	return buildADTSHeader(frameLength, profile, sampleRate, channels)
}

// AddDefaultRTMPPort exposes RTMP URL normalization for moved-layout packages.
func AddDefaultRTMPPort(raw string) string {
	return addDefaultRTMPPort(raw)
}

// NormalizeHLSURI exposes HLS URI normalization for moved-layout packages.
func NormalizeHLSURI(raw string) (string, error) {
	return normalizeHLSURI(raw)
}

func classifyVideoPacketType(au [][]byte, codec string) string {
	switch codec {
	case "h264":
		return classifyH264PacketType(au)
	case "h265":
		return classifyH265PacketType(au)
	default:
		return "unknown"
	}
}

func stripAnnexBStartCode(nalu []byte) []byte {
	if len(nalu) >= 4 && nalu[0] == 0x00 && nalu[1] == 0x00 {
		if nalu[2] == 0x01 {
			return nalu[3:]
		}
		if len(nalu) >= 5 && nalu[2] == 0x00 && nalu[3] == 0x01 {
			return nalu[4:]
		}
	}
	return nalu
}

func h264NALTypeFromUnit(nalu []byte) byte {
	nalu = stripAnnexBStartCode(nalu)
	if len(nalu) == 0 {
		return 0
	}
	return nalu[0] & 0x1F
}

func H264SPSPPSPresent(nalus [][]byte) (bool, bool) {
	hasSPS := false
	hasPPS := false
	for _, nalu := range nalus {
		switch h264NALTypeFromUnit(nalu) {
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
		if hasSPS && hasPPS {
			return true, true
		}
	}
	return hasSPS, hasPPS
}

func stripAnnexB(nalu []byte) []byte {
	if len(nalu) >= 4 && nalu[0] == 0x00 && nalu[1] == 0x00 {
		if nalu[2] == 0x01 {
			return nalu[3:]
		}
		if len(nalu) >= 5 && nalu[2] == 0x00 && nalu[3] == 0x01 {
			return nalu[4:]
		}
	}
	return nalu
}

func h264ExtractSPSPPS(nalus [][]byte) ([]byte, []byte) {
	var sps []byte
	var pps []byte

	for _, nalu := range nalus {
		switch h264NALTypeFromUnit(nalu) {
		case 7:
			sps = cloneBytes(nalu)
		case 8:
			pps = cloneBytes(nalu)
		}
	}

	return sps, pps
}

func sanitizeFileName(name string) string {
	return SanitizeFileName(name)
}

func prependStartCode(nalu []byte) []byte {
	startCode := []byte{0x00, 0x00, 0x00, 0x01}
	return append(startCode, nalu...)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
