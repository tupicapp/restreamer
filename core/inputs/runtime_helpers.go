package inputs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ilogger "restreamer/irajstreamer/core/logger"
	shared "restreamer/irajstreamer/core/shared"

	mp4codecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mp4/codecs"
	"go.uber.org/zap"
)

type streamManager struct {
	Stream
	startOnce      sync.Once
	closeOnce      sync.Once
	streamsToClose chan Stream
	done           chan struct{}
	events         *shared.EventEmitter
}

func Manage(s Stream) Stream {
	if s.IsRestartable() {
		return &streamManager{
			Stream:         s,
			done:           make(chan struct{}),
			streamsToClose: make(chan Stream, 10),
			events:         shared.NewEventEmitter(128),
		}
	}

	return s
}

func (s *streamManager) Start() {
	s.startOnce.Do(func() {
		s.followStream(s.Stream)
		go s.startWatch()
	})

	s.Stream.Start()
}

func (s *streamManager) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.events != nil {
			s.events.Close()
		}
	})

	s.Stream.Close()
}

func (s *streamManager) EventChan() chan shared.Event {
	if s.events == nil {
		return nil
	}
	return s.events.Chan()
}

func (s *streamManager) startWatch() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), s.RestartInterval())
	if err := s.Stream.WaitForStart(ctx); err != nil {
		getLogger().Error("manager failed to wait for stream to start",
			zap.String("stream_id", s.GetID()),
			zap.Error(err))
	}
	cancel()

	for {
		select {
		case <-ticker.C:
			state := s.State()
			logger := getLogger()

			logger.Info("manager checking stream state",
				zap.String("stream_type", s.Type()),
				zap.String("stream_id", s.GetID()),
				zap.Int64("last_io_ms_ago", time.Since(state.LastIO).Milliseconds()))

			if time.Since(state.LastIO) <= s.RestartInterval() {
				continue
			}

			logger.Warn("manager restarting stream",
				zap.String("stream_type", s.Type()),
				zap.String("stream_id", s.GetID()))

			newStream, err := s.Clone()
			if err != nil {
				logger.Error("manager failed to clone stream for restart",
					zap.String("stream_id", s.GetID()),
					zap.Error(err))
				s.emitEvent(shared.Event{
					Type:       shared.EventTypeStreamError,
					StreamID:   s.GetID(),
					StreamType: s.Type(),
					Message:    "stream manager clone failed",
					Error:      err,
				})
				continue
			}

			oldState := s.Stream.State()
			newStream.Start()
			formerStream := s.Stream

			select {
			case streamToClose := <-s.streamsToClose:
				streamToClose.Close()
				logger.Info("manager closing stream",
					zap.String("stream_type", s.Type()),
					zap.String("stream_id", s.GetID()))
			default:
			}

			go func() {
				s.streamsToClose <- formerStream
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = newStream.WaitForStart(ctx)
			cancel()

			s.Stream = newStream
			s.followStream(newStream)
			if !oldState.IsStarted {
				newStream.Stop()
			}

			logger.Warn("manager stream restarted", zap.String("stream_id", s.GetID()))
		case <-s.done:
			return
		}
	}
}

func (s *streamManager) emitEvent(event shared.Event) {
	if s.events == nil {
		return
	}
	s.events.Emit(event)
}

func (s *streamManager) followStream(stream Stream) {
	if stream == nil || s.events == nil {
		return
	}
	ch := stream.EventChan()
	if ch == nil {
		return
	}
	go func() {
		for {
			select {
			case <-s.done:
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				s.events.Emit(event)
			}
		}
	}()
}

func getLogger() *zap.Logger {
	return ilogger.GetLogger()
}

func buildADTSHeader(frameLength int, profile int, sampleRate int, channels int) []byte {
	header := make([]byte, 7)
	sampleRateIndex := map[int]int{
		96000: 0, 88200: 1, 64000: 2, 48000: 3, 44100: 4,
		32000: 5, 24000: 6, 22050: 7, 16000: 8, 12000: 9,
		11025: 10, 8000: 11, 7350: 12,
	}

	srIndex := sampleRateIndex[sampleRate]
	fullLength := frameLength + 7
	header[0] = 0xFF
	header[1] = 0xF1
	header[2] = byte(((profile-1)<<6)&0xC0 | (srIndex<<2)&0x3C | (channels>>2)&0x01)
	header[3] = byte(((channels & 0x3) << 6) | ((fullLength >> 11) & 0x03))
	header[4] = byte((fullLength >> 3) & 0xFF)
	header[5] = byte(((fullLength & 0x7) << 5) | 0x1F)
	header[6] = 0xFC
	return header
}

func addDefaultRTMPPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if !strings.Contains(u.Host, ":") {
		u.Host += ":1935"
	}
	return u.String()
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

	return (&url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(absPath),
	}).String(), nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
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

func classifyH264PacketType(au [][]byte) string {
	for _, nalu := range au {
		if len(nalu) == 0 {
			continue
		}
		switch nalu[0] & 0x1F {
		case 5:
			return "I"
		case 1:
			sliceType, err := parseH264SliceType(nalu)
			if err != nil {
				return "P"
			}
			return mapH264SliceType(sliceType)
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

	br := newBitCursor(rbsp)
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

type bitCursor struct {
	data    []byte
	bytePos int
	bitPos  uint8
}

func newBitCursor(data []byte) *bitCursor {
	return &bitCursor{data: data}
}

func (b *bitCursor) readBits(n int) (uint32, error) {
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

func (b *bitCursor) readUE() (uint32, error) {
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

type StreamInfo struct {
	Width         int
	Height        int
	FPS           float64
	VideoCodec    string
	PixFmt        string
	VideoBitrate  int
	AudioChannels int
	AudioRate     int
	AudioCodec    string
	AudioBitrate  int
	Format        string
	Duration      float64
}

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

func ProbeStream(rawURL string) (*StreamInfo, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-show_entries", "format=format_name,duration",
		"-show_entries", "stream",
		"-of", "json",
		rawURL,
	)

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe error: %w", err)
	}

	var fp ffprobeOutput
	if err := json.Unmarshal(out, &fp); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %w", err)
	}

	info := &StreamInfo{Format: fp.Format.FormatName}
	info.Duration, _ = strconv.ParseFloat(fp.Format.Duration, 64)

	for _, s := range fp.Streams {
		if s.Width > 0 && s.Height > 0 {
			info.Width = s.Width
			info.Height = s.Height
			info.VideoCodec = s.CodecName
			info.PixFmt = s.PixFmt
			if s.BitRate != "" {
				if v, convErr := strconv.Atoi(s.BitRate); convErr == nil {
					info.VideoBitrate = v / 1000
				}
			}
			if s.RFrameRate != "" && s.RFrameRate != "0/0" {
				parts := [2]float64{}
				fmt.Sscanf(s.RFrameRate, "%f/%f", &parts[0], &parts[1])
				if parts[1] != 0 {
					info.FPS = parts[0] / parts[1]
				}
			}
			continue
		}

		if s.Channels > 0 {
			info.AudioChannels = s.Channels
			info.AudioCodec = s.CodecName
			if s.SampleRate != "" {
				if sr, convErr := strconv.Atoi(s.SampleRate); convErr == nil {
					info.AudioRate = sr
				}
			}
			if s.BitRate != "" {
				if v, convErr := strconv.Atoi(s.BitRate); convErr == nil {
					info.AudioBitrate = v / 1000
				}
			}
		}
	}

	return info, nil
}

func isFFmpegAlive(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 {
		return false
	}

	if _, err := os.Stat(fmt.Sprintf("/proc/%d", cmd.Process.Pid)); os.IsNotExist(err) {
		return false
	}

	return true
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
