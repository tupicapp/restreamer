package outputs

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"restreamer/core/logger"

	"go.uber.org/zap"
)

func getLogger() *zap.Logger {
	return logger.GetLogger()
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

func sanitizeFileName(name string) string {
	replacer := strings.NewReplacer("/", "_", ":", "_", "\\", "_", " ", "_")
	return replacer.Replace(name)
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
