package inputs

import (
	"net/http/httptest"
	"restreamer/core/test_tools"
	"testing"
	"time"
)

type TestVideoConfig = test_tools.TestVideoConfig
type WindowMatchBenchmarkResult = test_tools.WindowMatchBenchmarkResult

func setupHLSVideoServer(t *testing.T, video TestVideoConfig) (string, *httptest.Server, error) {
	return test_tools.SetupHLSVideoServer(t, video)
}

func frameHash(frame *Frame) string {
	return test_tools.FrameHash(frame)
}

func windowMatchBenchmarkWithTiming(stream1, stream2 []*Frame, frameType string, elapsedTime time.Duration, threshold float64) WindowMatchBenchmarkResult {
	return test_tools.WindowMatchBenchmarkWithTiming(stream1, stream2, frameType, elapsedTime, threshold)
}

func printWindowMatchBenchmark(t *testing.T, result WindowMatchBenchmarkResult, frameType string) {
	test_tools.PrintWindowMatchBenchmark(t, result, frameType)
}

func equalPacketRateBenchmark(stream1, stream2 []*Frame, frameType string) test_tools.EqualPacketRateBenchmarkResult {
	return test_tools.EqualPacketRateBenchmark(stream1, stream2, frameType)
}

func printEqualPacketRateBenchmark(t *testing.T, result test_tools.EqualPacketRateBenchmarkResult, frameType string) {
	test_tools.PrintEqualPacketRateBenchmark(t, result, frameType)
}

func checkH264FrameHealth(t *testing.T, frames []*Frame) {
	test_tools.CheckH264FrameHealth(t, frames)
}

func checkStreamHealth(frames []*Frame, frameType string) test_tools.StreamHealthResult {
	return test_tools.CheckStreamHealth(frames, frameType)
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
