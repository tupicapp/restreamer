package inputs

import (
	"net/http/httptest"
	"restreamer/core/test"
	"testing"
	"time"
)

type TestVideoConfig = test.TestVideoConfig
type WindowMatchBenchmarkResult = test.WindowMatchBenchmarkResult

func setupHLSVideoServer(t *testing.T, video TestVideoConfig) (string, *httptest.Server, error) {
	return test.SetupHLSVideoServer(t, video)
}

func frameHash(frame *Frame) string {
	return test.FrameHash(frame)
}

func windowMatchBenchmarkWithTiming(stream1, stream2 []*Frame, frameType string, elapsedTime time.Duration, threshold float64) WindowMatchBenchmarkResult {
	return test.WindowMatchBenchmarkWithTiming(stream1, stream2, frameType, elapsedTime, threshold)
}

func printWindowMatchBenchmark(t *testing.T, result WindowMatchBenchmarkResult, frameType string) {
	test.PrintWindowMatchBenchmark(t, result, frameType)
}

func equalPacketRateBenchmark(stream1, stream2 []*Frame, frameType string) test.EqualPacketRateBenchmarkResult {
	return test.EqualPacketRateBenchmark(stream1, stream2, frameType)
}

func printEqualPacketRateBenchmark(t *testing.T, result test.EqualPacketRateBenchmarkResult, frameType string) {
	test.PrintEqualPacketRateBenchmark(t, result, frameType)
}

func checkH264FrameHealth(t *testing.T, frames []*Frame) {
	test.CheckH264FrameHealth(t, frames)
}

func checkStreamHealth(frames []*Frame, frameType string) test.StreamHealthResult {
	return test.CheckStreamHealth(frames, frameType)
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
