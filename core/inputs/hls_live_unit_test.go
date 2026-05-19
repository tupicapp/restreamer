package inputs

import (
	"testing"
	"time"
)

func newTestHLSLive(t *testing.T) *hlsInputLive {
	t.Helper()

	reader, ok := NewHLSLive("live-unit-reader", "http://example.com/live.m3u8").(*hlsInputLive)
	if !ok || reader == nil {
		t.Fatal("expected hlsInputLive instance")
	}

	return reader
}

func TestHLSLiveOnVideoFrameInjectsCachedH264ParameterSetsIntoKeyframe(t *testing.T) {
	reader := newTestHLSLive(t)

	sps := []byte{0x67, 0x64, 0x00, 0x29}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x21}

	reader.setH264ParameterSets(sps, pps)
	reader.onVideoFrame(90000, 90000, [][]byte{idr}, "h264", 90000)

	select {
	case frame := <-reader.videoChan:
		if !frame.IsKeyFrame {
			t.Fatalf("expected keyframe")
		}
		if len(frame.Payload) != 3 {
			t.Fatalf("expected 3 NAL units, got %d", len(frame.Payload))
		}
		if string(frame.Payload[0]) != string(sps) {
			t.Fatalf("unexpected SPS payload: got %v want %v", frame.Payload[0], sps)
		}
		if string(frame.Payload[1]) != string(pps) {
			t.Fatalf("unexpected PPS payload: got %v want %v", frame.Payload[1], pps)
		}
		if string(frame.Payload[2]) != string(idr) {
			t.Fatalf("unexpected IDR payload: got %v want %v", frame.Payload[2], idr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for video frame")
	}
}

func TestHLSLiveOnVideoFrameCachesInBandH264ParameterSetsForLaterKeyframes(t *testing.T) {
	reader := newTestHLSLive(t)

	sps := []byte{0x67, 0x42, 0x00, 0x1f}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	firstIDR := []byte{0x65, 0x88, 0x84}
	secondIDR := []byte{0x65, 0x99, 0x11}

	reader.onVideoFrame(90000, 90000, [][]byte{sps, pps, firstIDR}, "h264", 90000)
	select {
	case <-reader.videoChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first video frame")
	}

	cachedSPS, cachedPPS := reader.getH264ParameterSets()
	if string(cachedSPS) != string(sps) {
		t.Fatalf("cached SPS = %v, want %v", cachedSPS, sps)
	}
	if string(cachedPPS) != string(pps) {
		t.Fatalf("cached PPS = %v, want %v", cachedPPS, pps)
	}

	reader.onVideoFrame(180000, 180000, [][]byte{secondIDR}, "h264", 90000)

	select {
	case frame := <-reader.videoChan:
		if len(frame.Payload) != 3 {
			t.Fatalf("expected cached SPS/PPS to be injected, got %d NAL units", len(frame.Payload))
		}
		if string(frame.Payload[0]) != string(sps) {
			t.Fatalf("unexpected injected SPS payload: got %v want %v", frame.Payload[0], sps)
		}
		if string(frame.Payload[1]) != string(pps) {
			t.Fatalf("unexpected injected PPS payload: got %v want %v", frame.Payload[1], pps)
		}
		if string(frame.Payload[2]) != string(secondIDR) {
			t.Fatalf("unexpected IDR payload: got %v want %v", frame.Payload[2], secondIDR)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for second video frame")
	}
}
