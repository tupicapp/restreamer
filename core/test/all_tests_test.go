package test

import "testing"

type namedTest struct {
	name string
	fn   func(*testing.T)
}

func runNamedTests(t *testing.T, tests []namedTest) {
	t.Helper()

	for _, tc := range tests {
		t.Run(tc.name, tc.fn)
	}
}

// TestAll runs the curated end-to-end suite for the streaming pipeline.
// Integration and live-network cases are intentionally sequential to avoid
// shared fixture collisions and timing interference.
func TestAll(t *testing.T) {
	t.Log("=== Running All irajstreamer Tests ===")

	t.Run("Unit", func(t *testing.T) {
		runNamedTests(t, []namedTest{
			{name: "RewriteHLSPlaylist_RewritesRelativeSegmentURI", fn: TestRewriteHLSPlaylist_RewritesRelativeSegmentURI},
			{name: "RewriteHLSPlaylist_DoesNotRewriteAbsoluteSegmentURI", fn: TestRewriteHLSPlaylist_DoesNotRewriteAbsoluteSegmentURI},
			{name: "RewriteHLSPlaylist_RewritesLegacyRootRelativeProgramURIToConfiguredPrefix", fn: TestRewriteHLSPlaylist_RewritesLegacyRootRelativeProgramURIToConfiguredPrefix},
			{name: "JoinHLSPrefix_URLBase", fn: TestJoinHLSPrefix_URLBase},
			{name: "StreamerUpdateStreams_ReplacesAndRemoves", fn: TestStreamer_UpdateStreams_ReplacesAndRemoves},
			{name: "StreamerAddInputOutputAndSwitch", fn: TestStreamer_AddInputOutputAndSwitch},
			{name: "StreamerRemoveInputIfSame_OnlyRemovesMatchingInstance", fn: TestStreamer_RemoveInputIfSame_OnlyRemovesMatchingInstance},
			{name: "StreamerStopOutput_StopsWithoutRemoving", fn: TestStreamer_StopOutput_StopsWithoutRemoving},
			{name: "StreamManagerRestart", fn: TestStreamManager_RestartsOnStaleIO},
		})
	})

	t.Run("Integration", func(t *testing.T) {
		runNamedTests(t, []namedTest{
			{name: "StreamerHLSReaderToBufferingDestination", fn: TestStreamer_HLSReaderToBufferingDestination},
			{name: "StreamerHLSReaderLiveToBufferingDestination", fn: TestStreamer_HLSReaderLiveToBufferingDestination},
			{name: "StreamerRTMPReaderToBufferingDestination", fn: TestStreamer_RTMPReaderToBufferingDestination},
			{name: "HLSDestinationStreamFromRTMPReaderProducesPlayableHLS", fn: TestHLSDestinationStream_FromRTMPReader_ProducesPlayableHLS},
			{name: "StreamerSwitchBetweenInputs", fn: TestStreamer_SwitchBetweenInputs},
			{name: "StreamerHLSReaderTiming", fn: TestStreamer_HLSReaderTiming},
			{name: "StreamerRTMPReaderTiming", fn: TestStreamer_RTMPReaderTiming},
			{name: "HLSLiveInputToHLSOutput_FramesMatch", fn: TestHLSLiveInputToHLSOutput_FramesMatch},
		})
	})

	t.Run("Passthrough", func(t *testing.T) {
		runNamedTests(t, []namedTest{
			{name: "DirectHLSPassthrough", fn: TestDirectHLSPassthrough},
			{name: "HLSStreamerPassthrough", fn: TestHLSStreamerPassthrough},
			{name: "DirectHLSLivePassthrough", fn: TestDirectHLSLivePassthrough},
			{name: "HLSLiveStreamerPassthrough", fn: TestHLSLiveStreamerPassthrough},
			{name: "MultiHLSToHLSWindowSwitchesMatchReference", fn: TestMultiHLSToHLS_WindowSwitchesMatchReference},
			{name: "MultiHLSToHLSMixedFileAndLiveWindowSwitchesMatchReference", fn: TestMultiHLSToHLS_MixedFileAndLiveWindowSwitchesMatchReference},
		})
	})

	t.Log("=== All Tests Completed ===")
}

// In RTMP reading, encoder buffering can produce a short initial audio-only
// interval. Tests therefore compare windows after both streams have begun and
// allow minor A/V timing skew.
