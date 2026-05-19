package test

import (
	"testing"
)

// TestAll runs all tests in the irajstreamer package
// This is an integration test that executes all test functions
func TestAll(t *testing.T) {
	t.Log("=== Running All irajstreamer Tests ===")

	// Unit Tests
	t.Run("Unit", func(t *testing.T) {
		t.Parallel()
		t.Run("StreamerUpdateStreams", TestStreamer_UpdateStreams_ReplacesAndRemoves)
		t.Run("StreamerAddInputOutput", TestStreamer_AddInputOutputAndSwitch)
		t.Run("StreamManagerRestart", TestStreamManager_RestartsOnStaleIO)
		// t.Run("ValidH264Packet", TestIsValidH264Packet)
		// t.Run("ValidAACPacket", TestIsValidAACMPEG4AudioPacket)
	})

	// GOP Buffer Tests
	t.Run("GOPBuffer", func(t *testing.T) {
		t.Parallel()
		// t.Run("SwitchGatedOnKeyframe", TestGOPBuffer_SwitchGatedOnVideoKeyframe_AudioHeldForSync)
	})

	// Streamer Tests
	t.Run("Streamer", func(t *testing.T) {
		t.Parallel()
		// t.Run("HLSReaderToBufferingDestination", TestStreamer_HLSReaderToBufferingDestination)
		// t.Run("RTMPReaderToBufferingDestination", TestStreamer_RTMPReaderToBufferingDestination)
		// t.Run("HLSReaderLiveToBufferingDestination", TestStreamer_HLSReaderLiveToBufferingDestination)
	})

	// Switch Tests
	t.Run("Switch", func(t *testing.T) {
		t.Parallel()
		// t.Run("SwitchBetweenInputs", TestStreamer_SwitchBetweenInputs)
	})

	t.Log("=== All Tests Completed ===")
}

// in rtmp reading based on encoder buffer may it start first 1 second with audio but without video this depends on encoder.
// but for now we are ignoring audio-windows that hasnt video and video windows that hasnt audio.
// time window mathing of audio and video is assured to be less than 0.1
