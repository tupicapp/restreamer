package test

import (
	testtools "restreamer/core/test_tools"
)

// Re-export types from testtools for use in test package
type StreamHealthResult = testtools.StreamHealthResult
type PTSIssue = testtools.PTSIssue
type GapIssue = testtools.GapIssue
type DTSIssue = testtools.DTSIssue
type H264HealthIssue = testtools.H264HealthIssue
type FrameSequenceComparisonResult = testtools.FrameSequenceComparisonResult
type WindowComparison = testtools.WindowComparison
type WindowMatchBenchmarkResult = testtools.WindowMatchBenchmarkResult
type WindowMatch = testtools.WindowMatch
type MismatchContext = testtools.MismatchContext
type EqualPacketRateBenchmarkResult = testtools.EqualPacketRateBenchmarkResult
type MissingPacketContext = testtools.MissingPacketContext
type SwitchEvent = testtools.SwitchEvent
type SwitchLatencyResult = testtools.SwitchLatencyResult
type InputIDChange = testtools.InputIDChange
