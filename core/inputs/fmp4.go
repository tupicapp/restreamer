package inputs

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4"
	mp4codecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mp4/codecs"
	"go.uber.org/zap"
)

type fmp4Segment struct {
	host               segmentHost
	state              *segmentState
	fmp4Mu             sync.RWMutex
	fmp4InitURI        string
	fmp4TrackInfos     map[int]*fmp4TrackInfo
	data               []byte
	segmentURI         string
	processed          bool
	pendingAudioFrames []*Frame
	pendingVideoFrames []*Frame

	LastAudioPacket *Frame
	LastVideoPacket *Frame

	// SPS/PPS for H.264 keyframes
	sps []byte
	pps []byte
}

func NewFmp4(baseUrl string, segmentData []byte, m *playlist.MediaMap, segmentUri string, host segmentHost, state *segmentState) (Segment, error) {
	if m == nil {
		return nil, fmt.Errorf("missing fMP4 init segment")
	}

	c := http.DefaultClient
	getLogger().Debug("unmarshaling init segment")

	resolvedURI := resolveURL(baseUrl, m.URI)
	initData, err := fetchSegmentData(c, resolvedURI, m.ByteRangeStart, m.ByteRangeLength)
	if err != nil {
		return nil, err
	}

	var init fmp4.Init
	if err := init.Unmarshal(bytes.NewReader(initData)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal init segment from %s %s (size: %d): %w", m.URI, resolvedURI, len(initData), err)
	}

	trackInfos := make(map[int]*fmp4TrackInfo)
	var sps, pps []byte

	for _, track := range init.Tracks {
		codecLabel, ok := mp4CodecToString(track.Codec)
		if !ok {
			// Log unsupported codec for debugging
			getLogger().Warn("fmp4: unsupported codec type", zap.String("codec", fmt.Sprintf("%T", track.Codec)))
			continue
		}

		// Extract SPS/PPS from H.264 codec configuration
		if h264Codec, ok := track.Codec.(*mp4codecs.H264); ok {
			if len(h264Codec.SPS) > 0 {
				sps = make([]byte, len(h264Codec.SPS))
				copy(sps, h264Codec.SPS)
			}
			if len(h264Codec.PPS) > 0 {
				pps = make([]byte, len(h264Codec.PPS))
				copy(pps, h264Codec.PPS)
			}
		}

		trackInfos[track.ID] = &fmp4TrackInfo{
			codecLabel: codecLabel,
			isVideo:    track.Codec.IsVideo(),
			timeScale:  track.TimeScale,
		}
	}

	if len(trackInfos) == 0 {
		return nil, fmt.Errorf("no tracks found in fMP4 init segment (parsed %d tracks, none supported)", len(init.Tracks))
	}

	return &fmp4Segment{
		host:           host,
		state:          state,
		data:           segmentData, // Store the segment data, not the init data
		segmentURI:     segmentUri,
		fmp4TrackInfos: trackInfos,
		fmp4InitURI:    resolvedURI,
		sps:            sps,
		pps:            pps,
	}, nil
}

func (r *fmp4Segment) Read() error {
	audioLen := len(r.pendingAudioFrames)
	videoLen := len(r.pendingVideoFrames)
	if audioLen == 0 && videoLen == 0 {
		err := r.ReadSegment()
		if err != nil {
			return err
		}
	}

	if audioLen > videoLen {
		lastAudioFrame := r.pendingAudioFrames[0]
		r.host.enqueuePendingAudio(lastAudioFrame)
		r.pendingAudioFrames = r.pendingAudioFrames[1:]
	} else {
		lastVideoFrame := r.pendingVideoFrames[0]
		r.host.enqueuePendingVideo(lastVideoFrame)
		r.pendingVideoFrames = r.pendingVideoFrames[1:]
	}

	return nil
}

func (r *fmp4Segment) ReadSegment() error {
	if r.processed {
		return io.EOF
	}

	r.processed = true
	var parts fmp4.Parts
	if err := parts.Unmarshal(r.data); err != nil {
		return fmt.Errorf("failed to parse fMP4 segment %s: %w", r.segmentURI, err)
	}

	r.fmp4Mu.RLock()
	trackInfos := r.fmp4TrackInfos
	r.fmp4Mu.RUnlock()

	if trackInfos == nil {
		return fmt.Errorf("fMP4 init not loaded")
	}

	for _, part := range parts {
		for _, partTrack := range part.Tracks {
			info := trackInfos[partTrack.ID]
			if info == nil {
				continue
			}

			if info.isVideo {
				currentDTS := unitsToDuration(partTrack.BaseTime, uint32(MP4TimeScale))
				for _, sample := range partTrack.Samples {
					var au h264.AVCC
					if err := au.Unmarshal(sample.Payload); err != nil {
						return fmt.Errorf("failed to parse sample AVCC for %s: %w", r.host.streamURI(), err)
					}

					nalus := make([][]byte, len(au))
					for i, nalu := range au {
						nalus[i] = cloneBytes(nalu)
					}

					r.bufferVideoPacket(currentDTS, currentDTS, nalus, info.codecLabel)
					currentDTS += unitsToDuration(uint64(sample.Duration), uint32(MP4TimeScale))

					getLogger().Debug("fmp4 video sample",
						zap.Uint64("base_time", partTrack.BaseTime),
						zap.Int32("pts_offset", sample.PTSOffset),
						zap.Uint32("duration", sample.Duration),
						zap.Int64("dts_ms", currentDTS.Milliseconds()),
						zap.Int64("pts_ms", currentDTS.Milliseconds()))
				}

			} else {
				pts := unitsToDuration(partTrack.BaseTime, uint32(SampleRate))

				for _, sample := range partTrack.Samples {
					payload := cloneBytes(sample.Payload)

					r.bufferAudioPacket(pts, [][]byte{payload}, info.codecLabel)

					frameDur := time.Second * time.Duration(sample.Duration) / SampleRate
					pts += frameDur

					getLogger().Debug("fmp4 audio sample",
						zap.Uint64("base_time", partTrack.BaseTime),
						zap.Duration("frame_duration", frameDur),
						zap.Uint32("sample_duration", sample.Duration),
						zap.Int64("pts_ms", pts.Milliseconds()))
				}
			}
		}
	}

	return nil
}

func (r *fmp4Segment) bufferVideoPacket(pts, dts time.Duration, au [][]byte, codec string) {
	getLogger().Debug("bufferVideoPacket",
		zap.Int64("pts_ms", pts.Milliseconds()),
		zap.Int64("dts_ms", dts.Milliseconds()),
		zap.Int64("sequence_id", r.state.VideoSequenceID))

	r.state.sequenceIDMu.Lock()
	r.state.VideoSequenceID++
	sequenceID := r.state.VideoSequenceID
	r.state.sequenceIDMu.Unlock()

	// Check if this is a keyframe and if it already has SPS/PPS
	isKeyFrame := false
	hasSPS := false
	hasPPS := false

	for _, nalu := range au {
		if len(nalu) == 0 {
			continue
		}
		nalType := nalu[0] & 0x1F
		if nalType == 5 { // IDR frame
			isKeyFrame = true
		} else if nalType == 7 { // SPS
			hasSPS = true
		} else if nalType == 8 { // PPS
			hasPPS = true
		}
	}

	// Prepend SPS/PPS to keyframes if they're missing and we have them
	if isKeyFrame && len(r.sps) > 0 && len(r.pps) > 0 && (!hasSPS || !hasPPS) {
		newPayload := make([][]byte, 0, len(au)+2)
		if !hasSPS {
			spsCopy := make([]byte, len(r.sps))
			copy(spsCopy, r.sps)
			newPayload = append(newPayload, spsCopy)
		}
		if !hasPPS {
			ppsCopy := make([]byte, len(r.pps))
			copy(ppsCopy, r.pps)
			newPayload = append(newPayload, ppsCopy)
		}
		newPayload = append(newPayload, au...)
		au = newPayload
	}

	prevPTS := time.Duration(0)
	if r.state.lastVideoPTS != 0 {
		prevPTS = r.state.lastVideoPTS
	}

	duration := time.Duration(0)
	if prevPTS != 0 && pts >= prevPTS {
		duration = pts - prevPTS
	}

	frame := &Frame{
		PTS:        pts,
		DTS:        dts,
		Payload:    au,
		Codec:      codec,
		PacketType: classifyVideoPacketType(au, codec),
		Timestamp:  time.Now(),
		InputID:    r.host.streamID(),
		SequenceID: sequenceID,
		Duration:   duration,
		IsFile:     true,
	}
	if codec == "h264" {
		frame.VideoSPS, frame.VideoPPS = h264ExtractSPSPPS(au)
	}

	r.state.lastVideoPTS = pts

	frame.IsKeyFrame = IsTsKeyFrame(frame)

	if frame.IsKeyFrame {
		r.state.gopMu.Lock()
		r.state.lastVideoGOPID = sequenceID
		r.state.gopMu.Unlock()
	}

	r.state.gopMu.RLock()
	frame.GOPID = r.state.lastVideoGOPID
	r.state.gopMu.RUnlock()

	r.pendingVideoFrames = append(r.pendingVideoFrames, frame)

	r.host.incTotalVideoFrames()
}

func (r *fmp4Segment) bufferAudioPacket(pts time.Duration, payload [][]byte, codec string) {
	r.state.sequenceIDMu.Lock()
	sequenceID := r.state.AudioSequenceID + 1
	r.state.AudioSequenceID = sequenceID
	r.state.sequenceIDMu.Unlock()

	getLogger().Debug("bufferAudioPacket",
		zap.Int64("pts_ms", pts.Milliseconds()),
		zap.Int64("sequence_id", sequenceID))

	prevPTS := time.Duration(0)
	if r.state.lastAudioPTS != 0 {
		prevPTS = r.state.lastAudioPTS
	}

	duration := time.Duration(0)
	if prevPTS != 0 && pts >= prevPTS {
		duration = pts - prevPTS
	}

	frame := &Frame{
		PTS:        pts,
		DTS:        pts,
		Payload:    payload,
		Codec:      codec,
		Timestamp:  time.Now(),
		InputID:    r.host.streamID(),
		IsKeyFrame: true,
		SequenceID: sequenceID,
		Duration:   duration,
		IsFile:     true,
	}

	r.state.lastAudioPTS = pts

	if frame.IsKeyFrame {
		r.state.gopMu.Lock()
		r.state.lastAudioGOPID = sequenceID
		r.state.gopMu.Unlock()
	}

	r.state.gopMu.RLock()
	frame.GOPID = r.state.lastAudioGOPID
	r.state.gopMu.RUnlock()

	r.pendingAudioFrames = append(r.pendingAudioFrames, frame)

	r.host.incTotalAudioFrames()
}
