package inputs

import (
	"bytes"
	"fmt"
	"time"

	"github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts/codecs"
	"go.uber.org/zap"
)

type mpegtsSegment struct {
	*mpegts.Reader
	host  segmentHost
	state *segmentState
}

func (r *mpegtsSegment) Read() error {
	return r.Reader.Read()
}

func NewMpegTs(data []byte, host segmentHost, state *segmentState) (Segment, error) {
	reader := &mpegts.Reader{
		R: bytes.NewReader(data),
	}

	if err := reader.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize MPEG-TS reader: %w", err)
	}

	var videoTrack *mpegts.Track
	var audioTrack *mpegts.Track

	for _, track := range reader.Tracks() {
		switch track.Codec.(type) {
		case *codecs.H264:
			if videoTrack == nil {
				videoTrack = track
				getLogger().Info("hls reader: found H264 track", zap.Int("pid", int(track.PID)))
			}
		case *codecs.H265:
			if videoTrack == nil {
				videoTrack = track
				getLogger().Info("hls reader: found H265 track", zap.Int("pid", int(track.PID)))
			}
		case *codecs.MPEG4Audio:
			if audioTrack == nil {
				audioTrack = track
				getLogger().Info("hls reader: found MPEG4 audio track", zap.Int("pid", int(track.PID)))
			}
		case *codecs.Opus:
			if audioTrack == nil {
				audioTrack = track
				getLogger().Info("hls reader: found Opus track", zap.Int("pid", int(track.PID)))
			}
		case *codecs.MPEG1Audio:
			if audioTrack == nil {
				audioTrack = track
				getLogger().Info("hls reader: found MPEG1 audio track", zap.Int("pid", int(track.PID)))
			}
		case *codecs.AC3:
			if audioTrack == nil {
				audioTrack = track
				getLogger().Info("hls reader: found AC3 track", zap.Int("pid", int(track.PID)))
			}
		case *codecs.Unsupported:
			getLogger().Warn("hls reader: found unsupported video track", zap.Int("pid", int(track.PID)))
		}
	}

	tsReader := &mpegtsSegment{Reader: reader, host: host, state: state}

	if videoTrack != nil {
		switch videoTrack.Codec.(type) {
		case *codecs.H264:
			reader.OnDataH264(videoTrack, func(pts int64, dts int64, au [][]byte) error {
				adjustedPts := time.Duration(pts) * time.Second / MpegTSTimeScale
				adjustedDts := time.Duration(dts) * time.Second / MpegTSTimeScale

				tsReader.host.incTotalVideoFrames()

				tsReader.bufferVideoPacket(adjustedPts, adjustedDts, au, "h264")
				return nil
			})
		case *codecs.H265:
			reader.OnDataH265(videoTrack, func(pts int64, dts int64, au [][]byte) error {
				adjustedPts := time.Duration(pts) * time.Second / MpegTSTimeScale
				adjustedDts := time.Duration(dts) * time.Second / MpegTSTimeScale

				tsReader.bufferVideoPacket(adjustedPts, adjustedDts, au, "h265")
				return nil
			})
		case *codecs.MPEG4Video:
			reader.OnDataMPEGxVideo(videoTrack, func(pts int64, frame []byte) error {
				adjustedPts := time.Duration(pts) * time.Second / MpegTSTimeScale
				adjustedDts := adjustedPts

				tsReader.bufferVideoPacket(adjustedPts, adjustedDts, [][]byte{frame}, "mpeg4video")
				return nil
			})
		case *codecs.MPEG1Video:
			reader.OnDataMPEGxVideo(videoTrack, func(pts int64, frame []byte) error {
				adjustedPts := time.Duration(pts) * time.Second / MpegTSTimeScale
				adjustedDts := adjustedPts

				tsReader.bufferVideoPacket(adjustedPts, adjustedDts, [][]byte{frame}, "mpeg1video")
				return nil
			})
		}
	}

	if audioTrack != nil {
		switch audioTrack.Codec.(type) {
		case *codecs.MPEG4Audio:
			actualSampleRate := SampleRate
			if mc, ok := audioTrack.Codec.(*codecs.MPEG4Audio); ok && mc.SampleRate > 0 {
				actualSampleRate = mc.SampleRate
			}
			reader.OnDataMPEG4Audio(audioTrack, func(pts int64, aus [][]byte) error {
				ptsBase := time.Duration(pts) * time.Second / MpegTSTimeScale
				frameDur := time.Second * TsAACBytePerSample / time.Duration(actualSampleRate)

				for i, au := range aus {
					framePTS := ptsBase + time.Duration(i)*frameDur
					tsReader.bufferAudioPacket(framePTS, [][]byte{cloneBytes(au)}, "aac", actualSampleRate)
				}

				return nil
			})

		case *codecs.Opus:
			reader.OnDataOpus(audioTrack, func(pts int64, packets [][]byte) error {
				ptsBase := time.Duration(pts) * time.Second / MpegTSTimeScale
				frameDur := time.Second * TsAACBytePerSample / SampleRate

				for i, au := range packets {
					framePTS := ptsBase + time.Duration(i)*frameDur
					tsReader.bufferAudioPacket(framePTS, [][]byte{cloneBytes(au)}, "opus", 0)
				}

				return nil
			})
		case *codecs.MPEG1Audio:
			reader.OnDataMPEG1Audio(audioTrack, func(pts int64, frames [][]byte) error {
				ptsBase := time.Duration(pts) * time.Second / MpegTSTimeScale
				frameDur := time.Second * TsAACBytePerSample / SampleRate

				for i, au := range frames {
					framePTS := ptsBase + time.Duration(i)*frameDur
					tsReader.bufferAudioPacket(framePTS, [][]byte{cloneBytes(au)}, "mpeg1Audio", 0)
				}

				return nil
			})
		case *codecs.AC3:
			reader.OnDataAC3(audioTrack, func(pts int64, frame []byte) error {
				ptsBase := time.Duration(pts) * time.Second / MpegTSTimeScale

				tsReader.bufferAudioPacket(ptsBase, [][]byte{cloneBytes(frame)}, "ac3", 0)

				return nil
			})
		}
	}

	return tsReader, nil
}

func (r *mpegtsSegment) bufferVideoPacket(pts, dts time.Duration, au [][]byte, codec string) {
	r.state.sequenceIDMu.Lock()
	r.state.VideoSequenceID++
	sequenceID := r.state.VideoSequenceID
	r.state.sequenceIDMu.Unlock()

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

	r.state.lastVideoPTS = pts

	getLogger().Debug("bufferVideoPacket",
		zap.String("packet_type", frame.PacketType),
		zap.Int64("pts_ms", pts.Milliseconds()),
		zap.Int64("sequence_id", sequenceID))

	frame.IsKeyFrame = IsTsKeyFrame(frame)

	if frame.IsKeyFrame {
		r.state.gopMu.Lock()
		r.state.lastVideoGOPID = sequenceID
		r.state.gopMu.Unlock()
	}

	r.state.gopMu.RLock()
	frame.GOPID = r.state.lastVideoGOPID
	r.state.gopMu.RUnlock()

	// Always append to pending buffer to preserve order; no drops
	r.host.enqueuePendingVideo(frame)
}

func (r *mpegtsSegment) bufferAudioPacket(pts time.Duration, payload [][]byte, codec string, sampleRate int) {
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
		SampleRate: sampleRate,
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

	r.host.enqueuePendingAudio(frame)
	r.host.incTotalAudioFrames()
}
