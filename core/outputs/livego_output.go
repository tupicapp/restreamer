//go:build livego

package outputs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gwuhaolin/livego/av"
	lrtmp "github.com/gwuhaolin/livego/protocol/rtmp"

	"go.uber.org/zap"
)

const (
	livegoDefaultAudioRate     = 44100
	livegoDefaultAudioChannels = 2
	livegoDriftThreshold       = 30 * time.Millisecond
)

type livegoOutput struct {
	id  string
	url string

	videoChan chan *Frame
	audioChan chan *Frame

	done    chan struct{}
	Started chan struct{}

	audioMu sync.RWMutex
	videoMu sync.RWMutex
	ptsMu   sync.RWMutex

	closeOnce sync.Once

	writerMu sync.RWMutex
	writer   av.WriteCloser

	client *lrtmp.Client

	isStarted bool
	isInit    bool
	youtube   bool
	startTime time.Time
	lastVideo time.Time
	lastAudio time.Time

	lastVideoPTSDur time.Duration
	lastAudioPTSDur time.Duration

	// stats
	TotalAudioFrames   int64
	TotalVideoFrames   int64
	DroppedAudioFrames float64
	DroppedVideoFrames float64
	currentVideoFps    float64
	currentAudioFps    float64
	videoFpsTimer      time.Time
	audioFpsTimer      time.Time

	// codec state
	sps        []byte
	pps        []byte
	audioConf  []byte
	spsPpsSent bool
	aacSent    bool
}

func NewLivegoOutput(outputID, url string) (Stream, error) {
	return &livegoOutput{
		id:            outputID,
		url:           url,
		videoChan:     make(chan *Frame, DefaultChannelBufferSize),
		audioChan:     make(chan *Frame, DefaultChannelBufferSize),
		done:          make(chan struct{}),
		Started:       make(chan struct{}),
		videoFpsTimer: time.Now(),
		audioFpsTimer: time.Now(),
	}, nil
}

func (o *livegoOutput) Type() string { return "writer" }

func (o *livegoOutput) GetVideoChan() chan *Frame { return o.videoChan }
func (o *livegoOutput) GetAudioChan() chan *Frame { return o.audioChan }
func (o *livegoOutput) GetID() string             { return o.id }

func (o *livegoOutput) AudioLock() *sync.RWMutex { return &o.audioMu }
func (o *livegoOutput) VideoLock() *sync.RWMutex { return &o.videoMu }

func (o *livegoOutput) IsRestartable() bool { return true }

func (o *livegoOutput) IsKeyFrame(frame *Frame) bool {
	if frame == nil {
		return false
	}
	if frame.IsKeyFrame {
		return true
	}

	for _, nalu := range frame.Payload {
		n := stripAnnexB(nalu)
		if len(n) == 0 {
			continue
		}

		nalType := n[0] & 0x1F
		if nalType == 5 { // IDR
			return true
		}
	}
	return false
}

func (r *livegoOutput) RestartInterval() time.Duration { return 10 * time.Second }

func (o *livegoOutput) Start() {
	if o.isInit {
		o.isStarted = true
		return
	}

	o.isInit = true
	o.youtube = strings.Contains(strings.ToLower(o.url), "youtube")

	go o.connect()
}

func (o *livegoOutput) waitVideoCatchup() {
	for {
		o.ptsMu.RLock()
		diff := o.lastVideo.Sub(o.lastAudio)
		o.ptsMu.RUnlock()

		if diff < livegoDriftThreshold {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (o *livegoOutput) waitAudioCatchup() {
	for {
		o.ptsMu.RLock()
		diff := o.lastAudio.Sub(o.lastVideo)
		o.ptsMu.RUnlock()

		if diff < livegoDriftThreshold {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (o *livegoOutput) connect() {
	o.client = lrtmp.NewRtmpClient(nil, nil)

	for {
		select {
		case <-o.done:
			return
		default:
		}

		if err := o.client.Dial(o.url, av.PUBLISH); err != nil {
			getLogger().Error("livego rtmp dial failed", zap.String("stream_id", o.id), zap.String("url", o.url), zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}
		return
	}
}

func (o *livegoOutput) setWriter(w av.WriteCloser) {
	o.writerMu.Lock()
	defer o.writerMu.Unlock()

	if o.writer != nil {
		o.writer.Close(fmt.Errorf("replaced"))
	}

	o.writer = w
	o.startTime = time.Now()
	o.lastVideo = o.startTime
	o.lastAudio = o.startTime
	o.videoFpsTimer = time.Now()
	o.audioFpsTimer = time.Now()

	o.isStarted = true
	select {
	case <-o.Started:
	default:
		close(o.Started)
	}

	go o.runVideo()
	go o.runAudio()
}

func (o *livegoOutput) runVideo() {
	logger := getLogger()

	defer func() {
		close(o.videoChan)
		close(o.audioChan)
	}()

	for {
		select {
		case <-o.done:
			return
		case frame := <-o.videoChan:
			if frame == nil {
				continue
			}

			if !o.isStarted || !o.writerReady() {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			if err := o.writeVideoFrame(frame); err != nil {
				logger.Error("livego video write failed", zap.String("stream_id", o.id), zap.Error(err))
				o.DroppedVideoFrames++
			}
		}
	}
}

func (o *livegoOutput) runAudio() {
	logger := getLogger()
	for {
		select {
		case <-o.done:
			return
		case frame := <-o.audioChan:
			if frame == nil {
				continue
			}

			if !o.isStarted || !o.writerReady() {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			if err := o.writeAudioFrame(frame); err != nil {
				logger.Error("livego audio write failed", zap.String("stream_id", o.id), zap.Error(err))
				o.DroppedAudioFrames++
			}
		}
	}
}

func (o *livegoOutput) writerReady() bool {
	o.writerMu.RLock()
	defer o.writerMu.RUnlock()
	return o.writer != nil
}

func (o *livegoOutput) writeVideoFrame(frame *Frame) error {
	if len(frame.Payload) == 0 {
		return nil
	}

	o.waitVideoCatchup()

	o.extractSPSPPS(frame.Payload)

	isKey := o.IsKeyFrame(frame)
	if o.youtube && o.sps != nil && o.pps != nil {
		if err := o.sendVideoConfig(); err != nil {
			return err
		}
	} else if (isKey || !o.spsPpsSent) && o.sps != nil && o.pps != nil {
		if err := o.sendVideoConfig(); err != nil {
			return err
		}
	}

	payload, err := o.buildVideoPayload(frame, false)
	if err != nil {
		return err
	}

	pkt := &av.Packet{
		IsVideo:   true,
		StreamID:  1,
		TimeStamp: uint32((frame.DTS / time.Millisecond)),
		Header: &videoPacketHeader{
			isKey:           isKey,
			isSeq:           false,
			compositionTime: int32((frame.PTS - frame.DTS) / time.Millisecond),
		},
		Data: payload,
	}

	o.ptsMu.Lock()
	o.lastVideoPTSDur = frame.DTS
	o.lastVideo = o.startTime.Add(frame.DTS)
	o.ptsMu.Unlock()

	if err := o.writePacket(pkt); err != nil {
		return err
	}

	o.TotalVideoFrames++
	o.updateVideoFps()
	return nil
}

func (o *livegoOutput) sendVideoConfig() error {
	if o.sps == nil || o.pps == nil {
		return nil
	}

	conf, err := buildAVCDecoderConfig(o.sps, o.pps)
	if err != nil {
		return err
	}

	payload := buildVideoTag(true, true, 0, conf, nil)

	pkt := &av.Packet{
		IsVideo:   true,
		StreamID:  1,
		TimeStamp: uint32((o.lastVideoPTSDur / time.Millisecond)),
		Header: &videoPacketHeader{
			isKey:           true,
			isSeq:           true,
			compositionTime: 0,
		},
		Data: payload,
	}

	if err := o.writePacket(pkt); err != nil {
		return err
	}

	o.spsPpsSent = true
	return nil
}

func (o *livegoOutput) buildVideoPayload(frame *Frame, seq bool) ([]byte, error) {
	compTime := int32((frame.PTS - frame.DTS) / time.Millisecond)

	nalus := make([][]byte, 0, len(frame.Payload))
	for _, nalu := range frame.Payload {
		stripped := stripAnnexB(nalu)
		if len(stripped) == 0 {
			continue
		}
		nalus = append(nalus, stripped)
	}

	if len(nalus) == 0 {
		return nil, nil
	}

	buf := bytes.NewBuffer(nil)
	frameType := byte(2)
	if frame.IsKeyFrame {
		frameType = 1
	}
	firstByte := (frameType << 4) | 7
	buf.WriteByte(firstByte)
	buf.WriteByte(1) // AVC NALU
	buf.WriteByte(byte(compTime >> 16))
	buf.WriteByte(byte(compTime >> 8))
	buf.WriteByte(byte(compTime))

	for _, nalu := range nalus {
		binary.Write(buf, binary.BigEndian, uint32(len(nalu)))
		buf.Write(nalu)
	}

	return buf.Bytes(), nil
}

func (o *livegoOutput) writeAudioFrame(frame *Frame) error {
	if len(frame.Payload) == 0 || len(frame.Payload[0]) == 0 {
		return nil
	}

	o.waitAudioCatchup()

	raw := frame.Payload[0]
	if !o.aacSent {
		o.audioConf = buildAACConfig(livegoDefaultAudioRate, livegoDefaultAudioChannels)
		if err := o.sendAudioConfig(frame); err != nil {
			return err
		}
		o.aacSent = true
	}

	payload := buildAACRawTag(raw)
	pkt := &av.Packet{
		IsAudio:   true,
		StreamID:  1,
		TimeStamp: uint32(frame.PTS / time.Millisecond),
		Header: &audioPacketHeader{
			soundFormat:  av.SOUND_AAC,
			aacPacketTyp: av.AAC_RAW,
		},
		Data: payload,
	}

	o.ptsMu.Lock()
	o.lastAudioPTSDur = frame.PTS
	o.lastAudio = o.startTime.Add(frame.PTS)
	o.ptsMu.Unlock()

	if err := o.writePacket(pkt); err != nil {
		return err
	}

	o.TotalAudioFrames++
	o.updateAudioFps()
	return nil
}

func (o *livegoOutput) sendAudioConfig(frame *Frame) error {
	cfg := buildAACSequenceTag(o.audioConf)
	pkt := &av.Packet{
		IsAudio:   true,
		StreamID:  1,
		TimeStamp: 0,
		Header: &audioPacketHeader{
			soundFormat:  av.SOUND_AAC,
			aacPacketTyp: av.AAC_SEQHDR,
		},
		Data: cfg,
	}
	return o.writePacket(pkt)
}

func (o *livegoOutput) writePacket(p *av.Packet) error {
	o.writerMu.RLock()
	defer o.writerMu.RUnlock()
	if o.writer == nil {
		return nil
	}
	return o.writer.Write(p)
}

func (o *livegoOutput) updateVideoFps() {
	if o.TotalVideoFrames%30 == 0 {
		dur := time.Since(o.videoFpsTimer)
		if dur > 0 {
			o.currentVideoFps = 30 / dur.Seconds()
		}
		o.videoFpsTimer = time.Now()
	}
}

func (o *livegoOutput) updateAudioFps() {
	if o.TotalAudioFrames%100 == 0 {
		dur := time.Since(o.audioFpsTimer)
		if dur > 0 {
			o.currentAudioFps = 100 / dur.Seconds()
		}
		o.audioFpsTimer = time.Now()
	}
}

func (o *livegoOutput) extractSPSPPS(nalus [][]byte) {
	for _, nalu := range nalus {
		stripped := stripAnnexB(nalu)
		if len(stripped) == 0 {
			continue
		}
		nalType := stripped[0] & 0x1F
		switch nalType {
		case 7:
			if o.sps == nil {
				o.sps = append([]byte{}, stripped...)
			}
		case 8:
			if o.pps == nil {
				o.pps = append([]byte{}, stripped...)
			}
		}
	}
}

func (o *livegoOutput) GetStateTimes() (video, audio time.Time) {
	o.ptsMu.RLock()
	defer o.ptsMu.RUnlock()
	return o.lastVideo, o.lastAudio
}

func (o *livegoOutput) State() *State {
	videoPTS, audioPTS := o.GetStateTimes()

	return &State{
		IsStarted:          o.isStarted,
		IsResumable:        o.IsRestartable(),
		StreamID:           o.id,
		Type:               o.Type(),
		Url:                o.url,
		TotalVideoFrames:   o.TotalVideoFrames,
		TotalAudioFrames:   o.TotalAudioFrames,
		DroppedVideoFrames: o.DroppedVideoFrames,
		DroppedAudioFrames: o.DroppedAudioFrames,
		VideoFps:           o.currentVideoFps,
		AudioFps:           o.currentAudioFps,
		LastIO:             maxTime(videoPTS, audioPTS),
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (o *livegoOutput) WaitForStart(ctx context.Context) error {
	select {
	case <-o.Started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *livegoOutput) Stop() {
	o.isStarted = false
}

func (o *livegoOutput) Close() {
	o.closeOnce.Do(func() {
		close(o.done)

		o.writerMu.Lock()
		if o.writer != nil {
			o.writer.Close(fmt.Errorf("closed"))
		}
		o.writerMu.Unlock()

	})
}

func (o *livegoOutput) Clone() (Stream, error) {
	return NewLivegoOutput(o.id, o.url)
}

type videoPacketHeader struct {
	isKey           bool
	isSeq           bool
	compositionTime int32
}

func (v *videoPacketHeader) IsKeyFrame() bool       { return v.isKey }
func (v *videoPacketHeader) IsSeq() bool            { return v.isSeq }
func (v *videoPacketHeader) CodecID() uint8         { return av.VIDEO_H264 }
func (v *videoPacketHeader) CompositionTime() int32 { return v.compositionTime }

type audioPacketHeader struct {
	soundFormat  uint8
	aacPacketTyp uint8
}

func (a *audioPacketHeader) SoundFormat() uint8   { return a.soundFormat }
func (a *audioPacketHeader) AACPacketType() uint8 { return a.aacPacketTyp }

// helper functions

func stripAnnexB(nalu []byte) []byte {
	for len(nalu) > 0 && nalu[0] == 0x00 {
		nalu = nalu[1:]
	}
	if len(nalu) > 0 && nalu[0] == 0x01 {
		nalu = nalu[1:]
	}
	return nalu
}

func buildAVCDecoderConfig(sps, pps []byte) ([]byte, error) {
	if len(sps) < 4 {
		return nil, fmt.Errorf("invalid SPS")
	}
	buf := &bytes.Buffer{}
	buf.WriteByte(0x01)
	buf.WriteByte(sps[1])
	buf.WriteByte(sps[2])
	buf.WriteByte(sps[3])
	buf.WriteByte(0xFF) // 4 bytes length
	buf.WriteByte(0xE1) // num sps
	binary.Write(buf, binary.BigEndian, uint16(len(sps)))
	buf.Write(sps)
	buf.WriteByte(0x01)
	binary.Write(buf, binary.BigEndian, uint16(len(pps)))
	buf.Write(pps)
	return buf.Bytes(), nil
}

func buildVideoTag(isKey, isSeq bool, compositionTime int32, seqData []byte, nalus [][]byte) []byte {
	buf := &bytes.Buffer{}
	frameType := byte(2)
	if isKey {
		frameType = 1
	}
	buf.WriteByte((frameType << 4) | 7)
	if isSeq {
		buf.WriteByte(0)
	} else {
		buf.WriteByte(1)
	}
	buf.WriteByte(byte(compositionTime >> 16))
	buf.WriteByte(byte(compositionTime >> 8))
	buf.WriteByte(byte(compositionTime))

	if isSeq {
		buf.Write(seqData)
	} else {
		for _, n := range nalus {
			binary.Write(buf, binary.BigEndian, uint32(len(n)))
			buf.Write(n)
		}
	}
	return buf.Bytes()
}

func buildAACConfig(sampleRate, channels int) []byte {
	return buildAudioSpecificConfig(sampleRate, channels)
}

func buildAACSequenceTag(config []byte) []byte {
	payload := []byte{0xAF, 0x00}
	payload = append(payload, config...)
	return payload
}

func buildAACRawTag(raw []byte) []byte {
	payload := []byte{0xAF, 0x01}
	return append(payload, raw...)
}

func buildAudioSpecificConfig(sampleRate, channels int) []byte {
	srIdx := livegoSampleRateToIndex(sampleRate)
	if srIdx < 0 {
		srIdx = 4 // 44.1k
	}
	if channels <= 0 {
		channels = 2
	}
	config := make([]byte, 2)
	config[0] = byte(0x10 | (srIdx >> 1))
	config[1] = byte((srIdx&1)<<7 | (channels << 3))
	return config
}

func livegoSampleRateToIndex(rate int) int {
	switch rate {
	case 96000:
		return 0
	case 88200:
		return 1
	case 64000:
		return 2
	case 48000:
		return 3
	case 44100:
		return 4
	case 32000:
		return 5
	case 24000:
		return 6
	case 22050:
		return 7
	case 16000:
		return 8
	case 12000:
		return 9
	case 11025:
		return 10
	case 8000:
		return 11
	case 7350:
		return 12
	default:
		return -1
	}
}
