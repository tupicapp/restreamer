//go:build cgo && media

package encoder

/*
#cgo pkg-config: libavcodec libavutil libswresample
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/channel_layout.h>
#include <libavutil/frame.h>
#include <libavutil/mem.h>
#include <libavutil/opt.h>
#include <libavutil/samplefmt.h>
#include <libswresample/swresample.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int iraj_averror_eagain() { return AVERROR(EAGAIN); }
static int iraj_averror_eof() { return AVERROR_EOF; }
static int iraj_av_strerror_wrap(int errnum, char *buf, size_t buflen) {
	return av_strerror(errnum, buf, buflen);
}
static void iraj_channel_layout_default_wrap(AVChannelLayout *layout, int channels) {
	av_channel_layout_default(layout, channels);
}
static int iraj_av_input_buffer_padding_size() {
	return AV_INPUT_BUFFER_PADDING_SIZE;
}
static enum AVSampleFormat iraj_encoder_sample_fmt(const AVCodec *codec) {
	const void *configs = NULL;
	int num_configs = 0;
	if (codec == NULL) {
		return AV_SAMPLE_FMT_FLTP;
	}
	if (avcodec_get_supported_config(NULL, codec, AV_CODEC_CONFIG_SAMPLE_FORMAT, 0, &configs, &num_configs) < 0 || configs == NULL || num_configs <= 0) {
		return AV_SAMPLE_FMT_FLTP;
	}
	return ((const enum AVSampleFormat *)configs)[0];
}
static int iraj_swr_convert_frame(SwrContext *swr, AVFrame *dst, const uint8_t *src, int inSamples) {
	const uint8_t *inData[1] = { src };
	return swr_convert(swr, dst->data, dst->nb_samples, inData, inSamples);
}
static int iraj_swr_alloc_set_opts2_wrap(
	struct SwrContext **ps,
	const AVChannelLayout *out_ch_layout,
	enum AVSampleFormat out_sample_fmt,
	int out_sample_rate,
	const AVChannelLayout *in_ch_layout,
	enum AVSampleFormat in_sample_fmt,
	int in_sample_rate
) {
	return swr_alloc_set_opts2(ps, out_ch_layout, out_sample_fmt, out_sample_rate, in_ch_layout, in_sample_fmt, in_sample_rate, 0, NULL);
}
*/
import "C"

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"restreamer/irajstreamer/core/raw"
	shared "restreamer/irajstreamer/core/shared"
)

type aacEncoder struct {
	id     string
	input  <-chan *raw.AudioFrame
	output chan *shared.Frame
	errors chan error
	done   chan struct{}

	cfg       aacEncoderConfig
	startOnce sync.Once
	closeOnce sync.Once

	codecCtx *C.AVCodecContext
	frame    *C.AVFrame
	packet   *C.AVPacket
	swrCtx   *C.struct_SwrContext

	initialized         bool
	sampleRate          int
	channels            int
	samplesPerAU        int
	sequenceID          int64
	framePTS            int64
	audioSpecificConfig []byte
	nextPTS             time.Duration
	lastInputID         string
	lastTimestamp       time.Time
	lastGOPID           int64
	lastIsFile          bool
}

func NewAACEncoder(id string, input <-chan *raw.AudioFrame, opts ...AACEncoderOption) (AudioEncoder, error) {
	if input == nil {
		return nil, fmt.Errorf("aac encoder input channel is nil")
	}

	cfg := aacEncoderConfig{
		outputBuffer: 100,
		bitRate:      128_000,
		transport:    AACTransportRaw,
		objectType:   AACObjectTypeLC,
		afterburner:  true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := validateAACEncoderConfig(cfg); err != nil {
		return nil, err
	}
	cfg = normalizeAACEncoderConfig(cfg)

	return &aacEncoder{
		id:     id,
		input:  input,
		output: make(chan *shared.Frame, cfg.outputBuffer),
		errors: make(chan error, 8),
		done:   make(chan struct{}),
		cfg:    cfg,
	}, nil
}

func (e *aacEncoder) Start() error {
	e.startOnce.Do(func() {
		go e.run()
	})
	return nil
}

func (e *aacEncoder) Output() <-chan *shared.Frame { return e.output }

func (e *aacEncoder) Errors() <-chan error { return e.errors }

func (e *aacEncoder) Close() error {
	e.closeOnce.Do(func() {
		close(e.done)
	})
	return nil
}

func (e *aacEncoder) AudioSpecificConfig() []byte {
	if len(e.audioSpecificConfig) == 0 {
		return nil
	}
	return append([]byte(nil), e.audioSpecificConfig...)
}

func (e *aacEncoder) run() {
	defer close(e.output)
	defer close(e.errors)
	defer e.releaseEncoder()

	for {
		select {
		case <-e.done:
			return
		case frame, ok := <-e.input:
			if !ok {
				if err := e.flushEncoder(); err != nil {
					e.reportError(err)
				}
				return
			}
			if frame == nil {
				continue
			}
			if err := frame.Validate(); err != nil {
				e.reportError(fmt.Errorf("aac encoder %s: invalid raw frame: %w", e.id, err))
				continue
			}
			if frame.SampleFormat != raw.AudioCodecPCMS16LE {
				e.reportError(fmt.Errorf(
					"aac encoder %s: unsupported sample format %q, want %q",
					e.id,
					frame.SampleFormat,
					raw.AudioCodecPCMS16LE,
				))
				continue
			}
			if !e.initialized {
				if err := e.initEncoder(frame); err != nil {
					e.reportError(err)
					continue
				}
			}
			if frame.SampleRate != e.sampleRate || frame.Channels != e.channels {
				e.reportError(fmt.Errorf(
					"aac encoder %s: audio format changed from %d Hz/%d ch to %d Hz/%d ch; recreate encoder for a new format",
					e.id,
					e.sampleRate,
					e.channels,
					frame.SampleRate,
					frame.Channels,
				))
				continue
			}

			if err := e.sendFrame(frame); err != nil {
				e.reportError(err)
				continue
			}
			if err := e.receivePackets(frame); err != nil {
				e.reportError(err)
			}
		}
	}
}

func (e *aacEncoder) initEncoder(frame *raw.AudioFrame) error {
	if e.cfg.transport == AACTransportADTS {
		return fmt.Errorf("aac encoder %s: ADTS transport is not supported by the FFmpeg AAC encoder path; use raw transport", e.id)
	}

	sampleRate := e.cfg.sampleRate
	if sampleRate == 0 {
		sampleRate = frame.SampleRate
	}
	channels := e.cfg.channels
	if channels == 0 {
		channels = frame.Channels
	}
	if sampleRate <= 0 {
		return fmt.Errorf("aac encoder %s: invalid sample rate %d", e.id, sampleRate)
	}
	if channels <= 0 {
		return fmt.Errorf("aac encoder %s: invalid channel count %d", e.id, channels)
	}

	codec := C.avcodec_find_encoder(C.AV_CODEC_ID_AAC)
	if codec == nil {
		return fmt.Errorf("aac encoder %s: avcodec AAC encoder not found", e.id)
	}

	e.codecCtx = C.avcodec_alloc_context3(codec)
	if e.codecCtx == nil {
		return fmt.Errorf("aac encoder %s: failed to allocate codec context", e.id)
	}

	e.codecCtx.sample_rate = C.int(sampleRate)
	C.iraj_channel_layout_default_wrap(&e.codecCtx.ch_layout, C.int(channels))
	e.codecCtx.sample_fmt = C.iraj_encoder_sample_fmt(codec)
	e.codecCtx.bit_rate = C.int64_t(e.cfg.bitRate)
	e.codecCtx.time_base = C.AVRational{num: 1, den: C.int(sampleRate)}
	e.codecCtx.flags |= C.AV_CODEC_FLAG_GLOBAL_HEADER

	if ret := C.avcodec_open2(e.codecCtx, codec, nil); ret < 0 {
		return avErr("aac encoder "+e.id+": avcodec_open2 failed", ret)
	}

	e.frame = C.av_frame_alloc()
	if e.frame == nil {
		return fmt.Errorf("aac encoder %s: failed to allocate frame", e.id)
	}
	e.frame.format = C.int(e.codecCtx.sample_fmt)
	e.frame.sample_rate = e.codecCtx.sample_rate
	if ret := C.av_channel_layout_copy(&e.frame.ch_layout, &e.codecCtx.ch_layout); ret < 0 {
		return avErr("aac encoder "+e.id+": av_channel_layout_copy failed", ret)
	}
	e.frame.nb_samples = e.codecCtx.frame_size
	if e.frame.nb_samples <= 0 {
		e.frame.nb_samples = C.int(frame.SamplesPerChannel)
	}
	if e.frame.nb_samples <= 0 {
		return fmt.Errorf("aac encoder %s: invalid frame sample count", e.id)
	}
	if ret := C.av_frame_get_buffer(e.frame, 0); ret < 0 {
		return avErr("aac encoder "+e.id+": av_frame_get_buffer failed", ret)
	}

	e.packet = C.av_packet_alloc()
	if e.packet == nil {
		return fmt.Errorf("aac encoder %s: failed to allocate packet", e.id)
	}

	if ret := C.iraj_swr_alloc_set_opts2_wrap(
		&e.swrCtx,
		&e.codecCtx.ch_layout,
		e.codecCtx.sample_fmt,
		e.codecCtx.sample_rate,
		&e.codecCtx.ch_layout,
		C.AV_SAMPLE_FMT_S16,
		C.int(sampleRate),
	); ret < 0 {
		return avErr("aac encoder "+e.id+": swr_alloc_set_opts2 failed", ret)
	}
	if e.swrCtx == nil {
		return fmt.Errorf("aac encoder %s: failed to allocate swresample context", e.id)
	}
	if ret := C.swr_init(e.swrCtx); ret < 0 {
		return avErr("aac encoder "+e.id+": swr_init failed", ret)
	}

	if e.codecCtx.extradata != nil && e.codecCtx.extradata_size > 0 {
		e.audioSpecificConfig = C.GoBytes(unsafe.Pointer(e.codecCtx.extradata), e.codecCtx.extradata_size)
	}

	e.sampleRate = sampleRate
	e.channels = channels
	e.samplesPerAU = int(e.frame.nb_samples)
	e.initialized = true
	return nil
}

func (e *aacEncoder) sendFrame(frame *raw.AudioFrame) error {
	if frame.SamplesPerChannel <= 0 {
		return fmt.Errorf("aac encoder %s: invalid samples per channel %d", e.id, frame.SamplesPerChannel)
	}
	if e.samplesPerAU > 0 && frame.SamplesPerChannel != e.samplesPerAU {
		return fmt.Errorf(
			"aac encoder %s: unexpected frame sample count %d, want %d",
			e.id,
			frame.SamplesPerChannel,
			e.samplesPerAU,
		)
	}

	if ret := C.av_frame_make_writable(e.frame); ret < 0 {
		return avErr("aac encoder "+e.id+": av_frame_make_writable failed", ret)
	}

	payload := frame.Frame.Payload[0]
	ret := C.iraj_swr_convert_frame(
		e.swrCtx,
		e.frame,
		(*C.uint8_t)(unsafe.Pointer(&payload[0])),
		C.int(frame.SamplesPerChannel),
	)
	if ret < 0 {
		return avErr("aac encoder "+e.id+": swr_convert failed", ret)
	}

	e.frame.pts = C.int64_t(e.framePTS)
	e.framePTS += int64(e.frame.nb_samples)

	if ret := C.avcodec_send_frame(e.codecCtx, e.frame); ret < 0 {
		return avErr("aac encoder "+e.id+": avcodec_send_frame failed", ret)
	}
	return nil
}

func (e *aacEncoder) flushEncoder() error {
	if !e.initialized {
		return nil
	}
	if ret := C.avcodec_send_frame(e.codecCtx, nil); ret < 0 {
		return avErr("aac encoder "+e.id+": flush failed", ret)
	}
	return e.receivePackets(nil)
}

func (e *aacEncoder) receivePackets(src *raw.AudioFrame) error {
	for {
		ret := C.avcodec_receive_packet(e.codecCtx, e.packet)
		switch ret {
		case 0:
			pkt := e.buildOutputFrame(src)
			C.av_packet_unref(e.packet)

			select {
			case e.output <- pkt:
			case <-e.done:
				return nil
			}
		case C.iraj_averror_eagain(), C.iraj_averror_eof():
			return nil
		default:
			return avErr("aac encoder "+e.id+": avcodec_receive_packet failed", ret)
		}
	}
}

func (e *aacEncoder) buildOutputFrame(src *raw.AudioFrame) *shared.Frame {
	packet := C.GoBytes(unsafe.Pointer(e.packet.data), e.packet.size)

	duration := time.Duration(0)
	if src != nil && src.Frame != nil {
		duration = src.Frame.Duration
	}
	if duration <= 0 && e.samplesPerAU > 0 && e.sampleRate > 0 {
		duration = time.Duration(e.samplesPerAU) * time.Second / time.Duration(e.sampleRate)
	}

	e.sequenceID++

	out := &shared.Frame{
		Duration:   duration,
		Payload:    [][]byte{packet},
		Codec:      "aac",
		PacketType: string(e.cfg.transport),
		Timestamp:  time.Now(),
		InputID:    e.id,
		IsKeyFrame: true,
		SequenceID: e.sequenceID,
	}
	if src != nil && src.Frame != nil {
		out.PTS = src.Frame.PTS
		out.DTS = src.Frame.DTS
		out.Duration = src.Frame.Duration
		out.Timestamp = timestampOrNow(src.Frame.Timestamp)
		out.InputID = stringOrFallback(src.Frame.InputID, e.id)
		out.GOPID = src.Frame.GOPID
		out.SequenceID = src.Frame.SequenceID
		out.IsFile = src.Frame.IsFile
	}
	if out.DTS == 0 {
		out.DTS = out.PTS
	}
	if out.Duration == 0 {
		out.Duration = duration
	}

	e.nextPTS = out.PTS + out.Duration
	e.lastInputID = out.InputID
	e.lastTimestamp = out.Timestamp
	e.lastGOPID = out.GOPID
	e.lastIsFile = out.IsFile
	return out
}

func (e *aacEncoder) releaseEncoder() {
	if e.swrCtx != nil {
		C.swr_free(&e.swrCtx)
	}
	if e.frame != nil {
		C.av_frame_free(&e.frame)
	}
	if e.packet != nil {
		C.av_packet_free(&e.packet)
	}
	if e.codecCtx != nil {
		C.avcodec_free_context(&e.codecCtx)
	}
}

func (e *aacEncoder) reportError(err error) {
	if err == nil {
		return
	}
	select {
	case e.errors <- err:
	default:
	}
}

func timestampOrNow(ts time.Time) time.Time {
	if !ts.IsZero() {
		return ts
	}
	return time.Now()
}

func stringOrFallback(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
