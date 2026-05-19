//go:build cgo && media

package decoder

/*
#cgo pkg-config: libavcodec libavutil libswresample
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/channel_layout.h>
#include <libavutil/frame.h>
#include <libavutil/mem.h>
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
static int iraj_frame_channels(const AVFrame *frame) {
	return frame->ch_layout.nb_channels;
}
static int iraj_frame_sample_rate(const AVFrame *frame) {
	return frame->sample_rate;
}
static int iraj_frame_nb_samples(const AVFrame *frame) {
	return frame->nb_samples;
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
static int iraj_swr_convert_to_s16(
	struct SwrContext *swr,
	uint8_t *dst,
	int dst_samples,
	const AVFrame *src
) {
	uint8_t *out_data[1] = { dst };
	const uint8_t *in_data[AV_NUM_DATA_POINTERS] = {0};
	for (int i = 0; i < AV_NUM_DATA_POINTERS; i++) {
		in_data[i] = src->data[i];
	}
	return swr_convert(swr, out_data, dst_samples, in_data, src->nb_samples);
}
*/
import "C"

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/tupicapp/restreamer/core/raw"
	shared "github.com/tupicapp/restreamer/core/shared"
)

type aacDecoder struct {
	id     string
	input  <-chan *shared.Frame
	output chan *raw.AudioFrame
	errors chan error
	done   chan struct{}

	cfg       aacDecoderConfig
	startOnce sync.Once
	closeOnce sync.Once

	codecCtx *C.AVCodecContext
	packet   *C.AVPacket
	frame    *C.AVFrame
	swrCtx   *C.struct_SwrContext

	sequenceID int64

	metaMu    sync.Mutex
	metaByPTS map[int64]decoderMeta
}

func NewAACDecoder(id string, input <-chan *shared.Frame, opts ...AACDecoderOption) (AudioDecoder, error) {
	if input == nil {
		return nil, fmt.Errorf("aac decoder input channel is nil")
	}

	cfg := aacDecoderConfig{
		outputBuffer: 100,
		transport:    AACTransportRaw,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := validateAACDecoderConfig(cfg); err != nil {
		return nil, err
	}
	cfg = normalizeAACDecoderConfig(cfg)

	return &aacDecoder{
		id:        id,
		input:     input,
		output:    make(chan *raw.AudioFrame, cfg.outputBuffer),
		errors:    make(chan error, 8),
		done:      make(chan struct{}),
		cfg:       cfg,
		metaByPTS: make(map[int64]decoderMeta),
	}, nil
}

func (d *aacDecoder) Start() error {
	var err error
	d.startOnce.Do(func() {
		err = d.initDecoder()
		if err != nil {
			return
		}
		go d.run()
	})
	return err
}

func (d *aacDecoder) Output() <-chan *raw.AudioFrame { return d.output }

func (d *aacDecoder) Errors() <-chan error { return d.errors }

func (d *aacDecoder) Close() error {
	d.closeOnce.Do(func() {
		close(d.done)
	})
	return nil
}

func (d *aacDecoder) initDecoder() error {
	codec := C.avcodec_find_decoder(C.AV_CODEC_ID_AAC)
	if codec == nil {
		return fmt.Errorf("aac decoder %s: avcodec AAC decoder not found", d.id)
	}

	d.codecCtx = C.avcodec_alloc_context3(codec)
	if d.codecCtx == nil {
		return fmt.Errorf("aac decoder %s: failed to allocate codec context", d.id)
	}
	d.codecCtx.thread_count = 1

	if len(d.cfg.audioConfig) > 0 {
		if err := d.copyExtraData(d.cfg.audioConfig); err != nil {
			return err
		}
	}

	if ret := C.avcodec_open2(d.codecCtx, codec, nil); ret < 0 {
		return avErr("aac decoder "+d.id+": avcodec_open2 failed", ret)
	}

	d.packet = C.av_packet_alloc()
	if d.packet == nil {
		return fmt.Errorf("aac decoder %s: failed to allocate packet", d.id)
	}

	d.frame = C.av_frame_alloc()
	if d.frame == nil {
		return fmt.Errorf("aac decoder %s: failed to allocate frame", d.id)
	}

	return nil
}

func (d *aacDecoder) copyExtraData(config []byte) error {
	if len(config) == 0 {
		return nil
	}
	ptr := C.av_mallocz(C.size_t(len(config) + int(C.iraj_av_input_buffer_padding_size())))
	if ptr == nil {
		return fmt.Errorf("aac decoder %s: failed to allocate extradata", d.id)
	}
	C.memcpy(ptr, unsafe.Pointer(&config[0]), C.size_t(len(config)))
	d.codecCtx.extradata = (*C.uint8_t)(ptr)
	d.codecCtx.extradata_size = C.int(len(config))
	return nil
}

func (d *aacDecoder) run() {
	defer close(d.output)
	defer close(d.errors)
	defer d.releaseDecoder()

	for {
		select {
		case <-d.done:
			return
		case frame, ok := <-d.input:
			if !ok {
				if err := d.flushDecoder(); err != nil {
					d.reportError(err)
				}
				return
			}
			if frame == nil {
				continue
			}
			if frame.Codec != "" && frame.Codec != "aac" {
				d.reportError(fmt.Errorf("aac decoder %s: unsupported codec %q", d.id, frame.Codec))
				continue
			}

			if err := d.decodeFrame(frame); err != nil {
				d.reportError(err)
			}
		}
	}
}

func (d *aacDecoder) decodeFrame(frame *shared.Frame) error {
	if len(frame.Payload) == 0 {
		return nil
	}

	pts := frame.PTS
	dts := frame.DTS
	duration := frame.Duration

	for _, au := range frame.Payload {
		if len(au) == 0 {
			continue
		}
		if err := d.sendAccessUnit(frame, au, pts, dts, duration); err != nil {
			return err
		}
		if err := d.receiveFrames(); err != nil {
			return err
		}
		if duration > 0 {
			pts += duration
			if dts != 0 {
				dts += duration
			}
		}
	}

	return nil
}

func (d *aacDecoder) sendAccessUnit(src *shared.Frame, au []byte, pts, dts, duration time.Duration) error {
	C.av_packet_unref(d.packet)
	if ret := C.av_new_packet(d.packet, C.int(len(au))); ret < 0 {
		return avErr("aac decoder "+d.id+": av_new_packet failed", ret)
	}

	C.memcpy(unsafe.Pointer(d.packet.data), unsafe.Pointer(&au[0]), C.size_t(len(au)))

	key := framePTSKey(&shared.Frame{PTS: pts, DTS: dts, SequenceID: src.SequenceID})
	d.packet.pts = C.int64_t(key)
	d.packet.dts = C.int64_t(key)
	d.packet.flags |= C.AV_PKT_FLAG_KEY

	d.metaMu.Lock()
	d.metaByPTS[key] = decoderMeta{
		InputID:   src.InputID,
		Timestamp: src.Timestamp,
		Duration:  duration,
		GOPID:     src.GOPID,
	}
	d.metaMu.Unlock()

	ret := C.avcodec_send_packet(d.codecCtx, d.packet)
	C.av_packet_unref(d.packet)
	if ret < 0 {
		return avErr("aac decoder "+d.id+": avcodec_send_packet failed", ret)
	}
	return nil
}

func (d *aacDecoder) flushDecoder() error {
	if ret := C.avcodec_send_packet(d.codecCtx, nil); ret < 0 {
		return avErr("aac decoder "+d.id+": flush failed", ret)
	}
	return d.receiveFrames()
}

func (d *aacDecoder) receiveFrames() error {
	for {
		ret := C.avcodec_receive_frame(d.codecCtx, d.frame)
		switch ret {
		case 0:
			audioFrame, err := d.copyDecodedFrame(d.frame)

			C.av_frame_unref(d.frame)
			if err != nil {
				return err
			}

			select {
			case d.output <- audioFrame:
			case <-d.done:
				return nil
			}
		case C.iraj_averror_eagain(), C.iraj_averror_eof():
			return nil
		default:
			return avErr("aac decoder "+d.id+": avcodec_receive_frame failed", ret)
		}
	}
}

func (d *aacDecoder) copyDecodedFrame(src *C.AVFrame) (*raw.AudioFrame, error) {
	sampleRate := int(C.iraj_frame_sample_rate(src))
	channels := int(C.iraj_frame_channels(src))
	frameSize := int(C.iraj_frame_nb_samples(src))
	if sampleRate <= 0 || channels <= 0 || frameSize <= 0 {
		return nil, fmt.Errorf(
			"aac decoder %s: invalid stream info sampleRate=%d channels=%d frameSize=%d",
			d.id,
			sampleRate,
			channels,
			frameSize,
		)
	}

	var outLayout C.AVChannelLayout
	C.iraj_channel_layout_default_wrap(&outLayout, C.int(channels))
	defer C.av_channel_layout_uninit(&outLayout)

	if d.swrCtx != nil {
		C.swr_free(&d.swrCtx)
	}
	if ret := C.iraj_swr_alloc_set_opts2_wrap(
		&d.swrCtx,
		&outLayout,
		C.AV_SAMPLE_FMT_S16,
		C.int(sampleRate),
		&src.ch_layout,
		(C.enum_AVSampleFormat)(src.format),
		C.int(sampleRate),
	); ret < 0 {
		return nil, avErr("aac decoder "+d.id+": swr_alloc_set_opts2 failed", ret)
	}
	if d.swrCtx == nil {
		return nil, fmt.Errorf("aac decoder %s: failed to initialize swresample context", d.id)
	}
	if ret := C.swr_init(d.swrCtx); ret < 0 {
		return nil, avErr("aac decoder "+d.id+": swr_init failed", ret)
	}

	bytesPerSample, err := raw.PCMBytesPerSample(raw.AudioCodecPCMS16LE)
	if err != nil {
		return nil, err
	}
	outBytes := frameSize * channels * bytesPerSample
	payload := make([]byte, outBytes)
	if len(payload) > 0 {
		ret := C.iraj_swr_convert_to_s16(
			d.swrCtx,
			(*C.uint8_t)(unsafe.Pointer(&payload[0])),
			C.int(frameSize),
			src,
		)
		if ret < 0 {
			return nil, avErr("aac decoder "+d.id+": swr_convert failed", ret)
		}
	}

	meta := d.takeMeta(int64(src.best_effort_timestamp))
	duration := meta.Duration
	if duration <= 0 {
		duration = time.Duration(frameSize) * time.Second / time.Duration(sampleRate)
	}

	d.sequenceID++

	return &raw.AudioFrame{
		Frame: &shared.Frame{
			PTS:        time.Duration(int64(src.best_effort_timestamp)),
			DTS:        time.Duration(int64(src.best_effort_timestamp)),
			Duration:   duration,
			Payload:    [][]byte{payload},
			Codec:      raw.AudioCodecPCMS16LE,
			PacketType: raw.AudioCodecPCMS16LE,
			Timestamp:  meta.TimestampOrNow(),
			InputID:    meta.InputIDOr(d.id),
			IsKeyFrame: true,
			SequenceID: d.sequenceID,
			GOPID:      meta.GOPID,
		},
		SampleRate:        sampleRate,
		Channels:          channels,
		SampleFormat:      raw.AudioCodecPCMS16LE,
		SamplesPerChannel: frameSize,
	}, nil
}

func (d *aacDecoder) releaseDecoder() {
	if d.swrCtx != nil {
		C.swr_free(&d.swrCtx)
	}
	if d.frame != nil {
		C.av_frame_free(&d.frame)
	}
	if d.packet != nil {
		C.av_packet_free(&d.packet)
	}
	if d.codecCtx != nil {
		C.avcodec_free_context(&d.codecCtx)
	}
}

func (d *aacDecoder) reportError(err error) {
	if err == nil {
		return
	}
	select {
	case d.errors <- err:
	default:
	}
}

func (d *aacDecoder) takeMeta(pts int64) decoderMeta {
	d.metaMu.Lock()
	defer d.metaMu.Unlock()

	meta, ok := d.metaByPTS[pts]
	if ok {
		delete(d.metaByPTS, pts)
		return meta
	}

	return decoderMeta{}
}
