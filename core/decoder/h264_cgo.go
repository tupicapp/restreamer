//go:build cgo && media

package decoder

/*
#cgo pkg-config: libavcodec libavutil libswscale
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libswscale/swscale.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int iraj_averror_eagain() { return AVERROR(EAGAIN); }
static int iraj_averror_eof() { return AVERROR_EOF; }
static int iraj_av_strerror_wrap(int errnum, char *buf, size_t buflen) {
	return av_strerror(errnum, buf, buflen);
}
static void iraj_av_freep(void *ptr) { av_freep(ptr); }
static int iraj_frame_is_key(const AVFrame *frame) {
	return (frame->flags & AV_FRAME_FLAG_KEY) != 0;
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

	"github.com/nareix/joy4/codec/h264parser"
)

type h264Decoder struct {
	id     string
	input  <-chan *shared.Frame
	output chan *raw.VideoFrame
	errors chan error
	done   chan struct{}

	cfg       h264DecoderConfig
	startOnce sync.Once
	closeOnce sync.Once

	codecCtx *C.AVCodecContext
	packet   *C.AVPacket
	frame    *C.AVFrame
	swsCtx   *C.struct_SwsContext

	sequenceID int64

	metaMu    sync.Mutex
	metaByPTS map[int64]decoderMeta

	cachedSPS []byte
	cachedPPS []byte
}

type decoderMeta struct {
	InputID   string
	Timestamp time.Time
	Duration  time.Duration
	GOPID     int64
}

func NewH264Decoder(id string, input <-chan *shared.Frame, opts ...H264DecoderOption) (VideoDecoder, error) {
	if input == nil {
		return nil, fmt.Errorf("h264 decoder input channel is nil")
	}

	cfg := h264DecoderConfig{
		outputBuffer: 100,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.outputBuffer <= 0 {
		cfg.outputBuffer = 100
	}
	if (cfg.outputWidth == 0) != (cfg.outputHeight == 0) {
		return nil, fmt.Errorf("decoder output width and height must both be set or both be zero")
	}
	if cfg.outputWidth < 0 || cfg.outputHeight < 0 {
		return nil, fmt.Errorf("decoder output size must be positive")
	}

	return &h264Decoder{
		id:        id,
		input:     input,
		output:    make(chan *raw.VideoFrame, cfg.outputBuffer),
		errors:    make(chan error, 8),
		done:      make(chan struct{}),
		cfg:       cfg,
		metaByPTS: make(map[int64]decoderMeta),
	}, nil
}

func (d *h264Decoder) Start() error {
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

func (d *h264Decoder) Output() <-chan *raw.VideoFrame { return d.output }

func (d *h264Decoder) Errors() <-chan error { return d.errors }

func (d *h264Decoder) Close() error {
	d.closeOnce.Do(func() {
		close(d.done)
	})
	return nil
}

func (d *h264Decoder) run() {
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
			if frame.Codec != "" && frame.Codec != "h264" {
				d.reportError(fmt.Errorf("decoder %s: unsupported codec %q", d.id, frame.Codec))
				continue
			}

			au, hasPicture := d.buildAnnexBAccessUnit(frame)
			if len(au) == 0 {
				continue
			}

			if err := d.sendAccessUnit(frame, au, hasPicture); err != nil {
				d.reportError(err)
				continue
			}
			if err := d.receiveFrames(); err != nil {
				d.reportError(err)
			}
		}
	}
}

func (d *h264Decoder) initDecoder() error {
	codec := C.avcodec_find_decoder(C.AV_CODEC_ID_H264)
	if codec == nil {
		return fmt.Errorf("h264 decoder %s: avcodec H264 decoder not found", d.id)
	}

	d.codecCtx = C.avcodec_alloc_context3(codec)
	if d.codecCtx == nil {
		return fmt.Errorf("h264 decoder %s: failed to allocate codec context", d.id)
	}

	d.codecCtx.thread_count = 1

	if ret := C.avcodec_open2(d.codecCtx, codec, nil); ret < 0 {
		return avErr("h264 decoder "+d.id+": avcodec_open2 failed", ret)
	}

	d.packet = C.av_packet_alloc()
	if d.packet == nil {
		return fmt.Errorf("h264 decoder %s: failed to allocate packet", d.id)
	}

	d.frame = C.av_frame_alloc()
	if d.frame == nil {
		return fmt.Errorf("h264 decoder %s: failed to allocate frame", d.id)
	}

	return nil
}

func (d *h264Decoder) releaseDecoder() {
	if d.swsCtx != nil {
		C.sws_freeContext(d.swsCtx)
		d.swsCtx = nil
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

func (d *h264Decoder) buildAnnexBAccessUnit(frame *shared.Frame) ([]byte, bool) {
	var hasSPS bool
	var hasPPS bool
	var hasPicture bool
	var hasIDR bool

	for _, nalu := range frame.Payload {
		stripped := decoderStripAnnexBStartCode(nalu)
		if len(stripped) == 0 {
			continue
		}

		switch stripped[0] & 0x1F {
		case 7:
			d.cachedSPS = append(d.cachedSPS[:0], stripped...)
			hasSPS = true
		case 8:
			d.cachedPPS = append(d.cachedPPS[:0], stripped...)
			hasPPS = true
		case 1:
			hasPicture = true
		case 5:
			hasPicture = true
			hasIDR = true
		}
	}

	out := make([]byte, 0, estimateAnnexBSize(frame.Payload)+16)

	if hasIDR && !hasSPS && len(d.cachedSPS) > 0 {
		out = append(out, decoderPrependStartCode(d.cachedSPS)...)
	}
	if hasIDR && !hasPPS && len(d.cachedPPS) > 0 {
		out = append(out, decoderPrependStartCode(d.cachedPPS)...)
	}

	for _, nalu := range frame.Payload {
		stripped := decoderStripAnnexBStartCode(nalu)
		if len(stripped) == 0 {
			continue
		}
		out = append(out, decoderPrependStartCode(stripped)...)
	}

	return out, hasPicture
}

func estimateAnnexBSize(nalus [][]byte) int {
	total := 0
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		total += len(decoderStripAnnexBStartCode(nalu)) + 4
	}
	return total
}

func (d *h264Decoder) sendAccessUnit(frame *shared.Frame, au []byte, hasPicture bool) error {
	if len(au) == 0 {
		return nil
	}

	C.av_packet_unref(d.packet)
	if ret := C.av_new_packet(d.packet, C.int(len(au))); ret < 0 {
		return avErr("h264 decoder "+d.id+": av_new_packet failed", ret)
	}

	C.memcpy(unsafe.Pointer(d.packet.data), unsafe.Pointer(&au[0]), C.size_t(len(au)))

	pts := framePTSKey(frame)
	d.packet.pts = C.int64_t(pts)
	d.packet.dts = C.int64_t(frame.DTS.Nanoseconds())
	if frame.IsKeyFrame {
		d.packet.flags |= C.AV_PKT_FLAG_KEY
	}

	if hasPicture {
		d.metaMu.Lock()
		d.metaByPTS[pts] = decoderMeta{
			InputID:   frame.InputID,
			Timestamp: frame.Timestamp,
			Duration:  frame.Duration,
			GOPID:     frame.GOPID,
		}
		d.metaMu.Unlock()
	}

	ret := C.avcodec_send_packet(d.codecCtx, d.packet)
	C.av_packet_unref(d.packet)
	if ret < 0 {
		return avErr("h264 decoder "+d.id+": avcodec_send_packet failed", ret)
	}

	return nil
}

func framePTSKey(frame *shared.Frame) int64 {
	if frame.PTS != 0 {
		return frame.PTS.Nanoseconds()
	}
	if frame.DTS != 0 {
		return frame.DTS.Nanoseconds()
	}
	if frame.SequenceID != 0 {
		return frame.SequenceID
	}
	return time.Now().UnixNano()
}

func (d *h264Decoder) flushDecoder() error {
	if ret := C.avcodec_send_packet(d.codecCtx, nil); ret < 0 {
		return avErr("h264 decoder "+d.id+": flush failed", ret)
	}
	return d.receiveFrames()
}

func (d *h264Decoder) receiveFrames() error {
	for {
		ret := C.avcodec_receive_frame(d.codecCtx, d.frame)
		switch ret {
		case 0:
			raw, err := d.copyDecodedFrame(d.frame)
			C.av_frame_unref(d.frame)
			if err != nil {
				return err
			}

			select {
			case d.output <- raw:
			case <-d.done:
				return nil
			}
		case C.iraj_averror_eagain(), C.iraj_averror_eof():
			return nil
		default:
			return avErr("h264 decoder "+d.id+": avcodec_receive_frame failed", ret)
		}
	}
}

func (d *h264Decoder) copyDecodedFrame(src *C.AVFrame) (*raw.VideoFrame, error) {
	dstWidth := d.cfg.outputWidth
	dstHeight := d.cfg.outputHeight
	if dstWidth == 0 || dstHeight == 0 {
		dstWidth = int(src.width)
		dstHeight = int(src.height)
	}

	d.swsCtx = C.sws_getCachedContext(
		d.swsCtx,
		src.width,
		src.height,
		(C.enum_AVPixelFormat)(src.format),
		C.int(dstWidth),
		C.int(dstHeight),
		C.AV_PIX_FMT_YUV420P,
		C.SWS_BILINEAR,
		nil,
		nil,
		nil,
	)
	if d.swsCtx == nil {
		return nil, fmt.Errorf("h264 decoder %s: failed to initialize swscale context", d.id)
	}

	var dstData [4]*C.uint8_t
	var dstLinesize [4]C.int

	size := C.av_image_alloc(
		(**C.uint8_t)(unsafe.Pointer(&dstData[0])),
		(*C.int)(unsafe.Pointer(&dstLinesize[0])),
		C.int(dstWidth),
		C.int(dstHeight),
		C.AV_PIX_FMT_YUV420P,
		1,
	)
	if size < 0 {
		return nil, avErr("h264 decoder "+d.id+": av_image_alloc failed", size)
	}
	defer C.iraj_av_freep(unsafe.Pointer(&dstData[0]))

	if ret := C.sws_scale(
		d.swsCtx,
		(**C.uint8_t)(unsafe.Pointer(&src.data[0])),
		(*C.int)(unsafe.Pointer(&src.linesize[0])),
		0,
		src.height,
		(**C.uint8_t)(unsafe.Pointer(&dstData[0])),
		(*C.int)(unsafe.Pointer(&dstLinesize[0])),
	); ret <= 0 {
		return nil, fmt.Errorf("h264 decoder %s: sws_scale failed", d.id)
	}

	buf := C.GoBytes(unsafe.Pointer(dstData[0]), size)
	pts := int64(src.best_effort_timestamp)
	meta := d.takeMeta(pts)

	d.sequenceID++

	return &raw.VideoFrame{
		Frame: &shared.Frame{
			PTS:        time.Duration(pts),
			DTS:        time.Duration(pts),
			Duration:   meta.Duration,
			Payload:    [][]byte{buf},
			Codec:      raw.VideoCodec,
			PacketType: raw.YUV420PPixFmt,
			Timestamp:  meta.TimestampOrNow(),
			InputID:    meta.InputIDOr(d.id),
			IsKeyFrame: C.iraj_frame_is_key(src) != 0,
			SequenceID: d.sequenceID,
			GOPID:      meta.GOPID,
		},
		Width:  dstWidth,
		Height: dstHeight,
		PixFmt: raw.YUV420PPixFmt,
	}, nil
}

func (m decoderMeta) TimestampOrNow() time.Time {
	if !m.Timestamp.IsZero() {
		return m.Timestamp
	}
	return time.Now()
}

func (m decoderMeta) InputIDOr(fallback string) string {
	if m.InputID != "" {
		return m.InputID
	}
	return fallback
}

func (d *h264Decoder) takeMeta(pts int64) decoderMeta {
	d.metaMu.Lock()
	defer d.metaMu.Unlock()

	meta, ok := d.metaByPTS[pts]
	if ok {
		delete(d.metaByPTS, pts)
		return meta
	}

	return decoderMeta{}
}

func (d *h264Decoder) reportError(err error) {
	if err == nil {
		return
	}
	select {
	case d.errors <- err:
	default:
	}
}

func avErr(prefix string, code C.int) error {
	buf := make([]byte, 256)
	if ret := C.iraj_av_strerror_wrap(code, (*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))); ret == 0 {
		n := 0
		for n < len(buf) && buf[n] != 0 {
			n++
		}
		return fmt.Errorf("%s: %s", prefix, string(buf[:n]))
	}
	return fmt.Errorf("%s: ffmpeg error %d", prefix, int(code))
}

func InferResolutionFromH264Frame(frame *shared.Frame) (int, int, bool) {
	if frame == nil {
		return 0, 0, false
	}

	for _, nalu := range frame.Payload {
		stripped := decoderStripAnnexBStartCode(nalu)
		if len(stripped) == 0 || stripped[0]&0x1F != 7 {
			continue
		}

		sps, err := h264parser.ParseSPS(stripped)
		if err != nil {
			continue
		}
		if sps.Width <= 0 || sps.Height <= 0 {
			continue
		}
		return int(sps.Width), int(sps.Height), true
	}

	return 0, 0, false
}

func decoderStripAnnexBStartCode(nalu []byte) []byte {
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

func decoderPrependStartCode(nalu []byte) []byte {
	startCode := []byte{0x00, 0x00, 0x00, 0x01}
	return append(startCode, nalu...)
}
