//go:build cgo && media

package encoder

/*
#cgo pkg-config: libavcodec libavutil
#include <libavcodec/avcodec.h>
#include <libavutil/avutil.h>
#include <libavutil/imgutils.h>
#include <libavutil/opt.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int iraj_averror_eagain() { return AVERROR(EAGAIN); }
static int iraj_averror_eof() { return AVERROR_EOF; }
static int iraj_av_strerror_wrap(int errnum, char *buf, size_t buflen) {
	return av_strerror(errnum, buf, buflen);
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

type h264Encoder struct {
	id     string
	input  <-chan *raw.VideoFrame
	output chan *shared.Frame
	errors chan error
	done   chan struct{}

	cfg       h264EncoderConfig
	startOnce sync.Once
	closeOnce sync.Once

	codecCtx *C.AVCodecContext
	frame    *C.AVFrame
	packet   *C.AVPacket

	initialized   bool
	width         int
	height        int
	framePTS      int64
	sequenceID    int64
	cachedHeaders [][]byte
}

func NewH264Encoder(id string, input <-chan *raw.VideoFrame, opts ...H264EncoderOption) (VideoEncoder, error) {
	if input == nil {
		return nil, fmt.Errorf("h264 encoder input channel is nil")
	}

	cfg := h264EncoderConfig{
		outputBuffer: 100,
		fps:          25,
		gopSize:      25,
		bitRate:      2_000_000,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.outputBuffer <= 0 {
		cfg.outputBuffer = 100
	}
	if cfg.fps <= 0 {
		cfg.fps = 25
	}
	if cfg.gopSize <= 0 {
		cfg.gopSize = cfg.fps
	}
	if cfg.bitRate <= 0 {
		cfg.bitRate = 2_000_000
	}

	return &h264Encoder{
		id:     id,
		input:  input,
		output: make(chan *shared.Frame, cfg.outputBuffer),
		errors: make(chan error, 8),
		done:   make(chan struct{}),
		cfg:    cfg,
	}, nil
}

func (e *h264Encoder) Start() error {
	e.startOnce.Do(func() {
		go e.run()
	})
	return nil
}

func (e *h264Encoder) Output() <-chan *shared.Frame { return e.output }

func (e *h264Encoder) Errors() <-chan error { return e.errors }

func (e *h264Encoder) Close() error {
	e.closeOnce.Do(func() {
		close(e.done)
	})
	return nil
}

func (e *h264Encoder) run() {
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
				e.reportError(fmt.Errorf("encoder %s: invalid raw frame: %w", e.id, err))
				continue
			}
			if frame.Width <= 0 || frame.Height <= 0 {
				e.reportError(fmt.Errorf("encoder %s: invalid frame size %dx%d", e.id, frame.Width, frame.Height))
				continue
			}
			if !e.initialized {
				if err := e.initEncoder(frame.Width, frame.Height); err != nil {
					e.reportError(err)
					continue
				}
			}
			if frame.Width != e.width || frame.Height != e.height {
				e.reportError(fmt.Errorf(
					"encoder %s: frame size changed from %dx%d to %dx%d; recreate encoder for a new size",
					e.id,
					e.width,
					e.height,
					frame.Width,
					frame.Height,
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

func (e *h264Encoder) initEncoder(width, height int) error {
	codec := C.avcodec_find_encoder(C.AV_CODEC_ID_H264)
	if codec == nil {
		return fmt.Errorf("h264 encoder %s: avcodec H264 encoder not found", e.id)
	}

	e.codecCtx = C.avcodec_alloc_context3(codec)
	if e.codecCtx == nil {
		return fmt.Errorf("h264 encoder %s: failed to allocate codec context", e.id)
	}

	e.codecCtx.width = C.int(width)
	e.codecCtx.height = C.int(height)
	e.codecCtx.pix_fmt = C.AV_PIX_FMT_YUV420P
	e.codecCtx.time_base = C.AVRational{num: 1, den: C.int(e.cfg.fps)}
	e.codecCtx.framerate = C.AVRational{num: C.int(e.cfg.fps), den: 1}
	e.codecCtx.gop_size = C.int(e.cfg.gopSize)
	e.codecCtx.max_b_frames = 0
	e.codecCtx.bit_rate = C.int64_t(e.cfg.bitRate)

	preset := C.CString("veryfast")
	tune := C.CString("zerolatency")
	presetKey := C.CString("preset")
	tuneKey := C.CString("tune")
	defer C.free(unsafe.Pointer(preset))
	defer C.free(unsafe.Pointer(tune))
	defer C.free(unsafe.Pointer(presetKey))
	defer C.free(unsafe.Pointer(tuneKey))
	_ = C.av_opt_set(e.codecCtx.priv_data, presetKey, preset, 0)
	_ = C.av_opt_set(e.codecCtx.priv_data, tuneKey, tune, 0)

	if ret := C.avcodec_open2(e.codecCtx, codec, nil); ret < 0 {
		return avErr("h264 encoder "+e.id+": avcodec_open2 failed", ret)
	}

	e.frame = C.av_frame_alloc()
	if e.frame == nil {
		return fmt.Errorf("h264 encoder %s: failed to allocate frame", e.id)
	}
	e.frame.format = C.int(C.AV_PIX_FMT_YUV420P)
	e.frame.width = C.int(width)
	e.frame.height = C.int(height)
	if ret := C.av_frame_get_buffer(e.frame, 32); ret < 0 {
		return avErr("h264 encoder "+e.id+": av_frame_get_buffer failed", ret)
	}

	e.packet = C.av_packet_alloc()
	if e.packet == nil {
		return fmt.Errorf("h264 encoder %s: failed to allocate packet", e.id)
	}

	e.width = width
	e.height = height
	e.cachedHeaders = h264CodecHeaders(e.codecCtx)
	e.initialized = true
	return nil
}

func (e *h264Encoder) releaseEncoder() {
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

func (e *h264Encoder) sendFrame(frame *raw.VideoFrame) error {
	if ret := C.av_frame_make_writable(e.frame); ret < 0 {
		return avErr("h264 encoder "+e.id+": av_frame_make_writable failed", ret)
	}

	yPlane, uPlane, vPlane := raw.SplitYUV420P(frame.Frame.Payload[0], frame.Width, frame.Height)
	copyPlane(e.frame.data[0], e.frame.linesize[0], yPlane, frame.Width, frame.Height)
	copyPlane(e.frame.data[1], e.frame.linesize[1], uPlane, frame.Width/2, frame.Height/2)
	copyPlane(e.frame.data[2], e.frame.linesize[2], vPlane, frame.Width/2, frame.Height/2)

	e.frame.pts = C.int64_t(e.framePTS)
	e.framePTS++

	if ret := C.avcodec_send_frame(e.codecCtx, e.frame); ret < 0 {
		return avErr("h264 encoder "+e.id+": avcodec_send_frame failed", ret)
	}

	return nil
}

func copyPlane(dst *C.uint8_t, dstLineSize C.int, src []byte, width, height int) {
	if len(src) == 0 || width <= 0 || height <= 0 {
		return
	}

	base := uintptr(unsafe.Pointer(dst))
	srcStride := width
	for y := 0; y < height; y++ {
		dstRow := unsafe.Pointer(base + uintptr(y*int(dstLineSize)))
		srcOffset := y * srcStride
		C.memcpy(dstRow, unsafe.Pointer(&src[srcOffset]), C.size_t(width))
	}
}

func (e *h264Encoder) flushEncoder() error {
	if !e.initialized {
		return nil
	}
	if ret := C.avcodec_send_frame(e.codecCtx, nil); ret < 0 {
		return avErr("h264 encoder "+e.id+": flush failed", ret)
	}
	return e.receivePackets(nil)
}

func (e *h264Encoder) receivePackets(src *raw.VideoFrame) error {
	for {
		ret := C.avcodec_receive_packet(e.codecCtx, e.packet)
		switch ret {
		case 0:
			pkt, err := e.buildOutputFrame(src)
			C.av_packet_unref(e.packet)
			if err != nil {
				return err
			}

			select {
			case e.output <- pkt:
			case <-e.done:
				return nil
			}
		case C.iraj_averror_eagain(), C.iraj_averror_eof():
			return nil
		default:
			return avErr("h264 encoder "+e.id+": avcodec_receive_packet failed", ret)
		}
	}
}

func (e *h264Encoder) buildOutputFrame(src *raw.VideoFrame) (*shared.Frame, error) {
	payloadBytes := C.GoBytes(unsafe.Pointer(e.packet.data), e.packet.size)
	nalus := splitAnnexBAccessUnit(payloadBytes)
	if len(nalus) == 0 && len(payloadBytes) > 0 {
		nalus = [][]byte{append([]byte(nil), payloadBytes...)}
	}

	frameDuration := time.Second / time.Duration(e.cfg.fps)
	out := &shared.Frame{
		Payload:    nalus,
		Codec:      "h264",
		IsKeyFrame: (e.packet.flags & C.AV_PKT_FLAG_KEY) != 0,
		SequenceID: e.nextSequenceID(),
		Duration:   frameDuration,
		Timestamp:  time.Now(),
		InputID:    e.id,
	}

	if src != nil && src.Frame != nil {
		out.PTS = src.Frame.PTS
		out.DTS = src.Frame.DTS
		out.Duration = src.Frame.Duration
		out.Timestamp = src.Frame.Timestamp
		out.InputID = src.Frame.InputID
		out.GOPID = src.Frame.GOPID
	}

	if out.PTS == 0 {
		out.PTS = time.Duration(int64(e.packet.pts)) * frameDuration
	}
	if out.DTS == 0 {
		out.DTS = time.Duration(int64(e.packet.dts)) * frameDuration
	}
	if out.Duration == 0 {
		out.Duration = frameDuration
	}
	if out.Timestamp.IsZero() {
		out.Timestamp = time.Now()
	}
	if out.InputID == "" {
		out.InputID = e.id
	}
	if out.IsKeyFrame {
		out.Payload = prependH264Headers(out.Payload, e.cachedHeaders)
	}

	return out, nil
}

func (e *h264Encoder) nextSequenceID() int64 {
	e.sequenceID++
	return e.sequenceID
}

func (e *h264Encoder) reportError(err error) {
	if err == nil {
		return
	}
	select {
	case e.errors <- err:
	default:
	}
}

func splitAnnexBAccessUnit(au []byte) [][]byte {
	var nalus [][]byte
	for len(au) > 0 {
		start, prefix := findStartCode(au)
		if start < 0 {
			break
		}

		au = au[start+prefix:]
		next, _ := findStartCode(au)
		if next < 0 {
			if len(au) > 0 {
				nalus = append(nalus, append([]byte(nil), au...))
			}
			break
		}
		if next > 0 {
			nalus = append(nalus, append([]byte(nil), au[:next]...))
		}
		au = au[next:]
	}
	return nalus
}

func prependH264Headers(nalus [][]byte, headers [][]byte) [][]byte {
	if len(nalus) == 0 || len(headers) == 0 {
		return nalus
	}

	hasSPS := false
	hasPPS := false
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		switch nalu[0] & 0x1F {
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
	}
	if hasSPS && hasPPS {
		return nalus
	}

	out := make([][]byte, 0, len(headers)+len(nalus))
	if !hasSPS || !hasPPS {
		for _, header := range headers {
			if len(header) == 0 {
				continue
			}
			typ := header[0] & 0x1F
			if typ == 7 && hasSPS {
				continue
			}
			if typ == 8 && hasPPS {
				continue
			}
			out = append(out, append([]byte(nil), header...))
		}
	}
	out = append(out, nalus...)
	return out
}

func h264CodecHeaders(codecCtx *C.AVCodecContext) [][]byte {
	if codecCtx == nil || codecCtx.extradata == nil || codecCtx.extradata_size <= 0 {
		return nil
	}

	data := C.GoBytes(unsafe.Pointer(codecCtx.extradata), codecCtx.extradata_size)
	if headers := parseAVCDecoderConfig(data); len(headers) > 0 {
		return headers
	}

	return splitAnnexBAccessUnit(data)
}

func parseAVCDecoderConfig(data []byte) [][]byte {
	if len(data) < 7 || data[0] != 1 {
		return nil
	}

	pos := 5
	numSPS := int(data[pos] & 0x1F)
	pos++

	var headers [][]byte
	for i := 0; i < numSPS; i++ {
		if pos+2 > len(data) {
			return nil
		}
		size := int(data[pos])<<8 | int(data[pos+1])
		pos += 2
		if pos+size > len(data) {
			return nil
		}
		headers = append(headers, append([]byte(nil), data[pos:pos+size]...))
		pos += size
	}

	if pos >= len(data) {
		return headers
	}

	numPPS := int(data[pos])
	pos++
	for i := 0; i < numPPS; i++ {
		if pos+2 > len(data) {
			return nil
		}
		size := int(data[pos])<<8 | int(data[pos+1])
		pos += 2
		if pos+size > len(data) {
			return nil
		}
		headers = append(headers, append([]byte(nil), data[pos:pos+size]...))
		pos += size
	}

	return headers
}

func findStartCode(buf []byte) (idx int, prefixLen int) {
	for i := 0; i+3 < len(buf); i++ {
		if buf[i] == 0x00 && buf[i+1] == 0x00 {
			if buf[i+2] == 0x01 {
				return i, 3
			}
			if i+3 < len(buf) && buf[i+2] == 0x00 && buf[i+3] == 0x01 {
				return i, 4
			}
		}
	}
	return -1, 0
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
