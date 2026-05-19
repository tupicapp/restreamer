//go:build cgo && media

package raw

/*
#cgo pkg-config: libavutil libswresample
#include <libavutil/avutil.h>
#include <libavutil/channel_layout.h>
#include <libavutil/samplefmt.h>
#include <libswresample/swresample.h>

static void iraj_channel_layout_default_wrap(AVChannelLayout *layout, int channels) {
	av_channel_layout_default(layout, channels);
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

static int iraj_swr_get_out_samples_wrap(struct SwrContext *swr, int in_samples) {
	return swr_get_out_samples(swr, in_samples);
}
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"

	shared "github.com/tupicapp/restreamer/core/shared"
)

func ConvertPCM16AudioFrame(frame *AudioFrame, sampleRate int, channels int) (*AudioFrame, error) {
	if frame == nil {
		return nil, fmt.Errorf("raw audio frame is nil")
	}
	if err := frame.Validate(); err != nil {
		return nil, err
	}
	if frame.SampleFormat != AudioCodecPCMS16LE {
		return nil, fmt.Errorf("unsupported input sample format %q", frame.SampleFormat)
	}
	if sampleRate <= 0 {
		return nil, fmt.Errorf("invalid output sample rate %d", sampleRate)
	}
	if channels <= 0 {
		return nil, fmt.Errorf("invalid output channel count %d", channels)
	}

	if frame.SampleRate == sampleRate && frame.Channels == channels {
		return cloneAudioFrame(frame), nil
	}

	var inLayout C.AVChannelLayout
	var outLayout C.AVChannelLayout
	C.iraj_channel_layout_default_wrap(&inLayout, C.int(frame.Channels))
	C.iraj_channel_layout_default_wrap(&outLayout, C.int(channels))
	defer C.av_channel_layout_uninit(&inLayout)
	defer C.av_channel_layout_uninit(&outLayout)

	var swrCtx *C.struct_SwrContext
	if ret := C.iraj_swr_alloc_set_opts2_wrap(
		&swrCtx,
		&outLayout,
		C.AV_SAMPLE_FMT_S16,
		C.int(sampleRate),
		&inLayout,
		C.AV_SAMPLE_FMT_S16,
		C.int(frame.SampleRate),
	); ret < 0 {
		return nil, fmt.Errorf("audio resample swr_alloc_set_opts2 failed: %d", int(ret))
	}
	if swrCtx == nil {
		return nil, fmt.Errorf("audio resample failed to allocate context")
	}
	defer C.swr_free(&swrCtx)

	if ret := C.swr_init(swrCtx); ret < 0 {
		return nil, fmt.Errorf("audio resample swr_init failed: %d", int(ret))
	}

	inSamples := frame.SamplesPerChannel
	if inSamples <= 0 {
		inSamples = len(frame.Frame.Payload[0]) / (2 * frame.Channels)
	}
	if inSamples <= 0 {
		return nil, fmt.Errorf("invalid input sample count")
	}

	outSamplesCap := int(C.iraj_swr_get_out_samples_wrap(swrCtx, C.int(inSamples)))
	if outSamplesCap <= 0 {
		return nil, fmt.Errorf("audio resample invalid output sample capacity %d", outSamplesCap)
	}

	outPayload := make([]byte, outSamplesCap*channels*2)
	inData := (*C.uint8_t)(unsafe.Pointer(&frame.Frame.Payload[0][0]))
	outData := (*C.uint8_t)(unsafe.Pointer(&outPayload[0]))
	inSlice := []*C.uint8_t{inData}
	outSlice := []*C.uint8_t{outData}

	converted := C.swr_convert(
		swrCtx,
		(**C.uint8_t)(unsafe.Pointer(&outSlice[0])),
		C.int(outSamplesCap),
		(**C.uint8_t)(unsafe.Pointer(&inSlice[0])),
		C.int(inSamples),
	)
	if converted < 0 {
		return nil, fmt.Errorf("audio resample swr_convert failed: %d", int(converted))
	}

	outSamples := int(converted)
	outPayload = outPayload[:outSamples*channels*2]
	duration := time.Duration(outSamples) * time.Second / time.Duration(sampleRate)

	out := cloneAudioFrame(frame)
	out.Frame = &shared.Frame{
		PTS:        frame.Frame.PTS,
		DTS:        frame.Frame.DTS,
		Duration:   duration,
		Payload:    [][]byte{outPayload},
		Codec:      AudioCodecPCMS16LE,
		PacketType: AudioCodecPCMS16LE,
		Timestamp:  frame.Frame.Timestamp,
		InputID:    frame.Frame.InputID,
		IsKeyFrame: frame.Frame.IsKeyFrame,
		SequenceID: frame.Frame.SequenceID,
		GOPID:      frame.Frame.GOPID,
		IsFile:     frame.Frame.IsFile,
	}
	out.SampleRate = sampleRate
	out.Channels = channels
	out.SampleFormat = AudioCodecPCMS16LE
	out.SamplesPerChannel = outSamples
	return out, nil
}

func cloneAudioFrame(frame *AudioFrame) *AudioFrame {
	if frame == nil {
		return nil
	}

	out := *frame
	if frame.Frame != nil {
		cloned := *frame.Frame
		if len(frame.Frame.Payload) > 0 {
			cloned.Payload = make([][]byte, 0, len(frame.Frame.Payload))
			for _, payload := range frame.Frame.Payload {
				cloned.Payload = append(cloned.Payload, append([]byte(nil), payload...))
			}
		}
		out.Frame = &cloned
	}
	return &out
}
