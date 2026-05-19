//go:build cgo && media

package raw

/*
#cgo pkg-config: libavutil libswresample
#include <libavutil/avutil.h>
#include <libavutil/channel_layout.h>
#include <libavutil/opt.h>
#include <libavutil/samplefmt.h>
#include <libswresample/swresample.h>
#include <stdlib.h>

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

static int iraj_swr_convert_interleaved_s16(
	struct SwrContext *swr,
	uint8_t *dst,
	int dst_samples,
	const uint8_t *src,
	int src_samples
) {
	uint8_t *out_data[1] = { dst };
	const uint8_t *in_data[1] = { src };
	return swr_convert(swr, out_data, dst_samples, in_data, src_samples);
}

static int iraj_av_opt_set_int_wrap(void *obj, const char *name, int64_t value) {
	return av_opt_set_int(obj, name, value, 0);
}

static int iraj_av_opt_set_double_wrap(void *obj, const char *name, double value) {
	return av_opt_set_double(obj, name, value, 0);
}

static int iraj_av_opt_set_wrap(void *obj, const char *name, const char *value) {
	return av_opt_set(obj, name, value, 0);
}
*/
import "C"

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	shared "restreamer/core/shared"
)

type resampleQualityProfile struct {
	filterSize    int64
	phaseShift    int64
	cutoff        float64
	exactRational bool
	ditherMethod  string
}

func defaultPCM16ResampleQualityProfile() resampleQualityProfile {
	return resampleQualityProfile{
		filterSize:    64,
		phaseShift:    10,
		cutoff:        0.97,
		exactRational: true,
		ditherMethod:  "triangular_hp",
	}
}

type pcm16Resampler struct {
	mu sync.Mutex

	outputSampleRate int
	outputChannels   int

	inputSampleRate int
	inputChannels   int

	swrCtx *C.struct_SwrContext
}

func NewPCM16Resampler(sampleRate int, channels int) (PCM16Resampler, error) {
	if sampleRate <= 0 {
		return nil, fmt.Errorf("invalid output sample rate %d", sampleRate)
	}
	if channels <= 0 {
		return nil, fmt.Errorf("invalid output channel count %d", channels)
	}

	return &pcm16Resampler{
		outputSampleRate: sampleRate,
		outputChannels:   channels,
	}, nil
}

func ConvertPCM16AudioFrame(frame *AudioFrame, sampleRate int, channels int) (*AudioFrame, error) {
	resampler, err := NewPCM16Resampler(sampleRate, channels)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resampler.Close() }()

	return resampler.Convert(frame)
}

func (r *pcm16Resampler) Convert(frame *AudioFrame) (*AudioFrame, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if frame == nil {
		return nil, fmt.Errorf("raw audio frame is nil")
	}
	if err := frame.Validate(); err != nil {
		return nil, err
	}
	if frame.SampleFormat != AudioCodecPCMS16LE {
		return nil, fmt.Errorf("unsupported input sample format %q", frame.SampleFormat)
	}

	if frame.SampleRate == r.outputSampleRate && frame.Channels == r.outputChannels {
		return cloneAudioFrame(frame), nil
	}

	if err := r.ensureContext(frame.SampleRate, frame.Channels); err != nil {
		return nil, err
	}

	inSamples := frame.SamplesPerChannel
	if inSamples <= 0 {
		inSamples = len(frame.Frame.Payload[0]) / (2 * frame.Channels)
	}
	if inSamples <= 0 {
		return nil, fmt.Errorf("invalid input sample count")
	}

	outSamplesCap := int(C.iraj_swr_get_out_samples_wrap(r.swrCtx, C.int(inSamples)))
	if outSamplesCap <= 0 {
		return nil, fmt.Errorf("audio resample invalid output sample capacity %d", outSamplesCap)
	}

	outPayload := make([]byte, outSamplesCap*r.outputChannels*2)
	inData := (*C.uint8_t)(unsafe.Pointer(&frame.Frame.Payload[0][0]))
	outData := (*C.uint8_t)(unsafe.Pointer(&outPayload[0]))

	converted := C.iraj_swr_convert_interleaved_s16(
		r.swrCtx,
		outData,
		C.int(outSamplesCap),
		inData,
		C.int(inSamples),
	)
	if converted < 0 {
		return nil, fmt.Errorf("audio resample swr_convert failed: %d", int(converted))
	}

	outSamples := int(converted)
	outPayload = outPayload[:outSamples*r.outputChannels*2]
	duration := time.Duration(outSamples) * time.Second / time.Duration(r.outputSampleRate)

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
	out.SampleRate = r.outputSampleRate
	out.Channels = r.outputChannels
	out.SampleFormat = AudioCodecPCMS16LE
	out.SamplesPerChannel = outSamples
	return out, nil
}

func (r *pcm16Resampler) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.releaseContext()
	return nil
}

func (r *pcm16Resampler) ensureContext(inputSampleRate int, inputChannels int) error {
	if inputSampleRate <= 0 {
		return fmt.Errorf("invalid input sample rate %d", inputSampleRate)
	}
	if inputChannels <= 0 {
		return fmt.Errorf("invalid input channel count %d", inputChannels)
	}
	if r.swrCtx != nil && r.inputSampleRate == inputSampleRate && r.inputChannels == inputChannels {
		return nil
	}

	r.releaseContext()

	var inLayout C.AVChannelLayout
	var outLayout C.AVChannelLayout
	C.iraj_channel_layout_default_wrap(&inLayout, C.int(inputChannels))
	C.iraj_channel_layout_default_wrap(&outLayout, C.int(r.outputChannels))
	defer C.av_channel_layout_uninit(&inLayout)
	defer C.av_channel_layout_uninit(&outLayout)

	if ret := C.iraj_swr_alloc_set_opts2_wrap(
		&r.swrCtx,
		&outLayout,
		C.AV_SAMPLE_FMT_S16,
		C.int(r.outputSampleRate),
		&inLayout,
		C.AV_SAMPLE_FMT_S16,
		C.int(inputSampleRate),
	); ret < 0 {
		return fmt.Errorf("audio resample swr_alloc_set_opts2 failed: %d", int(ret))
	}
	if r.swrCtx == nil {
		return fmt.Errorf("audio resample failed to allocate context")
	}

	if err := applyResampleQualityProfile(r.swrCtx, defaultPCM16ResampleQualityProfile()); err != nil {
		r.releaseContext()
		return err
	}

	if ret := C.swr_init(r.swrCtx); ret < 0 {
		r.releaseContext()
		return fmt.Errorf("audio resample swr_init failed: %d", int(ret))
	}

	r.inputSampleRate = inputSampleRate
	r.inputChannels = inputChannels
	return nil
}

func (r *pcm16Resampler) releaseContext() {
	if r.swrCtx != nil {
		C.swr_free(&r.swrCtx)
	}
	r.inputSampleRate = 0
	r.inputChannels = 0
}

func applyResampleQualityProfile(swrCtx *C.struct_SwrContext, profile resampleQualityProfile) error {
	if swrCtx == nil {
		return fmt.Errorf("audio resample context is nil")
	}

	if err := setSwrIntOption(swrCtx, "filter_size", profile.filterSize); err != nil {
		return err
	}
	if err := setSwrIntOption(swrCtx, "phase_shift", profile.phaseShift); err != nil {
		return err
	}
	if err := setSwrDoubleOption(swrCtx, "cutoff", profile.cutoff); err != nil {
		return err
	}

	exactRational := int64(0)
	if profile.exactRational {
		exactRational = 1
	}
	if err := setSwrIntOption(swrCtx, "exact_rational", exactRational); err != nil {
		return err
	}

	if profile.ditherMethod != "" {
		if err := setSwrStringOption(swrCtx, "dither_method", profile.ditherMethod); err != nil {
			return err
		}
	}

	return nil
}

func setSwrIntOption(swrCtx *C.struct_SwrContext, name string, value int64) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	if ret := C.iraj_av_opt_set_int_wrap(unsafe.Pointer(swrCtx), cName, C.int64_t(value)); ret < 0 {
		return fmt.Errorf("audio resample av_opt_set_int(%s) failed: %d", name, int(ret))
	}
	return nil
}

func setSwrDoubleOption(swrCtx *C.struct_SwrContext, name string, value float64) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	if ret := C.iraj_av_opt_set_double_wrap(unsafe.Pointer(swrCtx), cName, C.double(value)); ret < 0 {
		return fmt.Errorf("audio resample av_opt_set_double(%s) failed: %d", name, int(ret))
	}
	return nil
}

func setSwrStringOption(swrCtx *C.struct_SwrContext, name string, value string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	if ret := C.iraj_av_opt_set_wrap(unsafe.Pointer(swrCtx), cName, cValue); ret < 0 {
		return fmt.Errorf("audio resample av_opt_set(%s=%s) failed: %d", name, value, int(ret))
	}
	return nil
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
