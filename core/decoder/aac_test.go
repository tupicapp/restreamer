package decoder

import "testing"

func TestBuildAACLCMPEG4AudioConfig(t *testing.T) {
	got, err := buildAACLCMPEG4AudioConfig(44100, 2)
	if err != nil {
		t.Fatalf("buildAACLCMPEG4AudioConfig() error = %v", err)
	}

	want := []byte{0x12, 0x10}
	if len(got) != len(want) {
		t.Fatalf("config length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("config[%d] = 0x%02x, want 0x%02x", i, got[i], want[i])
		}
	}
}

func TestValidateAACDecoderConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     aacDecoderConfig
		wantErr bool
	}{
		{
			name: "raw with config",
			cfg: aacDecoderConfig{
				transport:   AACTransportRaw,
				audioConfig: []byte{0x12, 0x10},
			},
		},
		{
			name: "adts without config",
			cfg: aacDecoderConfig{
				transport: AACTransportADTS,
			},
		},
		{
			name: "raw without config",
			cfg: aacDecoderConfig{
				transport: AACTransportRaw,
			},
			wantErr: true,
		},
		{
			name: "unsupported transport",
			cfg: aacDecoderConfig{
				transport: AACTransport("latm"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		err := validateAACDecoderConfig(tt.cfg)
		if tt.wantErr && err == nil {
			t.Fatalf("%s: expected error", tt.name)
		}
		if !tt.wantErr && err != nil {
			t.Fatalf("%s: unexpected error %v", tt.name, err)
		}
	}
}
