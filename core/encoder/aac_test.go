package encoder

import "testing"

func TestValidateAACEncoderConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     aacEncoderConfig
		wantErr bool
	}{
		{
			name: "defaults",
			cfg:  aacEncoderConfig{},
		},
		{
			name: "raw",
			cfg: aacEncoderConfig{
				transport: AACTransportRaw,
				bitRate:   128000,
			},
		},
		{
			name: "adts",
			cfg: aacEncoderConfig{
				transport: AACTransportADTS,
				bitRate:   128000,
			},
		},
		{
			name: "bad transport",
			cfg: aacEncoderConfig{
				transport: AACTransport("latm"),
			},
			wantErr: true,
		},
		{
			name: "negative bitrate",
			cfg: aacEncoderConfig{
				bitRate: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		err := validateAACEncoderConfig(tt.cfg)
		if tt.wantErr && err == nil {
			t.Fatalf("%s: expected error", tt.name)
		}
		if !tt.wantErr && err != nil {
			t.Fatalf("%s: unexpected error %v", tt.name, err)
		}
	}
}
