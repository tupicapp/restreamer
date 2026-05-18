package inputs

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluenviron/gohlslib/v2/pkg/playlist"
)

func TestApplyByteRange(t *testing.T) {
	data := []byte("abcdefgh")

	tests := []struct {
		name    string
		start   *uint64
		length  *uint64
		want    string
		wantErr bool
	}{
		{
			name: "full payload",
			want: "abcdefgh",
		},
		{
			name:   "bounded range",
			start:  uint64Ptr(2),
			length: uint64Ptr(3),
			want:   "cde",
		},
		{
			name:    "start beyond payload",
			start:   uint64Ptr(10),
			wantErr: true,
		},
		{
			name:    "length beyond payload",
			start:   uint64Ptr(2),
			length:  uint64Ptr(20),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := applyByteRange(data, tt.start, tt.length)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}

			if err != nil {
				t.Fatalf("applyByteRange failed: %v", err)
			}
			if string(out) != tt.want {
				t.Fatalf("unexpected output: %s", string(out))
			}
		})
	}
}

func TestIsFMP4Segment(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want bool
	}{
		{name: "m4s", uri: "seg.m4s", want: true},
		{name: "m4s with query", uri: "seg.m4s?token=1", want: true},
		{name: "ts", uri: "seg.ts", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFMP4Segment(&playlist.MediaSegment{URI: tt.uri}); got != tt.want {
				t.Fatalf("isFMP4Segment(%q) = %v, want %v", tt.uri, got, tt.want)
			}
		})
	}
}

func TestUnitsToDuration(t *testing.T) {
	tests := []struct {
		name      string
		timestamp uint64
		timeScale uint32
		want      time.Duration
	}{
		{name: "mpegts time scale", timestamp: 90000, timeScale: 90000, want: time.Second},
		{name: "default time scale", timestamp: 45000, want: 500 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unitsToDuration(tt.timestamp, tt.timeScale); got != tt.want {
				t.Fatalf("unitsToDuration(%d, %d) = %v, want %v", tt.timestamp, tt.timeScale, got, tt.want)
			}
		})
	}
}

func TestFetchSegmentData(t *testing.T) {
	data := []byte("abcdefgh")

	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		}))
		defer server.Close()

		out, err := fetchSegmentData(http.DefaultClient, server.URL, uint64Ptr(1), uint64Ptr(4))
		if err != nil {
			t.Fatalf("fetchSegmentData failed: %v", err)
		}
		if string(out) != "bcde" {
			t.Fatalf("unexpected output: %s", string(out))
		}
	})

	t.Run("non-200 response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		if _, err := fetchSegmentData(http.DefaultClient, server.URL, nil, nil); err == nil {
			t.Fatal("expected error for non-200 response")
		}
	})
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}
