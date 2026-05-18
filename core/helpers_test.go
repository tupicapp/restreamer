package irajstreamer

import "testing"

func TestIsValidH264Packet(t *testing.T) {
	if IsValidH264Packet([]byte{0x00, 0x00, 0x01, 0x65}) != true {
		t.Fatalf("expected 0x000001 start code to be valid")
	}
	if IsValidH264Packet([]byte{0x00, 0x00, 0x00, 0x01, 0x65}) != true {
		t.Fatalf("expected 0x00000001 start code to be valid")
	}
	if IsValidH264Packet([]byte{0x01, 0x02, 0x03}) != false {
		t.Fatalf("expected non-start-code data to be invalid")
	}
}

func TestIsValidAACMPEG4AudioPacket(t *testing.T) {
	if IsValidAACMPEG4AudioPacket([]byte{0xFF, 0xF1, 0x00}) != true {
		t.Fatalf("expected valid AAC sync word to be detected")
	}
	if IsValidAACMPEG4AudioPacket([]byte{0x00, 0x00}) != false {
		t.Fatalf("expected invalid AAC packet to be false")
	}
}
