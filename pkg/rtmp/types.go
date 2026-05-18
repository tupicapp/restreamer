package rtmp

import (
	"fmt"
	"time"

	"github.com/bluenviron/gortmplib"
)

type SessionCloseInfo struct {
	Code        string
	Description string
	Reason      string
	Err         error
}

type RejectError struct {
	Code        string
	Description string
	Reason      string
}

func (e *RejectError) Error() string {
	if e.Description != "" {
		return e.Description
	}
	return e.Reason
}

type MediaPacketValidator interface {
	ValidateTracks([]*gortmplib.Track) error
	ObserveVideoPacket(pts, dts time.Duration, au [][]byte) error
	ObserveAudioPacket(pts time.Duration, au []byte) error
}

func WriteStatusError(conn *gortmplib.ServerConn, code string, description string) error {
	if conn == nil {
		return nil
	}
	return fmt.Errorf("rtmp status error: %s - %s", code, description)
}
