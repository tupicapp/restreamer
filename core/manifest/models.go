package manifest

type BranchType string

const (
	BranchTypeElement BranchType = "element"
	BranchTypeSlot    BranchType = "slot"
)

type SourceType string

const (
	SourceTypeLink       SourceType = "link"
	SourceTypeAsset      SourceType = "asset"
	SourceTypeTransition SourceType = "transition"
)

type BoxMode string

const (
	BoxModeCover    BoxMode = "cover"
	BoxModeOriginal BoxMode = "original"
)

type Manifest struct {
	Version        string          `json:"version"`
	ChannelID      string          `json:"channel_id,omitempty"`
	ActiveSceneID  string          `json:"active_scene_id,omitempty"`
	OutputSettings *OutputSettings `json:"output_settings,omitempty"`
	Lives          []BranchRef     `json:"lives,omitempty"`
	Records        []BranchRef     `json:"records,omitempty"`
	Scenes         []Scene         `json:"scenes"`
}

type OutputSettings struct {
	Resolution      string `json:"resolution,omitempty"`
	Ratio           string `json:"ratio,omitempty"`
	Framerate       int    `json:"framerate,omitempty"`
	VideoBitrate    int    `json:"video_bitrate,omitempty"`
	AudioSampleRate int    `json:"audio_sample_rate,omitempty"`
	VideoCodec      string `json:"video_codec,omitempty"`
	AudioCodec      string `json:"audio_codec,omitempty"`
}

type BranchRef struct {
	BranchType BranchType `json:"branch_type"`
	ID         string     `json:"id"`
}

type Scene struct {
	ID            string           `json:"id"`
	Details       []any            `json:"details,omitempty"`
	Slots         []Slot           `json:"slots"`
	AudioElements [][]AudioElement `json:"audio_elements,omitempty"`
}

type Slot struct {
	ID       string      `json:"slot_id,omitempty"`
	Details  SlotDetails `json:"details,omitempty"`
	Elements []Element   `json:"elements"`
}

type SlotDetails struct {
	Top     int     `json:"top,omitempty"`
	Left    int     `json:"left,omitempty"`
	Width   int     `json:"width,omitempty"`
	Height  int     `json:"height,omitempty"`
	ZIndex  int     `json:"z_index,omitempty"`
	Opacity int     `json:"opacity,omitempty"`
	Volume  int     `json:"volume,omitempty"`
	BoxMode BoxMode `json:"box_mode,omitempty"`
}

type Element struct {
	ID         string         `json:"id,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	SourceType SourceType     `json:"source_type,omitempty"`
	SourceID   string         `json:"source_id,omitempty"`
	URL        string         `json:"url,omitempty"`
	AssetTrim  *AssetTrim     `json:"asset_trim,omitempty"`
	StartsAt   int64          `json:"starts_at"`
	FinishesAt int64          `json:"finishes_at"`
	Filters    []string       `json:"filters,omitempty"`
}

type AssetTrim struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

type AudioElement struct {
	Details    map[string]any `json:"details,omitempty"`
	URL        string         `json:"url,omitempty"`
	StartsAt   int64          `json:"starts_at"`
	FinishesAt int64          `json:"finishes_at"`
}
