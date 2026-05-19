package raw

type ProcessRequest struct {
	Canvas     CanvasSpec
	Placements []VideoPlacement
}

type Processor interface {
	Process(ProcessRequest) (*VideoFrame, error)
}

type Composer struct{}

func NewComposer() Processor {
	return &Composer{}
}

func (c *Composer) Process(req ProcessRequest) (*VideoFrame, error) {
	return ComposeYUV420P(req.Canvas, req.Placements)
}
