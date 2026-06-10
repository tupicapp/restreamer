package manifest

import (
	"fmt"

	"github.com/tupicapp/restreamer/core/timeline"
)

func CompileManifest(m Manifest) (timeline.Plan, error) {
	if err := ValidateManifest(m); err != nil {
		return timeline.Plan{}, err
	}

	var plan timeline.Plan
	var err error
	switch m.Version {
	case "1.0":
		plan, err = compileV1(m)
	default:
		return timeline.Plan{}, fmt.Errorf("manifest version %q is not supported yet", m.Version)
	}
	if err != nil {
		return timeline.Plan{}, err
	}
	if err := timeline.ValidatePlan(plan); err != nil {
		return timeline.Plan{}, err
	}
	return plan, nil
}
