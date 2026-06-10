package manifest

import "fmt"

func ValidateManifest(m Manifest) error {
	switch m.Version {
	case "1.0":
		return validateV1(m)
	case "":
		return fmt.Errorf("manifest version is required")
	default:
		return fmt.Errorf("manifest version %q is not supported yet", m.Version)
	}
}
