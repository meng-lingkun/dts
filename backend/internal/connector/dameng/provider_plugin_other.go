//go:build !linux

package damengconnector

import (
	"fmt"
	"strings"
)

func loadDriverPlugin(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return fmt.Errorf("QMIGRATION_DAMENG_DRIVER_PLUGIN is currently supported only on Linux")
}
