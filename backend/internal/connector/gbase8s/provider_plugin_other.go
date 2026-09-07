//go:build !linux

package gbase8sconnector

import (
	"fmt"
	"strings"
)

func loadDriverPlugin(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return fmt.Errorf("DTS_GBASE8S_DRIVER_PLUGIN is currently supported only on Linux")
}
