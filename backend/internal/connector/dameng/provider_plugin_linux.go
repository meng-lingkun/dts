//go:build linux

package damengconnector

import (
	"fmt"
	"plugin"
	"strings"
	"sync"
)

var damengPluginState struct {
	sync.Mutex
	loaded map[string]error
}

// loadDriverPlugin loads an optional Go plugin whose init functions register
// the vendor-provided DM database/sql driver. This keeps the proprietary DM
// driver outside QMigration's source archive while allowing the stock Linux
// Server/Worker binaries to activate it at runtime.
func loadDriverPlugin(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	damengPluginState.Lock()
	defer damengPluginState.Unlock()
	if damengPluginState.loaded == nil {
		damengPluginState.loaded = map[string]error{}
	}
	if err, ok := damengPluginState.loaded[path]; ok {
		return err
	}
	_, err := plugin.Open(path)
	if err != nil {
		err = fmt.Errorf("load Dameng database/sql provider plugin %s: %w", path, err)
	}
	damengPluginState.loaded[path] = err
	return err
}
