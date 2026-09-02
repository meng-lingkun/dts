//go:build linux

package gbase8sconnector

import (
	"fmt"
	"plugin"
	"strings"
	"sync"
)

var providerPluginState struct {
	sync.Mutex
	loaded map[string]error
}

func loadDriverPlugin(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	providerPluginState.Lock()
	defer providerPluginState.Unlock()
	if providerPluginState.loaded == nil {
		providerPluginState.loaded = map[string]error{}
	}
	if err, ok := providerPluginState.loaded[path]; ok {
		return err
	}
	_, err := plugin.Open(path)
	if err != nil {
		err = fmt.Errorf("load GBase 8s database/sql provider plugin %s: %w", path, err)
	}
	providerPluginState.loaded[path] = err
	return err
}
