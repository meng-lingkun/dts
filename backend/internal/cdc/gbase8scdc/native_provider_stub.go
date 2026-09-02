//go:build !linux || !cgo

package gbase8scdc

import "errors"

func OpenNativeProvider(path, wantSHA256, configJSON string) (Agent, error) {
	return nil, errors.New("GBase 8s CDC native C ABI provider requires Linux with CGO enabled")
}
