//go:build !linux || !cgo

package gbase8acdc

import "errors"

func OpenNativeProvider(path, wantSHA256, configJSON string) (Agent, error) {
	return nil, errors.New("GBase 8a CDC native C ABI provider requires Linux with CGO enabled")
}
