//go:build windows

package spoolfile

import (
	"fmt"
	"syscall"
	"unsafe"
)

var getDiskFreeSpaceExW = syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")

func diskCapacity(root string) (capacity int64, available int64, err error) {
	path, err := syscall.UTF16PtrFromString(root)
	if err != nil {
		return 0, 0, err
	}
	var availableBytes uint64
	var totalBytes uint64
	var totalFreeBytes uint64
	ok, _, callErr := getDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(path)),
		uintptr(unsafe.Pointer(&availableBytes)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&totalFreeBytes)),
	)
	if ok == 0 {
		return 0, 0, fmt.Errorf("GetDiskFreeSpaceExW %q: %w", root, callErr)
	}
	if totalBytes > uint64(^uint64(0)>>1) || availableBytes > uint64(^uint64(0)>>1) {
		return 0, 0, fmt.Errorf("disk capacity for %q exceeds int64", root)
	}
	return int64(totalBytes), int64(availableBytes), nil
}
