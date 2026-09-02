//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package spoolfile

import "syscall"

func diskCapacity(root string) (capacity int64, available int64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(root, &st); err != nil {
		return 0, 0, err
	}
	return int64(st.Blocks) * int64(st.Bsize), int64(st.Bavail) * int64(st.Bsize), nil
}
