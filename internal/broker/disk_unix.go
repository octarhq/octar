//go:build !windows

package broker

import "golang.org/x/sys/unix"

func freeDiskSpace(path string, free *uint64) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return err
	}
	// Bavail = blocks available to unprivileged processes; Bsize = block size.
	*free = stat.Bavail * uint64(stat.Bsize) //nolint:gosec // safe cast: Bsize > 0 by kernel contract
	return nil
}
