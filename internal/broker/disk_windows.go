//go:build windows

package broker

import (
	"golang.org/x/sys/windows"
)

func freeDiskSpace(path string, free *uint64) error {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	return windows.GetDiskFreeSpaceEx(p, free, nil, nil)
}
