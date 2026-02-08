package utils

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
)

// HasFilesystem checks if a device has a recognizable filesystem signature using blkid.
//
// Returns true if a filesystem is detected, false if the device is unformatted.
func HasFilesystem(ctx context.Context, device string) (bool, error) {
	// blkid -p -u filesystem <device>
	//   Exit 0: filesystem signature found
	//   Exit 2: no filesystem signature found
	err := exec.CommandContext(ctx, "blkid", "-p", "-u", "filesystem", device).Run()
	if err == nil {
		return true, nil
	}

	var execErr *exec.ExitError
	if errors.As(err, &execErr) && execErr.ExitCode() == 2 {
		return false, nil
	}

	return false, fmt.Errorf("failed to probe device %s: %w", device, err)
}

// FormatDevice formats a block device with the given filesystem type.
//
// This is intended as a fallback when LINSTOR's server-side filesystem
// creation was interrupted or failed during initial provisioning, leaving
// the device unformatted.
func FormatDevice(ctx context.Context, device, fsType string) error {
	var cmd *exec.Cmd

	switch fsType {
	case "ext4", "ext3", "ext2":
		cmd = exec.CommandContext(ctx, "mkfs."+fsType, device)
	case "xfs":
		cmd = exec.CommandContext(ctx, "mkfs.xfs", device)
	default:
		return fmt.Errorf("unsupported filesystem type for fallback format: %s", fsType)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to format device %s with %s: output: \"%s\", %w", device, fsType, out, err)
	}

	return nil
}
