package utils

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
)

// Fsck runs fsck on an unmounted device in "automatic" mode.
//
// The device is expected to contain a valid xfs or ext4 filesystem.
func Fsck(ctx context.Context, device string) error {
	// -l: lock the devices in /run/fsck/<device>.lock
	// -p: run into "auto" mode, not requesting confirmation on TTY.
	// -M: do not run if filesystem is mounted, which can happen if the volume is used multiple times on the same node.
	//     This is an option that is specific to fsck.ext4 and fsck.xfs, passed along by fsck.
	out, err := exec.CommandContext(ctx, "fsck", "-l", "-p", "-M", device).CombinedOutput()
	if err != nil {
		var execErr *exec.ExitError
		if errors.As(err, &execErr) && (execErr.ExitCode() <= 1 || execErr.ExitCode() == 8) {
			// Exit code 0: No error
			// Exit code 1: Filesystem errors corrected
			// Exit code 8: Operational error (when the volume is mounted in another mount namespace, and we do not see
			//                                 it in our /proc/mounts, so "-M" does not work)
			return nil
		}

		return fmt.Errorf("failed to run fsck: output: \"%s\", %w", out, err)
	}

	return nil
}
