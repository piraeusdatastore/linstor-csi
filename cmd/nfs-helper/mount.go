package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

func mount(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("expected two arguments: <device> <mountpoint>")
	}

	device := args[0]
	mountpoint := args[1]

	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return fmt.Errorf("failed to create mount point directory '%s': %w", mountpoint, err)
	}

	if err := exec.CommandContext(ctx, "mount", device, mountpoint).Run(); err != nil {
		return fmt.Errorf("failed to mount device '%s' on '%s': %w", device, mountpoint, err)
	}

	return nil
}
