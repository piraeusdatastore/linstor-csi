package main

import (
	"context"
	"fmt"

	mountutils "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

func growfs(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected one argument: <mount>")
	}

	mounter := mountutils.New("")

	mounts, err := mounter.List()
	if err != nil {
		return fmt.Errorf("failed to list mount points: %w", err)
	}

	for _, m := range mounts {
		if m.Path != args[0] {
			continue
		}

		resizer := mountutils.NewResizeFs(utilexec.New())

		needsResize, err := resizer.NeedResize(m.Device, m.Path)
		if err != nil {
			return fmt.Errorf("failed to determine if %s is needs resizing: %w", m.Path, err)
		}

		if !needsResize {
			// Nothing to do
			return nil
		}

		if _, err := resizer.Resize(m.Device, m.Path); err != nil {
			return fmt.Errorf("failed to resize %s to %s: %w", m.Path, m.Device, err)
		}

		return nil
	}

	return fmt.Errorf("failed to find mount point: %s", args[0])
}
