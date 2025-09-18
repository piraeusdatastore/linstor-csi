package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

// prepareDeviceLinks is a poor man's replacement for udev when running in a container.
// Based on the DRBD resource name, it creates symlinks /dev/drbd/by-res/<vln> -> /dev/drbdXXXX.
// Optionally, if HOST_DEV_PREFIX environment is set, it will use $HOST_DEV_PREFIX/dev/drbdXXXX as
// target.
func prepareDeviceLinks(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected one argument: <resource-name>")
	}

	resource := args[0]

	out, err := exec.CommandContext(ctx, "drbdsetup", "show", "--json", resource).Output()
	if err != nil {
		return fmt.Errorf("'drbdsetup show --json %s' failed: %w", resource, err)
	}

	var show []DrbdSetupShow

	err = json.Unmarshal(out, &show)
	if err != nil {
		return fmt.Errorf("'drbdsetup show --json %s' failed to parse json: %w", resource, err)
	}

	if len(show) != 1 {
		return fmt.Errorf("'drbdsetup show --json %s' has %d results, expected 1", resource, len(show))
	}

	hostDevDir := filepath.Join(os.Getenv("HOST_DEV_PREFIX"), "/dev")
	linkDir := filepath.Join("/dev/drbd/by-res/", resource)

	err = os.MkdirAll(linkDir, 0o755)
	if err != nil {
		return fmt.Errorf("failed to create link directory %s: %w", linkDir, err)
	}

	for _, v := range show[0].ThisHost.Volumes {
		err := os.Symlink(filepath.Join(hostDevDir, fmt.Sprintf("drbd%d", v.DeviceMinor)), filepath.Join(linkDir, strconv.FormatInt(v.VolumeNr, 10)))
		if err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}

	return nil
}

type DrbdSetupShow struct {
	ThisHost struct {
		Volumes []struct {
			VolumeNr    int64 `json:"volume_nr"`
			DeviceMinor int   `json:"device_minor"`
		} `json:"volumes"`
	} `json:"_this_host"`
}
