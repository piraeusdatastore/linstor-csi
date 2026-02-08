package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatDevice_UnsupportedFsType(t *testing.T) {
	ctx := context.Background()
	err := FormatDevice(ctx, "/dev/null", "ntfs")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported filesystem type")
}

func TestFormatDevice_SupportedFsTypes(t *testing.T) {
	// Verify that the supported filesystem types are accepted by the switch
	// (we can't actually format /dev/null, but we can check the command is constructed)
	supported := []string{"ext4", "ext3", "ext2", "xfs"}
	for _, fs := range supported {
		t.Run(fs, func(t *testing.T) {
			ctx := context.Background()
			err := FormatDevice(ctx, "/dev/null", fs)
			// mkfs on /dev/null will fail, but NOT with "unsupported filesystem type"
			if err != nil {
				assert.NotContains(t, err.Error(), "unsupported filesystem type")
			}
		})
	}
}

func TestHasFilesystem_NonexistentDevice(t *testing.T) {
	ctx := context.Background()
	_, err := HasFilesystem(ctx, "/dev/nonexistent-device-12345")
	// blkid should fail with an error (not exit code 2)
	assert.Error(t, err)
}
