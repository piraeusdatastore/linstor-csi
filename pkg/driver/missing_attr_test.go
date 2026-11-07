package driver

import (
	"context"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
)

// TestMissingAttrNamesTheMissingArgument drives the real CSI entry points with one required
// argument omitted and asserts the InvalidArgument message names that argument and its own RPC.
func TestMissingAttrNamesTheMissingArgument(t *testing.T) {
	d := &Driver{}
	ctx := context.Background()

	t.Run("NodeGetVolumeStats names VolumePath", func(t *testing.T) {
		_, err := d.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "pvc-11111111-2222-3333-4444-555555555555",
			VolumePath: "",
		})
		assert.ErrorContains(t, err, "it requires a VolumePath and none was provided")
	})

	t.Run("CreateVolume names its own RPC", func(t *testing.T) {
		_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
			Name:               "pvc-11111111-2222-3333-4444-555555555555",
			VolumeCapabilities: nil,
		})
		assert.ErrorContains(t, err, "CreateVolume failed for")
		assert.NotContains(t, err.Error(), "ValidateVolumeCapabilities")
	})

	t.Run("ControllerUnpublishVolume reports the volume it was given", func(t *testing.T) {
		_, err := d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
			VolumeId: "pvc-11111111-2222-3333-4444-555555555555",
			NodeId:   "",
		})
		assert.ErrorContains(t, err, "failed for pvc-11111111-2222-3333-4444-555555555555")
		assert.NotContains(t, err.Error(), "an unknown volume")
	})

	t.Run("ValidateVolumeCapabilities spells VolumeId like its siblings", func(t *testing.T) {
		_, err := d.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
			VolumeId: "",
		})
		assert.ErrorContains(t, err, "it requires a VolumeId and none was provided")
	})
}
