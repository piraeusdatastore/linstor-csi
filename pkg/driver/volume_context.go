package driver

import (
	"strings"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

const (
	VolumeContextMarker = linstor.ParameterNamespace + "/uses-volume-context"
	MountOptions        = linstor.ParameterNamespace + "/mount-options"
	PostMountXfsOpts    = linstor.ParameterNamespace + "/post-mount-xfs-opts"
)

// VolumeContext stores the context parameters required to mount a volume.
type VolumeContext struct {
	MountOptions        []string
	PostMountXfsOptions string
}

// NewVolumeContext creates a new default volume context, which does not specify any fancy mkfs/mount/post-mount options
func NewVolumeContext() *VolumeContext {
	return &VolumeContext{}
}

func VolumeContextFromParameters(params *volume.Parameters) *VolumeContext {
	mountOpts := parseMountOpts(params.MountOpts)

	return &VolumeContext{
		MountOptions:        mountOpts,
		PostMountXfsOptions: params.PostMountXfsOpts,
	}
}

func VolumeContextFromMap(ctx map[string]string) *VolumeContext {
	_, ok := ctx[VolumeContextMarker]
	if !ok {
		return nil
	}

	mountOpts := parseMountOpts(ctx[MountOptions])

	return &VolumeContext{
		MountOptions:        mountOpts,
		PostMountXfsOptions: ctx[PostMountXfsOpts],
	}
}

func (v *VolumeContext) ToMap() map[string]string {
	return map[string]string{
		VolumeContextMarker: "true",
		MountOptions:        encodeMountOpts(v.MountOptions),
		PostMountXfsOpts:    v.PostMountXfsOptions,
	}
}

func parseMountOpts(opts string) []string {
	if opts == "" {
		return nil
	}

	return strings.Split(opts, ",")
}

func encodeMountOpts(opts []string) string {
	return strings.Join(opts, ",")
}
