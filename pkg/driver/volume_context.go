package driver

import (
	"fmt"
	"strings"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

const (
	VolumeContextMarker    = linstor.ParameterNamespace + "/uses-volume-context"
	MountOptions           = linstor.ParameterNamespace + "/mount-options"
	PostMountXfsOpts       = linstor.ParameterNamespace + "/post-mount-xfs-opts"
	RemoteAccessPolicyOpts = linstor.ParameterNamespace + "/remote-access-policy"
)

// VolumeContext stores the context parameters required to mount a volume.
type VolumeContext struct {
	MountOptions        []string
	PostMountXfsOptions string
	RemoteAccessPolicy  volume.RemoteAccessPolicy
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
		RemoteAccessPolicy:  params.AllowRemoteVolumeAccess,
	}
}

func VolumeContextFromMap(ctx map[string]string) (*VolumeContext, error) {
	_, ok := ctx[VolumeContextMarker]
	if !ok {
		return nil, nil
	}

	mountOpts := parseMountOpts(ctx[MountOptions])

	var policy volume.RemoteAccessPolicy

	err := policy.UnmarshalText([]byte(ctx[RemoteAccessPolicyOpts]))
	if err != nil {
		return nil, fmt.Errorf("failed to parse volume context: %w", err)
	}

	return &VolumeContext{
		MountOptions:        mountOpts,
		PostMountXfsOptions: ctx[PostMountXfsOpts],
		RemoteAccessPolicy:  policy,
	}, nil
}

func (v *VolumeContext) ToMap() (map[string]string, error) {
	policy, err := v.RemoteAccessPolicy.MarshalText()
	if err != nil {
		return nil, fmt.Errorf("failed to ecnode remote policy: %w", err)
	}

	return map[string]string{
		VolumeContextMarker:    "true",
		MountOptions:           encodeMountOpts(v.MountOptions),
		PostMountXfsOpts:       v.PostMountXfsOptions,
		RemoteAccessPolicyOpts: string(policy),
	}, nil
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
