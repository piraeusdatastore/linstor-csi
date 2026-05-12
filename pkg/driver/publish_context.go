package driver

import (
	"github.com/piraeusdatastore/nri-volume-qos/pkg/meta"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
)

const (
	PublishContextMarker = linstor.ParameterNamespace + "/uses-publish-context"
	DevicePath           = linstor.ParameterNamespace + "/device-path"
	FsType               = linstor.ParameterNamespace + "/fs-type"
)

type PublishContext struct {
	FsType      string
	DevicePath  string
	IOQoSLimits meta.Limits
}

func PublishContextFromMap(ctx map[string]string) *PublishContext {
	_, ok := ctx[PublishContextMarker]
	if !ok {
		return nil
	}

	// limits is the zero value when no QoS metadata is present, so the bool result can be ignored.
	limits, _ := meta.FromMap(ctx)

	return &PublishContext{
		DevicePath:  ctx[DevicePath],
		FsType:      ctx[FsType],
		IOQoSLimits: limits,
	}
}

func (p *PublishContext) ToMap() map[string]string {
	m := map[string]string{
		PublishContextMarker: "true",
		DevicePath:           p.DevicePath,
		FsType:               p.FsType,
	}

	// The device is only set when IO QoS limits are configured; meta.Limits.ToMap then emits the
	// qos.linbit.com/* metadata keys (device plus any non-zero limits) consumed by nri-volume-qos.
	if p.IOQoSLimits.Device != "" {
		for k, v := range p.IOQoSLimits.ToMap() {
			m[k] = v
		}
	}

	return m
}
