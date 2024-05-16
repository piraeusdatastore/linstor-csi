package driver

import (
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
)

const (
	PublishContextMarker = linstor.ParameterNamespace + "/uses-publish-context"
	DevicePath           = linstor.ParameterNamespace + "/device-path"
)

type PublishContext struct {
	DevicePath string
}

func PublishContextFromMap(ctx map[string]string) *PublishContext {
	_, ok := ctx[PublishContextMarker]
	if !ok {
		return nil
	}

	return &PublishContext{
		DevicePath: ctx[DevicePath],
	}
}

func (p *PublishContext) ToMap() map[string]string {
	return map[string]string{
		PublishContextMarker: "true",
		DevicePath:           p.DevicePath,
	}
}
