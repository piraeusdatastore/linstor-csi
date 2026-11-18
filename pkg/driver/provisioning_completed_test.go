package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func TestProvisioningCompleted(t *testing.T) {
	t.Parallel()

	const volId = "pvc-new"

	for _, tt := range []struct {
		name       string
		props      map[string]string
		fromSource bool
		expected   bool
	}{
		{
			name:     "no marker",
			props:    map[string]string{},
			expected: false,
		},
		{
			name:     "marker for this volume",
			props:    map[string]string{linstor.PropertyProvisioningCompletedBy: "linstor-csi/v1", linstor.PropertyProvisioningCompletedFor: volId},
			expected: true,
		},
		{
			name:       "marker for this volume, restored",
			props:      map[string]string{linstor.PropertyProvisioningCompletedBy: "linstor-csi/v1", linstor.PropertyProvisioningCompletedFor: volId},
			fromSource: true,
			expected:   true,
		},
		{
			name:       "marker copied from the source volume",
			props:      map[string]string{linstor.PropertyProvisioningCompletedBy: "linstor-csi/v1", linstor.PropertyProvisioningCompletedFor: "pvc-source"},
			fromSource: true,
			expected:   false,
		},
		{
			name:     "marker without volume ID, no content source",
			props:    map[string]string{linstor.PropertyProvisioningCompletedBy: "linstor-csi/v1"},
			expected: true,
		},
		{
			name:       "marker without volume ID, from a content source",
			props:      map[string]string{linstor.PropertyProvisioningCompletedBy: "linstor-csi/v1"},
			fromSource: true,
			expected:   false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, provisioningCompleted(&volume.Info{Properties: tt.props}, volId, tt.fromSource))
		})
	}
}
