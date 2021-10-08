package volume_test

import (
	"testing"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"
	"github.com/stretchr/testify/assert"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func TestNewParameters(t *testing.T) {
	empty, err := volume.NewParameters(nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, empty.ResourceGroup)

	fixed, err := volume.NewParameters(map[string]string{
		"resourcegroup": "rg1",
	})
	assert.NoError(t, err)
	assert.Equal(t, "rg1", fixed.ResourceGroup)

	fixedWithNamespace, err := volume.NewParameters(map[string]string{
		linstor.ParameterNamespace + "/resourcegroup": "rg1",
	})
	assert.NoError(t, err)
	assert.Equal(t, "rg1", fixedWithNamespace.ResourceGroup)

	expected := map[string]string{
		"DrbdOptions/auto-quorum":  "suspend-io",
		"DrbdOptions/Net/protocol": "C",
	}
	legacy, err := volume.NewParameters(expected)
	assert.NoError(t, err)
	assert.Equal(t, expected, legacy.Properties)

	generalProps, err := volume.NewParameters(map[string]string{
		linstor.PropertyNamespace + "/DrbdOptions/auto-quorum":  "suspend-io",
		linstor.PropertyNamespace + "/DrbdOptions/Net/protocol": "C",
	})
	assert.NoError(t, err)
	assert.Equal(t, expected, generalProps.Properties)
}

func TestDisklessFlag(t *testing.T) {
	testcases := []struct {
		name     string
		params   volume.Parameters
		expected string
		isError  bool
	}{
		{
			name:     "default-layers",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Storage}},
			expected: lc.FlagDrbdDiskless,
			isError:  false,
		},
		{
			name:     "nvme-layers",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Nvme}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "both-diskless",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Nvme}},
			expected: lc.FlagDrbdDiskless,
			isError:  false,
		},
		{
			name:     "both-reversed",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Nvme, devicelayerkind.Drbd}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "openflex-like-nvme",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Openflex, devicelayerkind.Storage}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "no-diskless-layer",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Cache, devicelayerkind.Storage}},
			expected: "",
			isError:  true,
		},
	}

	t.Parallel()
	for _, tcase := range testcases {
		tcase := tcase
		t.Run(tcase.name, func(t *testing.T) {
			actual, err := tcase.params.DisklessFlag()
			assert.Equal(t, tcase.expected, actual)
			if tcase.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParameters_ToResourceGroupModify(t *testing.T) {
	testcases := []struct {
		name            string
		params          volume.Parameters
		existing        lapi.ResourceGroup
		expectedModify  lapi.ResourceGroupModify
		expectedChanged bool
		expectedError   bool
	}{
		{
			name:           "matching-rg-is-empty-modify",
			params:         volume.Parameters{Properties: map[string]string{"DrbdOptions/Net/Protocol": "C"}, LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Storage}, PlacementCount: 2, ResourceGroup: "matching-rg-is-empty-modify"},
			existing:       lapi.ResourceGroup{Name: "matching-rg-is-empty-modify", Props: map[string]string{lc.KeyStorPoolName: "", "DrbdOptions/Net/Protocol": "C"}, SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(devicelayerkind.Drbd), string(devicelayerkind.Storage)}, PlaceCount: 2}},
			expectedModify: lapi.ResourceGroupModify{OverrideProps: map[string]string{}},
		},
		{
			name:     "wrong-select-filters",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Writecache, devicelayerkind.Drbd, devicelayerkind.Storage}, PlacementCount: 3, ResourceGroup: "wrong-select-filters", StoragePool: "pool"},
			existing: lapi.ResourceGroup{Name: "wrong-select-filters", SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(devicelayerkind.Drbd)}, PlaceCount: 2}},
			expectedModify: lapi.ResourceGroupModify{
				OverrideProps: map[string]string{
					lc.KeyStorPoolName: "pool",
				},
				SelectFilter: lapi.AutoSelectFilter{
					LayerStack:  []string{string(devicelayerkind.Writecache), string(devicelayerkind.Drbd), string(devicelayerkind.Storage)},
					PlaceCount:  3,
					StoragePool: "pool",
				},
			},
			expectedChanged: true,
		},
		{
			name:          "differing-props-are-errors",
			params:        volume.Parameters{Properties: map[string]string{"DrbdOptions/Net/Protocol": "C"}, LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Storage}, PlacementCount: 2, ResourceGroup: "differing-props-are-errors"},
			existing:      lapi.ResourceGroup{Name: "differing-props-are-errors", Props: map[string]string{lc.KeyStorPoolName: "", "DrbdOptions/Net/Protocol": "A", "DrbdOptions/Foo/Bar": "baz"}, SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(devicelayerkind.Drbd), string(devicelayerkind.Storage)}, PlaceCount: 2}},
			expectedError: true,
		},
	}

	t.Parallel()
	for i := range testcases {
		tcase := testcases[i]
		t.Run(tcase.name, func(t *testing.T) {
			actualModified, actualChanged, err := tcase.params.ToResourceGroupModify(&tcase.existing)
			assert.Equal(t, tcase.expectedChanged, actualChanged)
			assert.Equal(t, tcase.expectedModify, actualModified)
			assert.Equal(t, tcase.expectedError, err != nil)
		})
	}
}
