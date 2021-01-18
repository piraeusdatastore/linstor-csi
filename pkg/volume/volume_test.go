package volume_test

import (
	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDisklessFlag(t *testing.T) {
	testcases := []struct {
		name     string
		params   volume.Parameters
		expected string
		isError  bool
	}{
		{
			name:     "default-layers",
			params:   volume.Parameters{LayerList: []lapi.LayerType{lapi.DRBD, lapi.STORAGE}},
			expected: lc.FlagDrbdDiskless,
			isError:  false,
		},
		{
			name:     "nvme-layers",
			params:   volume.Parameters{LayerList: []lapi.LayerType{lapi.NVME}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "both-diskless",
			params:   volume.Parameters{LayerList: []lapi.LayerType{lapi.DRBD, lapi.NVME}},
			expected: lc.FlagDrbdDiskless,
			isError:  false,
		},
		{
			name:     "both-reversed",
			params:   volume.Parameters{LayerList: []lapi.LayerType{lapi.NVME, lapi.DRBD}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "openflex-like-nvme",
			params:   volume.Parameters{LayerList: []lapi.LayerType{lapi.OPENFLEX, lapi.STORAGE}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "no-diskless-layer",
			params:   volume.Parameters{LayerList: []lapi.LayerType{lapi.CACHE, lapi.STORAGE}},
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
	}{
		{
			name:           "matching-rg-is-empty-modify",
			params:         volume.Parameters{DRBDOpts: map[string]string{"DrbdOptions/Net/Protocol": "C"}, LayerList: []lapi.LayerType{lapi.DRBD, lapi.STORAGE}, PlacementCount: 2, ResourceGroup: "matching-rg-is-empty-modify"},
			existing:       lapi.ResourceGroup{Name: "matching-rg-is-empty-modify", Props: map[string]string{lc.KeyStorPoolName: "", "DrbdOptions/Net/Protocol": "C"}, SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(lapi.DRBD), string(lapi.STORAGE)}, PlaceCount: 2}},
			expectedModify: lapi.ResourceGroupModify{OverrideProps: map[string]string{}},
		},
		{
			name:     "wrong-select-filters",
			params:   volume.Parameters{LayerList: []lapi.LayerType{lapi.WRITECACHE, lapi.DRBD, lapi.STORAGE}, PlacementCount: 3, ResourceGroup: "wrong-select-filters", StoragePool: "pool"},
			existing: lapi.ResourceGroup{Name: "wrong-select-filters", SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(lapi.DRBD)}, PlaceCount: 2}},
			expectedModify: lapi.ResourceGroupModify{
				OverrideProps: map[string]string{
					lc.KeyStorPoolName: "pool",
				},
				SelectFilter: lapi.AutoSelectFilter{
					LayerStack:  []string{string(lapi.WRITECACHE), string(lapi.DRBD), string(lapi.STORAGE)},
					PlaceCount:  3,
					StoragePool: "pool",
				},
			},
			expectedChanged: true,
		},
		{
			name:     "override-and-delete-props",
			params:   volume.Parameters{DRBDOpts: map[string]string{"DrbdOptions/Net/Protocol": "C"}, LayerList: []lapi.LayerType{lapi.DRBD, lapi.STORAGE}, PlacementCount: 2, ResourceGroup: "override-and-delete-props"},
			existing: lapi.ResourceGroup{Name: "override-and-delete-props", Props: map[string]string{lc.KeyStorPoolName: "", "DrbdOptions/Net/Protocol": "A", "DrbdOptions/Foo/Bar": "baz"}, SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(lapi.DRBD), string(lapi.STORAGE)}, PlaceCount: 2}},
			expectedModify: lapi.ResourceGroupModify{
				OverrideProps: map[string]string{"DrbdOptions/Net/Protocol": "C"},
				DeleteProps:   []string{"DrbdOptions/Foo/Bar"},
			},
			expectedChanged: true,
		},
	}

	t.Parallel()
	for i := range testcases {
		tcase := testcases[i]
		t.Run(tcase.name, func(t *testing.T) {
			actualModified, actualChanged := tcase.params.ToResourceGroupModify(&tcase.existing)
			assert.Equal(t, tcase.expectedChanged, actualChanged)
			assert.Equal(t, tcase.expectedModify, actualModified)
		})
	}
}
