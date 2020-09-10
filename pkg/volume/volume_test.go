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
			name: "openflex-like-nvme",
			params: volume.Parameters{LayerList: []lapi.LayerType{lapi.OPENFLEX, lapi.STORAGE}},
			expected: lc.FlagNvmeInitiator,
			isError: false,
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
