package driver

import (
	"fmt"
	"net/url"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/fakelinstor"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
)

// listVolumesDriver returns a driver backed by a fake LINSTOR controller that holds the given number of volumes.
func listVolumesDriver(t *testing.T, count int) *Driver {
	t.Helper()

	fake := fakelinstor.New()
	t.Cleanup(fake.Server.Close)

	u, err := url.Parse(fake.Server.URL)
	require.NoError(t, err)

	hl, err := lc.NewHighLevelClient(lapi.BaseURL(u), lapi.HTTPClient(fake.Server.Client()))
	require.NoError(t, err)

	for i := range count {
		name := fmt.Sprintf("pvc-%d", i)

		require.NoError(t, hl.ResourceDefinitions.Create(t.Context(), lapi.ResourceDefinitionCreate{
			ResourceDefinition: lapi.ResourceDefinition{Name: name},
		}))
		require.NoError(t, hl.ResourceDefinitions.CreateVolumeDefinition(t.Context(), name, lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{SizeKib: 1024},
		}))
	}

	cl, err := client.NewLinstor(client.APIClient(hl))
	require.NoError(t, err)

	d, err := NewDriver(cl)
	require.NoError(t, err)

	return d
}

func TestListVolumesPagination(t *testing.T) {
	t.Parallel()

	d := listVolumesDriver(t, 5)

	var (
		pages [][]string
		token string
	)

	for range 5 {
		resp, err := d.ListVolumes(t.Context(), &csi.ListVolumesRequest{MaxEntries: 2, StartingToken: token})
		require.NoError(t, err)

		var ids []string
		for _, e := range resp.GetEntries() {
			ids = append(ids, e.GetVolume().GetVolumeId())
		}

		pages = append(pages, ids)

		token = resp.GetNextToken()
		if token == "" {
			break
		}
	}

	assert.Equal(t, [][]string{{"pvc-0", "pvc-1"}, {"pvc-2", "pvc-3"}, {"pvc-4"}}, pages)
}

func TestListVolumesStartBeyondMaxEntries(t *testing.T) {
	t.Parallel()

	d := listVolumesDriver(t, 5)

	resp, err := d.ListVolumes(t.Context(), &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: "3"})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, "pvc-3", resp.GetEntries()[0].GetVolume().GetVolumeId())
	assert.Equal(t, "4", resp.GetNextToken())
}

func TestPageEnd(t *testing.T) {
	t.Parallel()

	tcases := []struct {
		start, total int
		maxEntries   int32
		end          int
		token        string
	}{
		{start: 0, total: 5, maxEntries: 0, end: 5},
		{start: 0, total: 5, maxEntries: 5, end: 5},
		{start: 0, total: 5, maxEntries: 2, end: 2, token: "2"},
		{start: 2, total: 5, maxEntries: 2, end: 4, token: "4"},
		{start: 4, total: 5, maxEntries: 2, end: 5},
		{start: 5, total: 5, maxEntries: 2, end: 5},
	}

	for _, tc := range tcases {
		end, token := pageEnd(tc.start, tc.total, tc.maxEntries)
		assert.Equal(t, tc.end, end, "start %d, total %d, max %d", tc.start, tc.total, tc.maxEntries)
		assert.Equal(t, tc.token, token, "start %d, total %d, max %d", tc.start, tc.total, tc.maxEntries)
	}
}
