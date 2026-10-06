/*
CSI Driver for Linstor
Copyright © 2019 LINBIT USA, LLC

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

package highlevelclient_test

import (
	"context"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/piraeusdatastore/linstor-csi/pkg/client/mocks"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
)

func TestHighLevelClient_ReservedCapacity(t *testing.T) {
	t.Parallel()

	volumeIn := func(pool string, sizeKiB int64) []lapi.Volume {
		return []lapi.Volume{{StoragePoolName: pool, LayerDataList: []lapi.VolumeLayer{{Data: &lapi.StorageVolume{UsableSizeKib: sizeKiB}}}}}
	}

	r := mocks.ResourceProvider{}
	r.EXPECT().GetResourceView(mock.Anything, []*lapi.ListOpts{{Node: []string{"node-1", "node-2"}, StoragePool: []string{"local-pool"}}}).Return([]lapi.ResourceWithVolumes{
		{Resource: lapi.Resource{Name: "rsc", NodeName: "node-1"}, Volumes: volumeIn("local-pool", 3)},
		{Resource: lapi.Resource{Name: "rsc", NodeName: "node-2"}, Volumes: volumeIn("local-pool", 3)},
	}, nil)

	c := &lc.HighLevelClient{Client: &lapi.Client{Resources: &r}}

	// Replicas in different spaces each reserve capacity, even when queried together.
	reserved, err := c.ReservedCapacity(context.Background(),
		&lapi.StoragePool{StoragePoolName: "local-pool", NodeName: "node-1", FreeSpaceMgrName: "node-1;local-pool"},
		&lapi.StoragePool{StoragePoolName: "local-pool", NodeName: "node-2", FreeSpaceMgrName: "node-2;local-pool"},
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(6), reserved)
	r.AssertExpectations(t)
}
