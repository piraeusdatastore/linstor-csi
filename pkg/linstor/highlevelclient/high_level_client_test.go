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

	lapiconsts "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/piraeusdatastore/linstor-csi/pkg/client/mocks"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
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

// lunNodes returns a client for node-a and node-b with the "lun" key and node-c without it.
func lunNodes(t *testing.T) *lc.HighLevelClient {
	t.Helper()

	nodeA := lapi.Node{Name: "node-a", Props: map[string]string{"Aux/lun": "yes"}}
	nodeB := lapi.Node{Name: "node-b", Props: map[string]string{"Aux/lun": "yes"}}
	nodeC := lapi.Node{Name: "node-c"}

	m := mocks.NodeProvider{}
	m.EXPECT().GetAll(mock.Anything, []*lapi.ListOpts{{}}).Return([]lapi.Node{nodeA, nodeB, nodeC}, nil).Maybe()
	m.EXPECT().GetAll(mock.Anything, []*lapi.ListOpts{{Prop: []string{"Aux/lun=yes"}}}).Return([]lapi.Node{nodeA, nodeB}, nil).Maybe()
	m.EXPECT().GetAll(mock.Anything, []*lapi.ListOpts{{Node: []string{"node-a"}}}).Return([]lapi.Node{nodeA}, nil).Maybe()
	m.EXPECT().GetAll(mock.Anything, []*lapi.ListOpts{{Node: []string{"node-c"}}}).Return([]lapi.Node{nodeC}, nil).Maybe()

	return &lc.HighLevelClient{Client: &lapi.Client{Nodes: &m}, PropertyNamespace: lapiconsts.NamespcAuxiliary}
}

func TestHighLevelClient_NodesForTopology(t *testing.T) {
	t.Parallel()

	c := lunNodes(t)

	testcases := []struct {
		name        string
		segments    map[string]string
		missingKeys []string
		expected    []string
	}{
		{
			name:     "node key",
			segments: map[string]string{topology.LinstorNodeKey: "node-unknown"},
			expected: []string{"node-unknown"},
		},
		{
			name:     "matching value",
			segments: map[string]string{"lun": "yes"},
			expected: []string{"node-a", "node-b"},
		},
		{
			name:        "missing key",
			missingKeys: []string{"lun"},
			expected:    []string{"node-c"},
		},
		{
			name:        "node key with missing key",
			segments:    map[string]string{topology.LinstorNodeKey: "node-c"},
			missingKeys: []string{"lun"},
			expected:    []string{"node-c"},
		},
		{
			name:        "node key that has the missing key",
			segments:    map[string]string{topology.LinstorNodeKey: "node-a"},
			missingKeys: []string{"lun"},
		},
	}

	for i := range testcases {
		testcase := &testcases[i]

		t.Run(testcase.name, func(t *testing.T) {
			t.Parallel()

			nodes, err := c.NodesForTopology(context.Background(), testcase.segments, testcase.missingKeys...)
			assert.NoError(t, err)
			assert.Equal(t, testcase.expected, nodes)
		})
	}
}

func TestHighLevelClient_GetAllTopologyNodes(t *testing.T) {
	t.Parallel()

	c := lunNodes(t)
	policy := volume.RemoteAccessPolicy{{FromSame: []string{"lun"}}}

	testcases := []struct {
		name       string
		requisites []*csi.Topology
		expected   []string
	}{
		{
			name:       "node with key",
			requisites: []*csi.Topology{{Segments: map[string]string{topology.LinstorNodeKey: "node-a", "lun": "yes"}}},
			expected:   []string{"node-a", "node-b"},
		},
		{
			name:       "node without key",
			requisites: []*csi.Topology{{Segments: map[string]string{topology.LinstorNodeKey: "node-c"}}},
			expected:   []string{"node-c"},
		},
		{
			name: "both",
			requisites: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node-a", "lun": "yes"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node-c"}},
			},
			expected: []string{"node-a", "node-b", "node-c"},
		},
	}

	for i := range testcases {
		testcase := &testcases[i]

		t.Run(testcase.name, func(t *testing.T) {
			t.Parallel()

			nodes, err := c.GetAllTopologyNodes(context.Background(), policy, testcase.requisites)
			assert.NoError(t, err)
			assert.Equal(t, testcase.expected, nodes)
		})
	}
}
