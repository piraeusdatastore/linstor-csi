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

package highlevelclient

import (
	"context"
	"fmt"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor/util"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// HighLevelClient is a golinstor client with convience functions.
type HighLevelClient struct {
	*lapi.Client
}

// NewHighLevelClient returns a pointer to a golinstor client with convience.
func NewHighLevelClient(options ...lapi.Option) (*HighLevelClient, error) {
	c, err := lapi.NewClient(options...)
	if err != nil {
		return nil, err
	}
	return &HighLevelClient{c}, nil
}

// GenericAccessibleTopologies returns topologies based on linstor storage pools
// and whether a resource is allowed to be accessed over the network.
func (c *HighLevelClient) GenericAccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	// Get all nodes where the resource is physically located.
	r, err := c.Resources.GetAll(ctx, vol.ID)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	// Volume is definitely accessible on the nodes it's deployed on.
	nodes := util.DeployedDiskfullyNodes(r)
	topos := make([]*csi.Topology, 0)
	for _, node := range nodes {
		topos = append(topos, &csi.Topology{Segments: map[string]string{topology.LinstorNodeKey: node}})
	}

	// If remote access is allowed, give access to all nodes with diskless storage pool.
	if params.AllowRemoteVolumeAccess {
		disklessPoolLabel := topology.ToStoragePoolLabel(params.DisklessStoragePool)
		topos = append(topos, &csi.Topology{Segments: map[string]string{disklessPoolLabel: topology.LinstorStoragePoolValue}})
	}

	return topos, nil
}

// NodesForTopology finds all matching nodes for the given topology segment.
//
// In the most common case, this just extracts the node name using the standard topology.LinstorNodeKey.
// In some cases CSI only gives us an "aggregate" topology, i.e. no node name, just some common property,
// in which case we query the LINSTOR API for all matching nodes.
func (c *HighLevelClient) NodesForTopology(ctx context.Context, segments map[string]string) ([]string, error) {
	// First, check if the segment already contains explicit node information. This is the common case,
	// no reason to make extra http requests for this.
	node, ok := segments[topology.LinstorNodeKey]
	if ok {
		return []string{node}, nil
	}

	opts := &lapi.ListOpts{}

	for k, v := range segments {
		opts.Prop = append(opts.Prop, fmt.Sprintf("Aux/%s=%s", k, v))
	}

	nodes, err := c.Nodes.GetAll(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes from segements %v: %w", segments, err)
	}

	result := make([]string, len(nodes))

	for i := range nodes {
		result[i] = nodes[i].Name
	}

	return result, nil
}
