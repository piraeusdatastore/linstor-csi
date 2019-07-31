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
	"github.com/LINBIT/linstor-csi/pkg/linstor/util"
	"github.com/LINBIT/linstor-csi/pkg/topology"
	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
)

// HighLevelClient is a golinstor client with convience functions.
type HighLevelClient struct {
	*lapi.Client
}

// NewHighLevelClient returns a pointer to a golinstor client with convience.
func NewHighLevelClient(options ...func(*lapi.Client) error) (*HighLevelClient, error) {
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
	nodes := util.DeployedDiskfullyNodes(r)

	// Get all nodes where the resource could be attached to.
	pools, err := c.Nodes.GetStoragePoolView(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}
	for _, sp := range pools {
		if util.NodeIsAccessible(sp, params) {
			nodes = append(nodes, sp.NodeName)
		}
	}

	// Return the deduplicated list of accessible nodes.
	var topos = make([]*csi.Topology, 0)
	for _, n := range uniq(nodes) {
		topos = append(topos, &csi.Topology{Segments: map[string]string{topology.LinstorNodeKey: n}})
	}

	return topos, nil
}

// remove duplicates from a slice.
func uniq(strs []string) []string {
	var seen = make(map[string]bool, len(strs))
	var j int

	for _, s := range strs {
		if seen[s] {
			continue
		}
		seen[s] = true
		strs[j] = s
		j++
	}

	return strs[:j]
}
