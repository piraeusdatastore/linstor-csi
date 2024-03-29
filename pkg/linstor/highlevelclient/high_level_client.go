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
	"strings"
	"sync"
	"time"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor/util"
	"github.com/piraeusdatastore/linstor-csi/pkg/slice"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// HighLevelClient is a golinstor client with convience functions.
type HighLevelClient struct {
	*lapi.Client
	PropertyNamespace string
}

// NewHighLevelClient returns a pointer to a golinstor client with convience.
func NewHighLevelClient(options ...lapi.Option) (*HighLevelClient, error) {
	c, err := lapi.NewClient(options...)
	if err != nil {
		return nil, err
	}

	c.Nodes = &NodeCacheProvider{
		NodeProvider: c.Nodes,
		timeout:      1 * time.Minute,
	}

	c.Resources = &ResourceCacheProvider{
		ResourceProvider: c.Resources,
		timeout:          1 * time.Minute,
	}

	return &HighLevelClient{Client: c}, nil
}

// GenericAccessibleTopologies returns topologies based on linstor storage pools
// and whether a resource is allowed to be accessed over the network.
func (c *HighLevelClient) GenericAccessibleTopologies(ctx context.Context, volId string, remoteAccessPolicy volume.RemoteAccessPolicy) ([]*csi.Topology, error) {
	// Get all nodes where the resource is physically located.
	r, err := c.Resources.GetAll(ctx, volId)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	// Volume is definitely accessible on the nodes it's deployed on.
	nodeNames := util.DeployedDiskfullyNodes(r)

	nodes, err := c.Nodes.GetAll(ctx, &lapi.ListOpts{Node: nodeNames})
	if err != nil {
		return nil, fmt.Errorf("unable to fetch diskful nodes: %w", err)
	}

	var topos []*csi.Topology

	for i := range nodes {
		segs := make(map[string]string)

		for k, v := range nodes[i].Props {
			if strings.HasPrefix(k, c.PropertyNamespace+"/") {
				segs[k[len(c.PropertyNamespace+"/"):]] = v
			}
		}

		for _, m := range remoteAccessPolicy.AccessibleSegments(segs) {
			if len(m) == 0 {
				// Empty segment -> access allowed from everywhere.
				// This is special cased, otherwise CSI chokes on an empty segment map.
				return nil, nil
			}

			topos = append(topos, &csi.Topology{Segments: m})
		}
	}

	return topos, nil
}

// GetAllTopologyNodes returns the list of nodes that satisfy the given topology requirements
func (c *HighLevelClient) GetAllTopologyNodes(ctx context.Context, remoteAccessPolicy volume.RemoteAccessPolicy, requisites []*csi.Topology) ([]string, error) {
	var accessibleSegments []map[string]string
	for _, req := range requisites {
		accessibleSegments = append(accessibleSegments, remoteAccessPolicy.AccessibleSegments(req.GetSegments())...)
	}

	accessibleSegments = volume.PrunePattern(accessibleSegments...)

	if len(accessibleSegments) == 0 {
		// No requisites means no restrictions, so just use the segment that will return all nodes
		accessibleSegments = []map[string]string{{}}
	}

	var allNodes []string

	for _, segment := range accessibleSegments {
		nodes, err := c.NodesForTopology(ctx, segment)
		if err != nil {
			return nil, err
		}

		allNodes = slice.AppendUnique(allNodes, nodes...)
	}

	return allNodes, nil
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
		opts.Prop = append(opts.Prop, fmt.Sprintf("%s/%s=%s", c.PropertyNamespace, k, v))
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

func (c *HighLevelClient) ReservedCapacity(ctx context.Context, node, pool string) (int64, error) {
	ress, err := c.Resources.GetResourceView(ctx, &lapi.ListOpts{
		Node:        []string{node},
		StoragePool: []string{pool},
	})
	if err != nil {
		return 0, err
	}

	var reserved int64

	for i := range ress {
		res := &ress[i]

		// can never be too careful with LINSTOR filtering
		if res.NodeName != node {
			continue
		}

		for j := range res.Volumes {
			vol := &res.Volumes[j]
			if vol.StoragePoolName != pool {
				continue
			}

			// Last layer is the storage layer
			if len(vol.LayerDataList) > 0 {
				storageVol, ok := vol.LayerDataList[len(vol.LayerDataList)-1].Data.(*lapi.StorageVolume)
				if ok {
					reserved += storageVol.UsableSizeKib
				}
			}
		}
	}

	return reserved, nil
}

type NodeCacheProvider struct {
	lapi.NodeProvider
	timeout      time.Duration
	nodesMu      sync.Mutex
	nodesUpdated time.Time
	nodes        []lapi.Node
	poolsMu      sync.Mutex
	poolsUpdated time.Time
	pools        []lapi.StoragePool
}

func (n *NodeCacheProvider) GetAll(ctx context.Context, opts ...*lapi.ListOpts) ([]lapi.Node, error) {
	n.nodesMu.Lock()
	defer n.nodesMu.Unlock()

	now := time.Now()

	if n.nodesUpdated.Add(n.timeout).After(now) {
		return filter(n.nodes, func(node lapi.Node) string {
			return node.Name
		}, nil, opts...), nil
	}

	nodes, err := n.NodeProvider.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	n.nodes = nodes
	n.nodesUpdated = now

	return filter(n.nodes, func(node lapi.Node) string {
		return node.Name
	}, nil, opts...), nil
}

func (n *NodeCacheProvider) GetStoragePoolView(ctx context.Context, opts ...*lapi.ListOpts) ([]lapi.StoragePool, error) {
	n.poolsMu.Lock()
	defer n.poolsMu.Unlock()

	now := time.Now()

	if n.poolsUpdated.Add(n.timeout).After(now) {
		return filter(n.pools,
			func(pool lapi.StoragePool) string { return pool.NodeName },
			func(pool lapi.StoragePool) string { return pool.StoragePoolName },
			opts...,
		), nil
	}

	pools, err := n.NodeProvider.GetStoragePoolView(ctx)
	if err != nil {
		return nil, err
	}

	n.pools = pools
	n.poolsUpdated = now

	return filter(n.pools,
		func(pool lapi.StoragePool) string { return pool.NodeName },
		func(pool lapi.StoragePool) string { return pool.StoragePoolName },
		opts...,
	), nil
}

type ResourceCacheProvider struct {
	lapi.ResourceProvider
	timeout             time.Duration
	resourceViewMu      sync.Mutex
	resourceViewUpdated time.Time
	resourceView        []lapi.ResourceWithVolumes
}

func (r *ResourceCacheProvider) GetResourceView(ctx context.Context, opts ...*lapi.ListOpts) ([]lapi.ResourceWithVolumes, error) {
	r.resourceViewMu.Lock()
	defer r.resourceViewMu.Unlock()

	now := time.Now()

	if r.resourceViewUpdated.Add(r.timeout).After(now) {
		return filter(r.resourceView,
			func(res lapi.ResourceWithVolumes) string { return res.NodeName },
			func(res lapi.ResourceWithVolumes) string { return res.Props[lc.KeyStorPoolName] },
			opts...,
		), nil
	}

	view, err := r.ResourceProvider.GetResourceView(ctx)
	if err != nil {
		return nil, err
	}

	r.resourceView = view
	r.resourceViewUpdated = now

	return filter(r.resourceView,
		func(res lapi.ResourceWithVolumes) string { return res.NodeName },
		func(res lapi.ResourceWithVolumes) string { return res.Props[lc.KeyStorPoolName] },
		opts...,
	), nil
}

func filter[T any](items []T, getNodeName, getPoolName func(T) string, opts ...*lapi.ListOpts) []T {
	filterNames := make(map[string]struct{})
	filterPools := make(map[string]struct{})

	for _, o := range opts {
		for _, n := range o.Node {
			filterNames[n] = struct{}{}
		}

		for _, sp := range o.StoragePool {
			filterPools[sp] = struct{}{}
		}
	}

	var result []T

	for _, item := range items {
		if len(filterNames) > 0 {
			if _, ok := filterNames[getNodeName(item)]; !ok {
				continue
			}
		}

		if len(filterPools) > 0 {
			if _, ok := filterPools[getPoolName(item)]; !ok {
				continue
			}
		}

		result = append(result, item)
	}

	return result
}
