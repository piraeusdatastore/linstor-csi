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

package topology

import "fmt"

//go:generate enumer -type=PlacementPolicy

// PlacementPolicy determines which scheduler will create volumes and report
// their accessible topology.
type PlacementPolicy int

const (
	// Unknown placement policy
	Unknown PlacementPolicy = iota
	// Manual place volumes with a list of nodes and clients.
	Manual
	// AutoPlace volumes using linstor's built in autoplace feature.
	AutoPlace
	// FollowTopology place volumes local to topology preferences, in order
	// of those preferences.
	FollowTopology
	// BalancedTopology places remote volumes in the same zone(Rack)
	// and pick Node, StoragePool, PrefNic based on utilization
	Balanced
)

const (
	// LinstorNodeKey refers to a node running the LINSTOR csi node service
	// and the linstor Satellite and is therefore capable of hosting LINSTOR volumes.
	LinstorNodeKey = "linbit.com/hostname"

	// LinstorStoragePoolKeyPrefix is the prefix used when specifying the available storage
	// pools on a node via CSI topology keys.
	LinstorStoragePoolKeyPrefix = "linbit.com"

	// The value assigned to the storage pool label, given that the node has access to the storage pool.
	LinstorStoragePoolValue = "true"
)

// Converts the storage pool name into a CSI Topology compatible label.
//
// There is an upper limit on the length of these keys (63 chars for prefix + 63 chars for the key) as per CSI Spec.
// LINSTOR enforces a stricter limit of 48 characters for storage pools, so this should not be an issue.
func ToStoragePoolLabel(storagePoolName string) string {
	// No additional checks since
	// a. storage pool names should always expand to valid label names.
	// b. invalid names are caught by the node-registrar sidecar in any case.
	return fmt.Sprintf("%s/sp-%s", LinstorStoragePoolKeyPrefix, storagePoolName)
}
