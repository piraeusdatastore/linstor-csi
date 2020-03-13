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

package util

import (
	apiconst "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// NodeIsAccessible returns true if the appropriate StoragePool is present for
// diskfull or diskless resources.
func NodeIsAccessible(sp lapi.StoragePool, p volume.Parameters) bool {
	return p.AllowRemoteVolumeAccess && matchDisklessPool(sp, p)
}

func matchDiskfullPool(sp lapi.StoragePool, p volume.Parameters) bool {
	return sp.StoragePoolName == p.StoragePool && sp.ProviderKind != lapi.DISKLESS
}

func matchDisklessPool(sp lapi.StoragePool, p volume.Parameters) bool {
	// The default diskless storage pool doesn't show up in the list and by
	// default all network attachable resources can use it.
	return p.DisklessStoragePool == volume.DefaultDisklessStoragePoolName ||
		// Otherwise, the user is using a particular diskless storage pool
		(sp.StoragePoolName == p.DisklessStoragePool && sp.ProviderKind == lapi.DISKLESS)
}

// DeployedDiskfullyNodes lists all nodes where a resource has volumes physically
// present.
func DeployedDiskfullyNodes(res []lapi.Resource) []string {
	var nodes = make([]string, 0)
	for _, r := range res {
		if DeployedDiskfully(r) {
			nodes = append(nodes, r.NodeName)
		}
	}
	return nodes
}

// DeployedDisklesslyNodes list all nodes where a resource has volumes that are
// attached over the network.
func DeployedDisklesslyNodes(res []lapi.Resource) []string {
	var nodes = make([]string, 0)
	for _, r := range res {
		if DeployedDiskfully(r) {
			nodes = append(nodes, r.NodeName)
		}
	}
	return nodes
}

// DeployedDiskfully returns true if the resource has volumes that are physically
// present and the resource state is healthy.
func DeployedDiskfully(res lapi.Resource) bool {
	return deployed(res) && healthy(res) &&
		doesNotcontainAny(res.Flags, apiconst.FlagDiskless)
}

func deployed(res lapi.Resource) bool {
	return res.Name != "" && res.NodeName != ""
}

// DeployedDisklessly returns true if the resource has volumes that are attached
// over the network and the resource state is healthy.
func DeployedDisklessly(res lapi.Resource) bool {
	return deployed(res) && healthy(res) &&
		containsAll(res.Flags, apiconst.FlagDiskless)
}

func healthy(res lapi.Resource) bool {
	return doesNotcontainAny(res.Flags, apiconst.FlagDelete, apiconst.FlagFailedDeployment, apiconst.FlagFailedDisconnect)
}

func containsAny(list []string, candidates ...string) bool {
	if len(candidates) == 0 {
		return false
	}

	for _, c := range candidates {
		for _, e := range list {
			if e == c {
				return true
			}
		}
	}
	return false
}

func doesNotcontainAny(list []string, candidates ...string) bool {
	return !containsAny(list, candidates...)
}

func containsAll(list []string, candidates ...string) bool {
	if len(candidates) == 0 {
		return false
	}

nextCandidate:
	for _, c := range candidates {
		for _, e := range list {
			if e == c {
				continue nextCandidate
			}
		}
		// Made it through all data with no match.
		return false
	}
	return true
}
