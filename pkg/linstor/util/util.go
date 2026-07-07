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
	"iter"

	apiconst "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
)

// DeployedDiskfullyNodes lists all nodes where a resource has volumes physically
// present.
func DeployedDiskfullyNodes(res []lapi.Resource) []string {
	nodes := make([]string, 0)
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

func GetDrbdLayer(layer *lapi.ResourceLayer) *lapi.DrbdResource {
	if layer.Type == devicelayerkind.Drbd {
		return layer.Drbd
	}

	for i := range layer.Children {
		drbd := GetDrbdLayer(&layer.Children[i])
		if drbd != nil {
			return drbd
		}
	}

	return nil
}

// ConsistencyGroupVolumes yields all the volume numbers and names of CSI enabled consistency group volumes.
func ConsistencyGroupVolumes(vds ...lapi.VolumeDefinition) iter.Seq2[int, string] {
	return func(yield func(int, string) bool) {
		for i := range vds {
			if vds[i].VolumeNumber != nil && vds[i].Props[linstor.PropertyCSIVolumeName] != "" {
				if !yield(int(*vds[i].VolumeNumber), vds[i].Props[linstor.PropertyCSIVolumeName]) {
					return
				}
			}
		}
	}
}

// ConsistencyGroupVolumeNumberFor yields all the volume numbers matching the volume name in the consistency group volume.
func ConsistencyGroupVolumeNumberFor(volumeName string, vds ...lapi.VolumeDefinition) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range vds {
			if vds[i].VolumeNumber != nil && vds[i].Props[linstor.PropertyCSIVolumeName] == volumeName {
				if !yield(int(*vds[i].VolumeNumber)) {
					return
				}
			}
		}
	}
}
