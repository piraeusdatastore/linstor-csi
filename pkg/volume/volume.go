/*
CSI Driver for Linstor
Copyright © 2018 LINBIT USA, LLC

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

package volume

import (
	"context"
	"strings"

	lc "github.com/LINBIT/golinstor"
	"github.com/container-storage-interface/spec/lib/go/csi"
)

// Info provides the everything need to manipulate volumes.
type Info struct {
	ID            string
	SizeBytes     int64
	ResourceGroup string
	FsType        string
	Properties    map[string]string
	UseQuorum     bool
}

// Assignment represents a volume situated on a particular node.
type Assignment struct {
	// Node is the node that the assignment is valid for.
	Node string
	// Path is a location on the Node's filesystem where the volume may be accessed.
	Path string
}

type Snapshot struct {
	csi.Snapshot
	Remote string
}

// CreateDeleter handles the creation and deletion of volumes.
type CreateDeleter interface {
	Querier
	CompatibleVolumeId(name, pvcNamespace, pvcName string) string
	Create(ctx context.Context, vol *Info, params *Parameters, topologies *csi.TopologyRequirement) error
	Clone(ctx context.Context, vol, src *Info, params *Parameters, topologies *csi.TopologyRequirement) error
	Delete(ctx context.Context, volId string) error

	// AccessibleTopologies returns the list of key value pairs volume topologies
	// for the volume or nil if not applicable.
	AccessibleTopologies(ctx context.Context, volId string, params *Parameters) ([]*csi.Topology, error)

	// GetLegacyVolumeContext tries to fetch the volume context from legacy properties.
	GetLegacyVolumeParameters(ctx context.Context, volId string) (*Parameters, error)
}

// SnapshotCreateDeleter handles the creation and deletion of snapshots.
type SnapshotCreateDeleter interface {
	// CompatibleSnapshotId returns an ID unique to the suggested name
	CompatibleSnapshotId(name string) string
	SnapCreate(ctx context.Context, id string, sourceVolId string, params *SnapshotParameters) (*Snapshot, error)
	SnapDelete(ctx context.Context, snap *Snapshot) error
	// FindSnapByID searches the snapshot in the backend
	// It returns:
	// * the snapshot, nil if not found
	// * true, if the snapshot is either in progress or successful
	// * any error encountered
	FindSnapByID(ctx context.Context, id string) (*Snapshot, bool, error)
	FindSnapsBySource(ctx context.Context, sourceVol *Info, start, limit int) ([]*Snapshot, error)
	// List Snapshots should return a sorted list of snapshots.
	ListSnaps(ctx context.Context, start, limit int) ([]*Snapshot, error)
	// VolFromSnap creates a new volume based on the provided snapshot.
	VolFromSnap(ctx context.Context, snap *Snapshot, vol *Info, params *Parameters, snapParams *SnapshotParameters, topologies *csi.TopologyRequirement) error
	// DeleteTemporarySnapshotID deletes the temporary snapshot ID.
	DeleteTemporarySnapshotID(ctx context.Context, id string, snapParams *SnapshotParameters) error
	// ReconcileRemote creates or updates a remote based on the given snapshot parameters.
	ReconcileRemote(ctx context.Context, params *SnapshotParameters) error
}

// AttacherDettacher handles operations relating to volume accessiblity on nodes.
type AttacherDettacher interface {
	Querier
	Attach(ctx context.Context, volId, node string, rwxBlock bool) (string, error)
	Detach(ctx context.Context, volId, node string) error
	NodeAvailable(ctx context.Context, node string) error
	FindAssignmentOnNode(ctx context.Context, volId, node string) (*Assignment, error)
	// Status returns the currently deployed nodes and condition of the given volume
	Status(ctx context.Context, volId string) ([]string, *csi.VolumeCondition, error)
}

// Querier retrives various states of volumes.
type Querier interface {
	// ListAllWithStatus returns a sorted list of volume and their status.
	ListAllWithStatus(ctx context.Context) ([]VolumeStatus, error)
	// FindByID returns nil when volume is not found.
	FindByID(ctx context.Context, ID string) (*Info, error)
	// AllocationSizeKiB returns the number of KiB required to provision required bytes.
	AllocationSizeKiB(requiredBytes, limitBytes int64, fsType string) (int64, error)
	// CapacityBytes returns the amount of free space, in bytes, in the storage pool specified by the params and topology.
	CapacityBytes(ctx context.Context, pools []string, overProvision *float64, segments map[string]string) (int64, error)
}

// Mounter handles the filesystems located on volumes.
type Mounter interface {
	Mount(ctx context.Context, source, target, fsType string, readonly bool, mntOpts []string) error
	Unmount(target string) error
	IsMountPoint(target string) (bool, error)
}

// VolumeStats provides details about filesystem usage.
type VolumeStats struct {
	AvailableBytes  int64
	TotalBytes      int64
	UsedBytes       int64
	AvailableInodes int64
	TotalInodes     int64
	UsedInodes      int64
}

type VolumeStatus struct {
	Info
	Nodes      []string
	Conditions *csi.VolumeCondition
}

// VolumeStatter provides info about volume/filesystem usage.
type VolumeStatter interface {
	// GetVolumeStats determines filesystem usage.
	GetVolumeStats(path string) (VolumeStats, error)
}

type NodeInformer interface {
	GetNodeTopologies(ctx context.Context, nodename string) (*csi.Topology, error)
}

// Expander handles the resizing operations for volumes.
type Expander interface {
	// NodeExpand runs the appropriate resize operation on the target path.
	// Must return os.ErrNotExist if the path does not exist or is not a valid mount point.
	NodeExpand(target string) error
	ControllerExpand(ctx context.Context, vol *Info) error
}

// Add the given prefix to the property name.
// If the property is already prefixed (with "Aux/"), no modification is made.
func maybeAddTopologyPrefix(prefix string, props ...string) []string {
	const auxPrefix = lc.NamespcAuxiliary + "/"

	result := make([]string, len(props))
	for i, prop := range props {
		if strings.HasPrefix(prop, auxPrefix) {
			result[i] = prop
		} else {
			result[i] = prefix + "/" + prop
		}
	}

	return result
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
