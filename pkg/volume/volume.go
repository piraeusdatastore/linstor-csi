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
	"sort"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// Info provides the everything need to manipulate volumes.
type Info struct {
	Name         string            `json:"name"`
	ID           string            `json:"id"`
	CreatedBy    string            `json:"createdBy"`
	CreationTime time.Time         `json:"creationTime"`
	SizeBytes    int64             `json:"sizeBytes"`
	Readonly     bool              `json:"readonly"`
	Parameters   map[string]string `json:"parameters"`
	Snapshots    []*SnapInfo       `json:"snapshots"`
}

// Sort sorts a list of snaphosts.
func Sort(vols []*Info) {
	sort.Slice(vols, func(j, k int) bool {
		return vols[j].CreationTime.Before(vols[k].CreationTime)
	})
}

// SnapInfo provides everything needed to manipulate snapshots.
type SnapInfo struct {
	Name    string        `json:"name"`
	CsiSnap *csi.Snapshot `json:"csiSnapshot"`
}

// SnapSort sorts a list of snaphosts.
func SnapSort(snaps []*SnapInfo) {
	sort.Slice(snaps, func(j, k int) bool {
		if snaps[j].CsiSnap.CreationTime.Seconds == snaps[k].CsiSnap.CreationTime.Seconds {
			return snaps[j].CsiSnap.CreationTime.Nanos < snaps[k].CsiSnap.CreationTime.Nanos
		}
		return snaps[j].CsiSnap.CreationTime.Seconds < snaps[k].CsiSnap.CreationTime.Seconds
	})
}

type Assignment struct {
	Vol  *Info
	Node string
	Path string
}

// CreateDeleter handles the creation and deletion of volumes.
type CreateDeleter interface {
	Querier
	Create(ctx context.Context, vol *Info, req *csi.CreateVolumeRequest) error
	Delete(ctx context.Context, vol *Info) error

	// AccessibleTopologies returns the list of key value pairs volume topologies
	// for the volume or nil if not applicable.
	AccessibleTopologies(ctx context.Context, vol *Info) ([]*csi.Topology, error)
}

// SnapshotCreateDeleter handles the creation and deletion of snapshots.
type SnapshotCreateDeleter interface {
	SnapCreate(ctx context.Context, snap *SnapInfo) (*SnapInfo, error)
	SnapDelete(ctx context.Context, snap *SnapInfo) error
	GetSnapByName(ctx context.Context, name string) (*SnapInfo, error)
	GetSnapByID(ctx context.Context, ID string) (*SnapInfo, error)
	// List Snapshots should return a sorted list of snapshots.
	ListSnaps(ctx context.Context) ([]*SnapInfo, error)
	// CanonicalizeSnapshotName tries to return a relatively similar version
	// of the suggestedName if the storage backend cannot use the suggestedName
	// in its original form.
	CanonicalizeSnapshotName(ctx context.Context, suggestedName string) string
	// VolFromSnap creats a new volume based on the provided snapshot.
	VolFromSnap(ctx context.Context, snap *SnapInfo, vol *Info) error
	// VolFromVol creats a new volume based on the provided volume.
	VolFromVol(ctx context.Context, sourceVol, vol *Info) error
}

type AttacherDettacher interface {
	Querier
	Attach(ctx context.Context, vol *Info, node string) error
	Detach(ctx context.Context, vol *Info, node string) error
	NodeAvailable(ctx context.Context, node string) error
	GetAssignmentOnNode(ctx context.Context, vol *Info, node string) (*Assignment, error)
}

// Querier retrives various states of volumes.
type Querier interface {
	// ListAll should return a sorted list of pointers to Info.
	ListAll(ctx context.Context) ([]*Info, error)
	GetByName(ctx context.Context, name string) (*Info, error)
	//GetByID should return nil when volume is not found.
	GetByID(ctx context.Context, ID string) (*Info, error)
	// AllocationSizeKiB returns the number of KiB required to provision required bytes.
	AllocationSizeKiB(requiredBytes, limitBytes int64) (int64, error)
	// CapacityBytes determines the capacity of the underlying storage in Bytes.
	CapacityBytes(ctx context.Context, params map[string]string) (int64, error)
}

type Mounter interface {
	Mount(vol *Info, source, target, fsType string, options []string) error
	Unmount(target string) error
}
