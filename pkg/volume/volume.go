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
	Create(vol *Info) error
	Delete(vol *Info) error

	// CanonicalizeVolumeName tries to return a relatively similar version
	// of the suggestedName if the storage backend cannot use the suggestedName
	// in its original form.
	CanonicalizeVolumeName(suggestedName string) string
}

// SnapshotCreateDeleter handles the creation and deletion of snapshots.
type SnapshotCreateDeleter interface {
	SnapCreate(snap *SnapInfo) (*SnapInfo, error)
	SnapDelete(snap *SnapInfo) error
	GetSnapByName(name string) (*SnapInfo, error)
	GetSnapByID(ID string) (*SnapInfo, error)
	// List Snapshots should return a sorted list of snapshots.
	ListSnaps() ([]*SnapInfo, error)
	// CanonicalizeSnapshotName tries to return a relatively similar version
	// of the suggestedName if the storage backend cannot use the suggestedName
	// in its original form.
	CanonicalizeSnapshotName(suggestedName string) string
	// VolFromSnap creats a new volume based on the provided snapshot.
	VolFromSnap(snap *SnapInfo, vol *Info) error
	// VolFromVol creats a new volume based on the provided volume.
	VolFromVol(sourceVol, vol *Info) error
}

type AttacherDettacher interface {
	Querier
	Attach(vol *Info, node string) error
	Detach(vol *Info, node string) error
	NodeAvailable(node string) (bool, error)
	GetAssignmentOnNode(vol *Info, node string) (*Assignment, error)
}

// Querier retrives various states of volumes.
type Querier interface {
	ListAll(parameters map[string]string) ([]*Info, error)
	GetByName(name string) (*Info, error)
	//GetByID should return nil when volume is not found.
	GetByID(ID string) (*Info, error)
	// AllocationSizeKiB returns the number of KiB required to provision required bytes.
	AllocationSizeKiB(requiredBytes, limitBytes int64) (int64, error)
}

type Mounter interface {
	Mount(vol *Info, source, target, fsType string, options []string) error
	Unmount(target string) error
}
