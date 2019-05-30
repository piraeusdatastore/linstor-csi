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

package client

import (
	"context"
	"fmt"
	"time"

	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/pborman/uuid"
)

type MockStorage struct {
	createdVolumes  []*volume.Info
	assignedVolumes []*volume.Assignment
}

func (s *MockStorage) ListAll(ctx context.Context, page, perPage int) ([]*volume.Info, error) {
	return s.createdVolumes, nil
}

func (s *MockStorage) AllocationSizeKiB(requiredBytes, limitBytes int64) (int64, error) {
	return requiredBytes / 1024, nil
}

func (s *MockStorage) GetByName(ctx context.Context, name string) (*volume.Info, error) {
	for _, vol := range s.createdVolumes {
		if vol.Name == name {
			return vol, nil
		}
	}
	return nil, nil
}

func (s *MockStorage) GetByID(ctx context.Context, ID string) (*volume.Info, error) {
	for _, vol := range s.createdVolumes {
		if vol.ID == ID {
			return vol, nil
		}
	}
	return nil, nil
}

func (s *MockStorage) Create(ctx context.Context, vol *volume.Info, req *csi.CreateVolumeRequest) error {
	vol.ID = vol.Name
	s.createdVolumes = append(s.createdVolumes, vol)
	return nil
}

func (s *MockStorage) Delete(ctx context.Context, vol *volume.Info) error {
	for i, v := range s.createdVolumes {
		if v != nil && (v.Name == vol.Name) {
			// csi-test counts numbers of snapshots, so we have to actually delete it.
			s.createdVolumes[i] = s.createdVolumes[len(s.createdVolumes)-1]
			s.createdVolumes[len(s.createdVolumes)-1] = nil
			s.createdVolumes = s.createdVolumes[:len(s.createdVolumes)-1]
		}
	}
	return nil
}

func (s *MockStorage) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	return nil, nil
}

func (s *MockStorage) CanonicalizeSnapshotName(ctx context.Context, suggestedName string) string {
	return suggestedName
}

func (s *MockStorage) SnapCreate(ctx context.Context, snap *volume.SnapInfo) (*volume.SnapInfo, error) {
	// Fill in missing snapshot fields on creation, keep original SourceVolumeId.
	snap.CsiSnap = &csi.Snapshot{
		Id:             uuid.New(),
		SourceVolumeId: snap.CsiSnap.SourceVolumeId,
		CreatedAt:      time.Now().UnixNano(),
		Status: &csi.SnapshotStatus{
			Type: csi.SnapshotStatus_READY,
		},
	}

	vol, _ := s.GetByID(ctx, snap.CsiSnap.SourceVolumeId)
	vol.Snapshots = append(vol.Snapshots, snap)

	return snap, nil
}

func (s *MockStorage) SnapDelete(ctx context.Context, snap *volume.SnapInfo) error {
	for _, vol := range s.createdVolumes {
		for i, ss := range vol.Snapshots {
			if snap.CsiSnap.Id == ss.CsiSnap.Id {
				// csi-test counts numbers of snapshots, so we have to actually delete it.
				vol.Snapshots[i] = vol.Snapshots[len(vol.Snapshots)-1]
				vol.Snapshots[len(vol.Snapshots)-1] = nil
				vol.Snapshots = vol.Snapshots[:len(vol.Snapshots)-1]
				return nil
			}
		}
	}
	return nil
}

func (s *MockStorage) GetSnapByName(ctx context.Context, name string) (*volume.SnapInfo, error) {
	for _, vol := range s.createdVolumes {
		for _, ss := range vol.Snapshots {
			if ss.Name == name {
				return ss, nil
			}
		}
	}
	return nil, nil
}

func (s *MockStorage) ListSnaps(ctx context.Context) ([]*volume.SnapInfo, error) {
	snaps := make([]*volume.SnapInfo, 0)
	for _, vol := range s.createdVolumes {
		for _, ss := range vol.Snapshots {
			snaps = append(snaps, ss)
		}
	}
	volume.SnapSort(snaps)
	return snaps, nil
}

func (s *MockStorage) GetSnapByID(ctx context.Context, ID string) (*volume.SnapInfo, error) {
	for _, vol := range s.createdVolumes {
		for _, ss := range vol.Snapshots {
			if ss.CsiSnap.Id == ID {
				return ss, nil
			}
		}
	}
	return nil, nil
}

func (s *MockStorage) VolFromSnap(ctx context.Context, snap *volume.SnapInfo, vol *volume.Info) error {
	vol.ID = vol.Name
	s.createdVolumes = append(s.createdVolumes, vol)
	return nil
}

func (s *MockStorage) VolFromVol(ctx context.Context, sourceVol, vol *volume.Info) error {
	vol.ID = vol.Name
	s.createdVolumes = append(s.createdVolumes, vol)
	return nil
}

func (s *MockStorage) Attach(ctx context.Context, vol *volume.Info, node string) error {
	s.assignedVolumes = append(s.assignedVolumes, &volume.Assignment{Vol: vol, Node: node, Path: "/dev/null"})
	return nil
}

func (s *MockStorage) Detach(ctx context.Context, vol *volume.Info, node string) error {
	for _, a := range s.assignedVolumes {
		if a.Vol.Name == vol.Name && a.Node == node {
			// Close enough for testing...
			a = nil
		}
	}
	return nil
}

func (s *MockStorage) NodeAvailable(ctx context.Context, node string) error {
	// Hard coding magic string to pass csi-test.
	if node == "some-fake-node-id" {
		return fmt.Errorf("it's obvious that %s is a fake node", node)
	}

	return nil
}

func (s *MockStorage) GetAssignmentOnNode(ctx context.Context, vol *volume.Info, node string) (*volume.Assignment, error) {
	for _, a := range s.assignedVolumes {
		if a.Vol.ID == vol.ID && a.Node == node {
			// Close enough for testing...
			return a, nil
		}
	}
	return nil, nil
}

func (s *MockStorage) CapacityBytes(ctx context.Context, params map[string]string) (int64, error) {
	return 50000000, nil
}

func (s *MockStorage) Mount(vol *volume.Info, source, target, fsType string, options []string) error {
	return nil
}
func (s *MockStorage) Unmount(target string) error {
	return nil
}
