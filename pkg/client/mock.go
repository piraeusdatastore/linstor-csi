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
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	ptypes "github.com/golang/protobuf/ptypes"
	"github.com/pborman/uuid"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

type MockStorage struct {
	createdVolumes  []*volume.Info
	assignedVolumes []*volume.Assignment
}

func (s *MockStorage) ListAll(ctx context.Context) ([]*volume.Info, error) {
	var vols = make([]*volume.Info, 0)
	vols = append(vols, s.createdVolumes...)
	volume.Sort(vols)

	return vols, nil
}

func (s *MockStorage) AllocationSizeKiB(requiredBytes, limitBytes int64) (int64, error) {
	return requiredBytes / 1024, nil
}

func (s *MockStorage) FindByName(ctx context.Context, name string) (*volume.Info, error) {
	for _, vol := range s.createdVolumes {
		if vol.Name == name {
			return vol, nil
		}
	}
	return nil, nil
}

func (s *MockStorage) FindByID(ctx context.Context, id string) (*volume.Info, error) {
	for _, vol := range s.createdVolumes {
		if vol.ID == id {
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
		SnapshotId:     uuid.New(),
		SourceVolumeId: snap.CsiSnap.SourceVolumeId,
		CreationTime:   ptypes.TimestampNow(),
		ReadyToUse:     true,
	}

	vol, _ := s.FindByID(ctx, snap.CsiSnap.SourceVolumeId)
	vol.Snapshots = append(vol.Snapshots, snap)

	return snap, nil
}

func (s *MockStorage) SnapDelete(ctx context.Context, snap *volume.SnapInfo) error {
	for _, vol := range s.createdVolumes {
		for i, ss := range vol.Snapshots {
			if snap.CsiSnap.SnapshotId == ss.CsiSnap.SnapshotId {
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
	var snaps = make([]*volume.SnapInfo, 0)
	for _, vol := range s.createdVolumes {
		snaps = append(snaps, vol.Snapshots...)
	}
	volume.SnapSort(snaps)

	return snaps, nil
}

func (s *MockStorage) GetSnapByID(ctx context.Context, id string) (*volume.SnapInfo, error) {
	for _, vol := range s.createdVolumes {
		for _, ss := range vol.Snapshots {
			if ss.CsiSnap.SnapshotId == id {
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

func mockMountPath(target string) string {
	return filepath.Join(os.TempDir(), filepath.Base(target), "_mounted")
}

func (s *MockStorage) Mount(vol *volume.Info, source, target, fsType string, readonly bool, options []string) error {
	p := mockMountPath(target)
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return os.MkdirAll(p, 0755)
	}

	return nil
}

func (s *MockStorage) IsNotMountPoint(target string) (bool, error) {
	_, err := os.Stat(mockMountPath(target))
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (s *MockStorage) Unmount(target string) error {
	notMounted, err := s.IsNotMountPoint(target)
	if err != nil {
		return err
	}
	if notMounted {
		return nil
	}

	// currently unused
	// p := mockMountPath(target)
	// return os.RemoveAll(p)
	return nil
}

func (s *MockStorage) GetVolumeStats(path string) (volume.VolumeStats, error) {
	return volume.VolumeStats{}, nil
}

func (s *MockStorage) NodeExpand(source, target string) error {
	return nil
}

func (s *MockStorage) ControllerExpand(ctx context.Context, vol *volume.Info) error {
	return nil
}

func (s *MockStorage) GetNodeTopologies(_ context.Context, node string) (*csi.Topology, error) {
	return &csi.Topology{
		Segments: map[string]string{
			"mock.example.com/node": node,
		},
	}, nil
}
