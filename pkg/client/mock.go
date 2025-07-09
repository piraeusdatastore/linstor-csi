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
	"sort"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

type MockStorage struct {
	createdVolumes  []*volume.Info
	assignedVolumes map[string][]volume.Assignment
	snapshots       []*volume.Snapshot
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		createdVolumes:  nil,
		assignedVolumes: make(map[string][]volume.Assignment),
		snapshots:       nil,
	}
}

func (s *MockStorage) ListAllWithStatus(ctx context.Context) ([]volume.VolumeStatus, error) {
	vols := make([]volume.VolumeStatus, 0)
	for _, info := range s.createdVolumes {
		vols = append(vols, volume.VolumeStatus{
			Info: *info,
		})
	}

	sort.Slice(vols, func(i, j int) bool {
		return vols[i].ID < vols[j].ID
	})

	return vols, nil
}

func (s *MockStorage) AllocationSizeKiB(requiredBytes, limitBytes int64, fsType string) (int64, error) {
	return requiredBytes / 1024, nil
}

func (s *MockStorage) FindByID(ctx context.Context, id string) (*volume.Info, error) {
	for _, vol := range s.createdVolumes {
		if vol.ID == id {
			return vol, nil
		}
	}
	return nil, nil
}

func (s *MockStorage) CompatibleVolumeId(name, pvcNamespace, pvcName string) string {
	return name
}

func (s *MockStorage) Create(ctx context.Context, vol *volume.Info, params *volume.Parameters, topologies *csi.TopologyRequirement) error {
	s.createdVolumes = append(s.createdVolumes, vol)
	return nil
}

func (s *MockStorage) Clone(ctx context.Context, vol, src *volume.Info, params *volume.Parameters, topologies *csi.TopologyRequirement) error {
	s.createdVolumes = append(s.createdVolumes, vol)
	return nil
}

func (s *MockStorage) Delete(ctx context.Context, volId string) error {
	for i, v := range s.createdVolumes {
		if v != nil && (v.ID == volId) {
			// csi-test counts numbers of snapshots, so we have to actually delete it.
			s.createdVolumes[i] = s.createdVolumes[len(s.createdVolumes)-1]
			s.createdVolumes[len(s.createdVolumes)-1] = nil
			s.createdVolumes = s.createdVolumes[:len(s.createdVolumes)-1]
		}
	}
	return nil
}

func (s *MockStorage) AccessibleTopologies(ctx context.Context, volId string, params *volume.Parameters) ([]*csi.Topology, error) {
	return nil, nil
}

func (s *MockStorage) GetLegacyVolumeParameters(ctx context.Context, volId string) (*volume.Parameters, error) {
	return nil, nil
}

func (s *MockStorage) CompatibleSnapshotId(name string) string {
	return name
}

func (s *MockStorage) SnapCreate(ctx context.Context, id, sourceVolId string, params *volume.SnapshotParameters) (*volume.Snapshot, error) {
	for _, snap := range s.snapshots {
		if snap.SnapshotId == id {
			return nil, fmt.Errorf("snapshot '%s' already exists", id)
		}
	}

	var size int64

	for i := range s.createdVolumes {
		if s.createdVolumes[i].ID == id {
			size = s.createdVolumes[i].SizeBytes
		}
	}

	// Fill in missing snapshot fields on creation, keep original SourceVolumeId.
	snap := &volume.Snapshot{
		Snapshot: csi.Snapshot{
			SnapshotId:     id,
			SourceVolumeId: sourceVolId,
			CreationTime:   timestamppb.Now(),
			SizeBytes:      size,
			ReadyToUse:     true,
		},
	}

	s.snapshots = append(s.snapshots, snap)

	return snap, nil
}

func (s *MockStorage) SnapDelete(ctx context.Context, snap *volume.Snapshot) error {
	for i, item := range s.snapshots {
		if item.GetSnapshotId() == snap.GetSnapshotId() {
			s.snapshots = append(s.snapshots[:i], s.snapshots[i+1:]...)
			break
		}
	}

	return nil
}

func (s *MockStorage) DeleteTemporarySnapshotID(ctx context.Context, id string, snapParams *volume.SnapshotParameters) error {
	return nil
}

func (s *MockStorage) ReconcileRemote(ctx context.Context, params *volume.SnapshotParameters) error {
	return nil
}

func (s *MockStorage) ListSnaps(ctx context.Context, start, limit int) ([]*volume.Snapshot, error) {
	if limit == 0 {
		limit = len(s.snapshots) - start
	}

	end := start + limit
	if end > len(s.snapshots) {
		end = len(s.snapshots)
	}

	return s.snapshots[start:end], nil
}

func (s *MockStorage) FindSnapByID(ctx context.Context, id string) (*volume.Snapshot, bool, error) {
	for _, snap := range s.snapshots {
		if snap.GetSnapshotId() == id {
			return snap, true, nil
		}
	}
	return nil, false, nil
}

func (s *MockStorage) FindSnapsBySource(ctx context.Context, sourceVol *volume.Info, start, limit int) ([]*volume.Snapshot, error) {
	var results []*volume.Snapshot

	for _, snap := range s.snapshots {
		if snap.GetSourceVolumeId() == sourceVol.ID {
			results = append(results, snap)
		}
	}

	if limit == 0 {
		limit = len(s.snapshots) - start
	}

	end := start + limit
	if end > len(results) {
		end = len(results)
	}
	return results[start:end], nil
}

func (s *MockStorage) VolFromSnap(ctx context.Context, snap *volume.Snapshot, vol *volume.Info, parameters *volume.Parameters, snapParams *volume.SnapshotParameters, topologies *csi.TopologyRequirement) error {
	s.createdVolumes = append(s.createdVolumes, vol)
	return nil
}

func (s *MockStorage) VolFromVol(ctx context.Context, sourceVol, vol *volume.Info) error {
	s.createdVolumes = append(s.createdVolumes, vol)
	return nil
}

func (s *MockStorage) Attach(ctx context.Context, volId, node string, rwxBlock bool) (string, error) {
	s.assignedVolumes[volId] = append(s.assignedVolumes[volId], volume.Assignment{Node: node, Path: "/dev/" + volId})
	return "/dev/" + volId, nil
}

func (s *MockStorage) Detach(ctx context.Context, volId, node string) error {
	for i, a := range s.assignedVolumes[volId] {
		if a.Node == node {
			s.assignedVolumes[volId] = append(s.assignedVolumes[volId][:i], s.assignedVolumes[volId][i+1:]...)
			break
		}
	}
	return nil
}

func (s *MockStorage) NodeAvailable(ctx context.Context, node string) error {
	// Hard coding magic string to pass csi-test.
	if strings.Contains(node, "fake-node-id") {
		return fmt.Errorf("it's obvious that %s is a fake node", node)
	}

	return nil
}

func (s *MockStorage) FindAssignmentOnNode(ctx context.Context, volId, node string) (*volume.Assignment, error) {
	for _, a := range s.assignedVolumes[volId] {
		if a.Node == node {
			return &a, nil
		}
	}
	return nil, nil
}

func (s *MockStorage) Status(ctx context.Context, volId string) ([]string, *csi.VolumeCondition, error) {
	nodes := make([]string, 0, len(s.assignedVolumes[volId]))
	for _, a := range s.assignedVolumes[volId] {
		nodes = append(nodes, a.Node)
	}

	return nodes, &csi.VolumeCondition{Abnormal: false, Message: "All replicas normal"}, nil
}

func (s *MockStorage) CapacityBytes(ctx context.Context, pools []string, overProvision *float64, segments map[string]string) (int64, error) {
	return 50000000, nil
}

func (s *MockStorage) Mount(ctx context.Context, source, target, fsType string, readonly bool, mntOpts []string) error {
	if _, err := os.Stat(target); os.IsNotExist(err) {
		return os.MkdirAll(target, 0755)
	}

	return nil
}

func (s *MockStorage) IsMountPoint(target string) (bool, error) {
	_, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (s *MockStorage) Unmount(target string) error {
	mounted, err := s.IsMountPoint(target)
	if err != nil {
		return err
	}

	if mounted {
		return os.RemoveAll(target)
	}

	return nil
}

func (s *MockStorage) GetVolumeStats(path string) (volume.VolumeStats, error) {
	return volume.VolumeStats{}, nil
}

func (s *MockStorage) NodeExpand(target string) error {
	_, err := os.Stat(target)
	if err != nil {
		return err
	}

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
