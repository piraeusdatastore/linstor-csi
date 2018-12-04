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

package driver

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pborman/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Driver fullfils CSI controller, node, and indentity server interfaces.
type Driver struct {
	endpoint           string
	nodeID             string
	srv                *grpc.Server
	log                *log.Logger
	storage            volume.CreateDeleter
	assignments        volume.AttacherDettacher
	mount              volume.Mount
	defaultControllers string
	defaultStoragePool string
}

// Config provides sensible defaults for creating Drivers
type Config struct {
	Endpoint           string
	Node               string
	DefaultControllers string
	DefaultStoragePool string
	LogOut             io.Writer
	Storage            volume.CreateDeleter
	Assignments        volume.AttacherDettacher
	Mount              volume.Mount
}

// NewDriver build up a driver.
func NewDriver(cfg Config) (*Driver, error) {
	d := Driver{endpoint: cfg.Endpoint}

	if cfg.LogOut == nil {
		cfg.LogOut = ioutil.Discard
	}
	// TODO: Need to validate this.
	d.nodeID = cfg.Node
	d.log = log.New(cfg.LogOut, "linstor-csi: ", log.Ldate|log.Ltime|log.Lshortfile)

	d.storage = cfg.Storage
	d.assignments = cfg.Assignments
	d.mount = cfg.Mount

	return &d, nil
}

// GetPluginInfo returns driver info
func (d Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "io.drbd.com.linstor-csi",
		VendorVersion: "0.1.0",
	}, nil
}

// GetPluginCapabilities returns available capabilities of the plugin
func (d Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

// Probe returns the health and readiness of the plugin.
func (d Driver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// NodeStageVolume is called by the CO prior to the volume being consumed
// by any workloads on the node by NodePublishVolume. The Plugin SHALL
// assume that this RPC will be executed on the node where the volume will
// be used. This RPC SHOULD be called by the CO when a workload that wants
// to use the specified volume is placed (scheduled) on the specified node
// for the first time or for the first time since a NodeUnstageVolume call
// for the specified volume was called and returned success on that node.
// If the corresponding Controller Plugin has PUBLISH_UNPUBLISH_VOLUME
// controller capability and the Node Plugin has STAGE_UNSTAGE_VOLUME
// capability, then the CO MUST guarantee that this RPC is called after
// ControllerPublishVolume is called for the given volume on the given node
// and returns a success. The CO MUST guarantee that this RPC is called and
// returns a success before any NodePublishVolume is called for the given
// volume on the given node.
// This operation MUST be idempotent. If the volume corresponding to the
// volume_id is already staged to the staging_target_path, and is identical
// to the specified volume_capability the Plugin MUST reply 0 OK.
// If this RPC failed, or the CO does not know if it failed or not, it MAY
// choose to call NodeStageVolume again, or choose to call NodeUnstageVolume.
func (d Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.VolumeId == "" {
		return &csi.NodeStageVolumeResponse{}, missingAttr(
			"NodeStageVolume", req.VolumeId, "VolumeId")
	}

	if req.StagingTargetPath == "" {
		return &csi.NodeStageVolumeResponse{}, missingAttr(
			"NodeStageVolume", req.VolumeId, "StagingTargetPath")
	}

	if req.VolumeCapability == nil {
		return &csi.NodeStageVolumeResponse{}, missingAttr(
			"NodeStageVolume", req.VolumeId, "VolumeCapability slice")
	}

	mnt := req.VolumeCapability.GetMount()
	mntOpts := mnt.MountFlags

	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	existingVolume, err := d.storage.GetByID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	assignedTo, err := d.assignments.GetAssignmentOnNode(existingVolume, d.nodeID)
	if err != nil {
		return nil, err
	}

	err = d.mount.Mount(existingVolume, assignedTo.Path, req.StagingTargetPath, fsType, mntOpts)
	if err != nil {
		return nil, err
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume is a reverse operation of NodeStageVolume. This RPC MUST undo
// the work by the corresponding NodeStageVolume. This RPC SHALL be called
// by the CO once for each staging_target_path that was successfully setup
// via NodeStageVolume.
// If the corresponding Controller Plugin has PUBLISH_UNPUBLISH_VOLUME
// controller capability and the Node Plugin has STAGE_UNSTAGE_VOLUME
// capability, the CO MUST guarantee that this RPC is called and returns
// success before calling ControllerUnpublishVolume for the given node and
// the given volume. The CO MUST guarantee that this RPC is called after
// all NodeUnpublishVolume have been called and returned success for the
// given volume on the given node.
// The Plugin SHALL assume that this RPC will be executed on the node where
// the volume is being used.
// This RPC MAY be called by the CO when the workload using the volume is
// being moved to a different node, or all the workloads using the volume
// on a node have finished.
// This operation MUST be idempotent. If the volume corresponding to the
// volume_id is not staged to the staging_target_path, the Plugin MUST
// reply 0 OK.
// If this RPC failed, or the CO does not know if it failed or not, it MAY
// choose to call NodeUnstageVolume again.
func (d Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.VolumeId == "" {
		return &csi.NodeUnstageVolumeResponse{}, missingAttr("NodeUnstageVolume", req.VolumeId, "VolumeId")
	}
	if req.StagingTargetPath == "" {
		return &csi.NodeUnstageVolumeResponse{}, missingAttr("NodeUnstageVolume", req.VolumeId, "StagingTargetPath")
	}

	err := d.mount.Unmount(req.StagingTargetPath)
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume is called by the CO when a workload that wants to use
// the specified volume is placed (scheduled) on a node. The Plugin SHALL
// assume that this RPC will be executed on the node where the volume
// will be used.
// If the corresponding Controller Plugin has PUBLISH_UNPUBLISH_VOLUME
// controller capability, the CO MUST guarantee that this RPC is called
// after ControllerPublishVolume is called for the given volume on the
// given node and returns a success.
// This operation MUST be idempotent. If the volume corresponding to the
// volume_id has already been published at the specified target_path, and
// is compatible with the specified volume_capability and readonly flag,
// the Plugin MUST reply 0 OK.
// If this RPC failed, or the CO does not know if it failed or not, it MAY
// choose to call NodePublishVolume again, or choose to call
// NodeUnpublishVolume.
// This RPC MAY be called by the CO multiple times on the same node for the
// same volume with possibly different target_path and/or other arguments
// if the volume has MULTI_NODE capability (i.e., access_mode is either
// MULTI_NODE_READER_ONLY, MULTI_NODE_SINGLE_WRITER or MULTI_NODE_MULTI_WRITER).
func (d Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return &csi.NodePublishVolumeResponse{}, missingAttr("NodePublishVolume", req.VolumeId, "VolumeId")
	}

	if req.StagingTargetPath == "" {
		return &csi.NodePublishVolumeResponse{}, missingAttr("NodePublishVolume", req.VolumeId, "StagingTargetPath")
	}

	if req.TargetPath == "" {
		return &csi.NodePublishVolumeResponse{}, missingAttr("NodePublishVolume", req.VolumeId, "TargetPath")
	}

	if req.VolumeCapability == nil {
		return &csi.NodePublishVolumeResponse{}, missingAttr("NodePublishVolume", req.VolumeId, "VolumeCapability slice")
	}

	mnt := req.VolumeCapability.GetMount()
	mntOpts := mnt.MountFlags
	mntOpts = append(mntOpts, "bind")
	if req.Readonly {
		mntOpts = append(mntOpts, "ro")
	}
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	existingVolume, err := d.storage.GetByID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	err = d.mount.Mount(existingVolume, req.StagingTargetPath, req.TargetPath, fsType, mntOpts)
	if err != nil {
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume is a reverse operation of NodePublishVolume. This
// RPC MUST undo the work by the corresponding NodePublishVolume. This RPC
// SHALL be called by the CO at least once for each target_path that was
// successfully setup via NodePublishVolume. If the corresponding
// Controller Plugin has PUBLISH_UNPUBLISH_VOLUME controller capability,
// the CO SHOULD issue all NodeUnpublishVolume (as specified above) before
// calling ControllerUnpublishVolume for the given node and the given
// volume. The Plugin SHALL assume that this RPC will be executed on the
// node where the volume is being used.
// This RPC is typically called by the CO when the workload using the
// volume is being moved to a different node, or all the workload using the
// volume on a node has finished.
// This operation MUST be idempotent. If this RPC failed, or the CO does
// not know if it failed or not, it can choose to call NodeUnpublishVolume again.
func (d Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, missingAttr("NodeUnpublishVolume", req.VolumeId, "VolumeId")
	}

	if req.TargetPath == "" {
		return nil, missingAttr("NodeUnpublishVolume", req.VolumeId, "TargetPath")
	}

	err := d.mount.Unmount(req.TargetPath)
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats is unimplemented, there's no danger of it getting called
// until we update the node capabilities to say that we can do this.
func (d Driver) NodeGetVolumeStats(context.Context, *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return &csi.NodeGetVolumeStatsResponse{}, nil
}

// NodeGetCapabilities allows the CO to check the supported capabilities of
// node service provided by the Plugin.
func (d Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				}}},
		},
	}, nil
}

// NodeGetInfo The Plugin SHALL assume that this RPC will be executed on the node where
// the volume will be used. The CO SHOULD call this RPC for the node at
// which it wants to place the workload. The CO MAY call this RPC more than
// once for a given node. The SP SHALL NOT expect the CO to call this RPC
// more than once. The result of this call will be used by CO in
// ControllerPublishVolume.
func (d Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
		// TODO: I think we're limited by the number of linux block devices, which
		// is a massive number, but I'm not sure if something up the stack limits us.
		MaxVolumesPerNode: math.MaxInt64,
	}, nil
}

// CreateVolume will be called by the CO to provision a new volume on
// behalf of a user (to be consumed as either a block device or a mounted
// filesystem).
// This operation MUST be idempotent. If a volume corresponding to the
// specified volume name already exists, is accessible from
// accessibility_requirements, and is compatible with the specified
// capacity_range, volume_capabilities and parameters in the
// CreateVolumeRequest, the Plugin MUST reply 0 OK with the corresponding
// CreateVolumeResponse.
func (d Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.Name == "" {
		return &csi.CreateVolumeResponse{}, missingAttr("CreateVolume", req.Name, "Name")
	}
	if req.VolumeCapabilities == nil || len(req.VolumeCapabilities) == 0 {
		return &csi.CreateVolumeResponse{}, missingAttr("ValidateVolumeCapabilities", req.Name, "VolumeCapabilities")
	}

	// TODO: This needs to be a function.
	size := req.CapacityRange.GetRequiredBytes()
	limit := req.CapacityRange.GetLimitBytes()
	unlimited := limit == int64(0)
	if size > limit && !unlimited {
		return &csi.CreateVolumeResponse{}, status.Error(
			codes.Internal, fmt.Sprintf(
				"CreateVolume failed for %s: wanted %d bytes of storage, but size is limited to %d bytes",
				req.Name, size, limit))
	}

	createdByMe := "LINSTOR CSI Driver"

	// Handle case were a volume of the same name is already present.
	existingVolume, err := d.storage.GetByName(req.Name)
	if err != nil {
		return &csi.CreateVolumeResponse{}, status.Error(
			codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
	}

	if existingVolume != nil {

		d.log.Printf("found existing volume %s (%+v)", req.Name, existingVolume)

		if existingVolume.CreatedBy != createdByMe {
			return &csi.CreateVolumeResponse{}, status.Error(
				codes.AlreadyExists, fmt.Sprintf(
					"CreateVolume failed for %s: volume already present and wasn't created by %s",
					req.Name, createdByMe))
		}

		if existingVolume.SizeBytes != size {
			return &csi.CreateVolumeResponse{}, status.Error(
				codes.AlreadyExists, fmt.Sprintf(
					"CreateVolume failed for %s: volume already present, created by %s, but size differs (existing: %d, wanted:%d",
					req.Name, createdByMe, existingVolume.SizeBytes, size))
		}

		d.log.Printf("%s already present, but matches request", req.Name)

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId: existingVolume.ID,
				// TODO: Make sure this doesn't exceede LimitBytes.
				CapacityBytes: existingVolume.SizeBytes,
			}}, nil
	}

	// Spec says volume ID and name must differ.
	volumeID := uuid.New()
	vol := &volume.Info{
		Name: req.Name, ID: volumeID, SizeBytes: size, CreatedBy: createdByMe,
		CreationTime: time.Now(),
		Parameters:   make(map[string]string)}
	for k, v := range req.Parameters {
		vol.Parameters[k] = v
	}
	d.log.Printf("creating new volume %s (%+v)", vol.Name, vol)
	err = d.storage.Create(vol)
	if err != nil {
		return &csi.CreateVolumeResponse{}, status.Error(
			codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId: volumeID,
			// TODO: Make sure this doesn't exceede LimitBytes.
			CapacityBytes: size,
		}}, nil
}

// DeleteVolume will be called by the CO to deprovision a volume.
// This operation MUST be idempotent. If a volume corresponding to the
// specified volume_id does not exist or the artifacts associated with the
// volume do not exist anymore, the Plugin MUST reply 0 OK.
func (d Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("DeleteVolume", req.VolumeId, "VolumeId")
	}

	existingVolume, err := d.storage.GetByID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if existingVolume == nil {
		// Volume doesn't exist, and that's the point of this call after all.
		return &csi.DeleteVolumeResponse{}, nil
	}
	d.log.Printf("found existing volume %s (%+v)", existingVolume.Name, existingVolume)

	if err := d.storage.Delete(existingVolume); err != nil {
		return nil, err
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume will be called by the CO when it wants to place
// a workload that uses the volume onto a node. The Plugin SHOULD perform
// the work that is necessary for making the volume available on the given
// node. The Plugin MUST NOT assume that this RPC will be executed on the
// node where the volume will be used.
// This operation MUST be idempotent. If the volume corresponding to the
// volume_id has already been published at the node corresponding to the
// node_id, and is compatible with the specified volume_capability and
// readonly flag, the Plugin MUST reply 0 OK.
// If the operation failed or the CO does not know if the operation has
// failed or not, it MAY choose to call ControllerPublishVolume again or
// choose to call ControllerUnpublishVolume.
// The CO MAY call this RPC for publishing a volume to multiple nodes if
// the volume has MULTI_NODE capability (i.e., MULTI_NODE_READER_ONLY,
// MULTI_NODE_SINGLE_WRITER or MULTI_NODE_MULTI_WRITER).
func (d Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, missingAttr("ControllerPublishVolume", req.VolumeId, "VolumeId")
	}
	if req.NodeId == "" {
		return nil, missingAttr("ControllerPublishVolume", req.VolumeId, "NodeId")
	}
	if req.VolumeCapability == nil {
		return nil, missingAttr("ControllerPublishVolume", req.VolumeId, "VolumeCapability")
	}

	// Don't try to assign volumes that don't exist.
	existingVolume, err := d.storage.GetByID(req.VolumeId)
	if err != nil {
		return nil, status.Error(
			codes.Internal, fmt.Sprintf(
				"ControllerPublishVolume failed for %s: %v", req.VolumeId, err))
	}
	if existingVolume == nil {
		return nil, status.Error(
			codes.NotFound, fmt.Sprintf(
				"ControllerPublishVolume failed for %s: volume not present in storage backend",
				req.VolumeId))
	}
	d.log.Printf("found existing volume %s (%+v)", existingVolume.Name, existingVolume)

	// Or have had their attributes changed.
	if existingVolume.Readonly != req.Readonly {
		return nil, status.Error(
			codes.AlreadyExists, fmt.Sprintf(
				"ControllerPublishVolume failed for %s: volume attributes changes for already existing volume",
				req.VolumeId))
	}

	// Don't even attempt to put it on nodes that aren't avaible.
	ok, err := d.assignments.NodeAvailable(req.NodeId)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf(
			"ControllerPublishVolume failed for %s: %v", req.VolumeId, err))
	}
	if !ok {
		return nil, status.Error(codes.NotFound, fmt.Sprintf(
			"ControllerPublishVolume failed for %s node %s is not present in storage backend",
			req.VolumeId, req.NodeId))
	}

	err = d.assignments.Attach(existingVolume, req.NodeId)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf(
			"ControllerPublishVolume failed for %s: %v", req.VolumeId, err))
	}

	// TODO: Mount some stuff.

	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume is a reverse operation of
// ControllerPublishVolume. It MUST be called after all NodeUnstageVolume
// and NodeUnpublishVolume on the volume are called and succeed. The Plugin
// SHOULD perform the work that is necessary for making the volume ready to
// be consumed by a different node. The Plugin MUST NOT assume that this
// RPC will be executed on the node where the volume was previously used.
// This RPC is typically called by the CO when the workload using the
// volume is being moved to a different node, or all the workload using the
// volume on a node has finished.
// This operation MUST be idempotent. If the volume corresponding to the
// volume_id is not attached to the node corresponding to the node_id, the
// Plugin MUST reply 0 OK. If this operation failed, or the CO does not
// know if the operation failed or not, it can choose to call
// ControllerUnpublishVolume again.
func (d Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, missingAttr("ControllerUnpublishVolume", req.VolumeId, "VolumeId")
	}

	// TODO: Mount Stuff.

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities will be called by the CO to check if a
// pre-provisioned volume has all the capabilities that the CO wants.
// This RPC call SHALL return confirmed only if all the volume capabilities
// specified in the request are supported. This operation MUST be idempotent.
func (d Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.VolumeId == "" {
		return nil, missingAttr("ValidateVolumeCapabilities", req.VolumeId, "volumeId")
	}

	if req.VolumeCapabilities == nil {
		return nil, missingAttr("ValidateVolumeCapabilities", req.VolumeId, "VolumeCapabilities")
	}

	existingVolume, err := d.storage.GetByID(req.VolumeId)
	if err != nil {
		return nil, status.Error(
			codes.Internal, fmt.Sprintf("ValidateVolumeCapabilities failed for %s: %v", req.VolumeId, err))
	}
	if existingVolume == nil {
		return nil, status.Error(
			codes.NotFound, fmt.Sprintf("ValidateVolumeCapabilities failed for %s: volume not present in storage backend", req.VolumeId))
	}
	d.log.Printf("found existing volume %s (%+v)", existingVolume.Name, existingVolume)

	supportedCapabilities := []*csi.VolumeCapability{
		{AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	}

	for _, reqCap := range req.VolumeCapabilities {
		if reqCap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: supportedCapabilities,
		}}, nil
}

// ListVolumes SHALL return the information about all the volumes that it
// knows about. If volumes are created and/or deleted while the CO is
// concurrently paging through ListVolumes results then it is possible that
// the CO MAY either witness duplicate volumes in the list, not witness
// existing volumes, or both. The CO SHALL NOT expect a consistent "view"
// of all volumes when paging through the volume list via multiple calls to
// ListVolumes.
func (d Driver) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return &csi.ListVolumesResponse{}, nil
}

// GetCapacity allows the CO to query the capacity of the storage pool from which the
// controller provisions volumes.
func (d Driver) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return &csi.GetCapacityResponse{}, nil
}

// ControllerGetCapabilities allows the CO to check the supported
// capabilities of controller service provided by the Plugin.
func (d Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				}}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				}}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				}}},
			// TODO: Add ListVolumes.
		},
	}, nil
}

func (d Driver) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return &csi.CreateSnapshotResponse{}, nil
}

func (d Driver) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return &csi.DeleteSnapshotResponse{}, nil
}

func (d Driver) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return &csi.ListSnapshotsResponse{}, nil
}

// Run the server.
func (d Driver) Run() error {
	u, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("unable to parse address: %q", err)
	}

	addr := path.Join(u.Host, filepath.FromSlash(u.Path))
	if u.Host == "" {
		addr = filepath.FromSlash(u.Path)
	}

	// CSI plugins talk only over UNIX sockets currently
	if u.Scheme != "unix" {
		return fmt.Errorf("currently only unix domain sockets are supported, have: %s", u.Scheme)
	}
	if err = os.Remove(addr); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove previously used unix domain socket file %s, error: %v", addr, err)
	}

	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	d.srv = grpc.NewServer()
	csi.RegisterIdentityServer(d.srv, d)
	csi.RegisterControllerServer(d.srv, d)
	csi.RegisterNodeServer(d.srv, d)

	d.log.Printf("server started (%s)", addr)
	return d.srv.Serve(listener)
}

// Stop the server.
func (d Driver) Stop() error {
	return fmt.Errorf("Not implemented")
}

func missingAttr(methodCall, volumeID, attr string) error {
	if volumeID == "" {
		volumeID = "an unknown volume"
	}
	return status.Error(codes.InvalidArgument, fmt.Sprintf("%s failed for %s: it requires a %s and none was provided", methodCall, volumeID, attr))
}
