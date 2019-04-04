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
	"math"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/haySwim/data"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Version is set via ldflags configued in the Makefile.
var Version = "UNKNOWN"

// Driver fullfils CSI controller, node, and indentity server interfaces.
type Driver struct {
	Storage     volume.CreateDeleter
	Assignments volume.AttacherDettacher
	Mounter     volume.Mounter
	Snapshots   volume.SnapshotCreateDeleter
	srv         *grpc.Server
	log         *logrus.Entry
	version     string
	name        string
	endpoint    string
	nodeID      string
}

// NewDriver build up a driver.
func NewDriver(options ...func(*Driver) error) (*Driver, error) {
	// Set up default noop-ish storage backend.
	mockStorage := &client.MockStorage{}

	d := &Driver{
		name:        "io.drbd.linstor-csi",
		version:     Version,
		nodeID:      "localhost",
		Storage:     mockStorage,
		Assignments: mockStorage,
		Mounter:     mockStorage,
		Snapshots:   mockStorage,
		log:         logrus.NewEntry(logrus.New()),
	}

	d.log.Logger.SetOutput(ioutil.Discard)
	d.log.Logger.SetFormatter(&logrus.TextFormatter{})

	d.endpoint = fmt.Sprintf("unix:///var/lib/kubelet/plugins/%s/csi.sock", d.name)

	for _, opt := range options {
		err := opt(d)
		if err != nil {
			return nil, err
		}
	}

	// Add in fields that may have been configured above.
	d.log = d.log.WithFields(logrus.Fields{
		"linstorCSIComponent": "driver",
		"version":             d.version,
		"provisioner":         d.name,
		"nodeID":              d.nodeID,
	})

	return d, nil
}

// Storage configures the volume service backend.
func Storage(s volume.CreateDeleter) func(*Driver) error {
	return func(d *Driver) error {
		d.Storage = s
		return nil
	}
}

// Assignments configures the volume attachment service backend.
func Assignments(a volume.AttacherDettacher) func(*Driver) error {
	return func(d *Driver) error {
		d.Assignments = a
		return nil
	}
}

// Snapshots configures the volume snapshot service backend.
func Snapshots(s volume.SnapshotCreateDeleter) func(*Driver) error {
	return func(d *Driver) error {
		d.Snapshots = s
		return nil
	}
}

// Mounter configures the volume mounting service backend.
func Mounter(m volume.Mounter) func(*Driver) error {
	return func(d *Driver) error {
		d.Mounter = m
		return nil
	}
}

// NodeID configures the driver node ID.
func NodeID(nodeID string) func(*Driver) error {
	return func(d *Driver) error {
		d.nodeID = nodeID
		return nil
	}
}

// Endpoint configures the driver name.
func Endpoint(ep string) func(*Driver) error {
	return func(d *Driver) error {
		d.endpoint = ep
		return nil
	}
}

// Name configures the driver name.
func Name(name string) func(*Driver) error {
	return func(d *Driver) error {
		d.name = name
		return nil
	}
}

// LogOut sets the driver to write logs to the provided io.writer
// instead of discarding logs.
func LogOut(out io.Writer) func(*Driver) error {
	return func(d *Driver) error {
		d.log.Logger.SetOutput(out)
		return nil
	}
}

// LogFmt sets the format of the log outpout via the provided logrus.Formatter.
func LogFmt(fmt logrus.Formatter) func(*Driver) error {
	return func(d *Driver) error {
		d.log.Logger.SetFormatter(fmt)
		return nil
	}
}

// Debug sets debugging log behavor.
func Debug(d *Driver) error {
	d.log.Logger.SetLevel(logrus.DebugLevel)
	d.log.Logger.SetReportCaller(true)
	return nil
}

// GetPluginInfo returns driver info
func (d Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          d.name,
		VendorVersion: d.version,
	}, nil
}

// GetPluginCapabilities returns available capabilities of the plugin
func (d Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				}}},
			{Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				}}},
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

	existingVolume, err := d.Storage.GetByID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	assignedTo, err := d.Assignments.GetAssignmentOnNode(existingVolume, d.nodeID)
	if err != nil {
		return nil, err
	}

	err = d.Mounter.Mount(existingVolume, assignedTo.Path, req.StagingTargetPath, fsType, mntOpts)
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

	err := d.Mounter.Unmount(req.StagingTargetPath)
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

	existingVolume, err := d.Storage.GetByID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	err = d.Mounter.Mount(existingVolume, req.StagingTargetPath, req.TargetPath, fsType, mntOpts)
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

	err := d.Mounter.Unmount(req.TargetPath)
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
		MaxVolumesPerNode:  math.MaxInt64,
		AccessibleTopology: &csi.Topology{},
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

	requiredKiB, err := d.Storage.AllocationSizeKiB(req.CapacityRange.GetRequiredBytes(), req.CapacityRange.GetLimitBytes())
	if err != nil {
		return &csi.CreateVolumeResponse{}, status.Error(
			codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
	}

	volumeSize := data.NewKibiByte(data.KiB * data.ByteSize(requiredKiB))

	// Handle case were a volume of the same name is already present.
	existingVolume, err := d.Storage.GetByName(req.Name)
	if err != nil {
		return &csi.CreateVolumeResponse{}, status.Error(
			codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
	}

	if existingVolume != nil {

		d.log.WithFields(logrus.Fields{
			"requestedVolumeName": req.Name,
			"existingVolume":      fmt.Sprintf("%+v", existingVolume),
		}).Info("found existing volume")

		if existingVolume.CreatedBy != d.name {
			return &csi.CreateVolumeResponse{}, status.Error(
				codes.AlreadyExists, fmt.Sprintf(
					"CreateVolume failed for %s: volume already present and wasn't created by %s",
					req.Name, d.name))
		}

		if existingVolume.SizeBytes != int64(volumeSize.InclusiveBytes()) {
			return &csi.CreateVolumeResponse{}, status.Error(
				codes.AlreadyExists, fmt.Sprintf(
					"CreateVolume failed for %s: volume already present, created by %s, but size differs (existing: %d, wanted: %d",
					req.Name, d.name, existingVolume.SizeBytes, int64(volumeSize.InclusiveBytes())))
		}

		d.log.WithFields(logrus.Fields{
			"requestedVolumeName": req.Name,
			"volumeSize":          volumeSize,
			"expectedCreatedBy":   d.name,
			"existingVolume":      fmt.Sprintf("%+v", existingVolume),
		}).Info("volume already present, but matches request")

		topos, err := d.Storage.AccessibleTopologies(existingVolume)
		if err != nil {
			return &csi.CreateVolumeResponse{}, status.Errorf(
				codes.Internal, "CreateVolume failed for %s: unable to determine volume topology: %v",
				req.Name, err)
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:           existingVolume.ID,
				CapacityBytes:      existingVolume.SizeBytes,
				AccessibleTopology: topos,
			}}, nil
	}

	return d.createNewVolume(req)
}

// DeleteVolume will be called by the CO to deprovision a volume.
// This operation MUST be idempotent. If a volume corresponding to the
// specified volume_id does not exist or the artifacts associated with the
// volume do not exist anymore, the Plugin MUST reply 0 OK.
func (d Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("DeleteVolume", req.VolumeId, "VolumeId")
	}

	existingVolume, err := d.Storage.GetByID(req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if existingVolume == nil {
		// Volume doesn't exist, and that's the point of this call after all.
		return &csi.DeleteVolumeResponse{}, nil
	}
	d.log.WithFields(logrus.Fields{
		"existingVolume": fmt.Sprintf("%+v", existingVolume),
	}).Info("found existing volume")

	if err := d.Storage.Delete(existingVolume); err != nil {
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
	existingVolume, err := d.Storage.GetByID(req.VolumeId)
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
	d.log.WithFields(logrus.Fields{
		"existingVolume": fmt.Sprintf("%+v", existingVolume),
	}).Info("found existing volume")

	// Or have had their attributes changed.
	if existingVolume.Readonly != req.Readonly {
		return nil, status.Error(
			codes.AlreadyExists, fmt.Sprintf(
				"ControllerPublishVolume failed for %s: volume attributes changes for already existing volume",
				req.VolumeId))
	}

	// Don't even attempt to put it on nodes that aren't avaible.
	if ok, err := d.Assignments.NodeAvailable(req.NodeId); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf(
			"ControllerPublishVolume failed for %s: %v", req.VolumeId, err))
	} else if !ok {
		return nil, status.Error(codes.NotFound, fmt.Sprintf(
			"ControllerPublishVolume failed for %s node %s is not present in storage backend",
			req.VolumeId, req.NodeId))
	}

	err = d.Assignments.Attach(existingVolume, req.NodeId)
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
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ControllerUnpublishVolume", req.GetVolumeId(), "VolumeId")
	}
	if req.GetNodeId() == "" {
		return nil, missingAttr("ControllerUnpublishVolume", req.GetNodeId(), "NodeId")
	}

	vol, err := d.Storage.GetByID(req.VolumeId)
	if err != nil {
		return nil, status.Error(
			codes.Internal, fmt.Sprintf(
				"ControllerUnpublishVolume failed for %s: %v", req.GetVolumeId(), err))
	}
	// If it's not there, it's as detached as we can make it, right?
	if vol == nil {
		d.log.WithFields(logrus.Fields{
			"volumeId": req.GetVolumeId(),
			"nodeId":   req.GetNodeId(),
		}).Info("volume to be unpublished was not found in storage backend")
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	d.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
		"nodeId": req.GetNodeId(),
	}).Info("found existing volume to unpublish")

	if err := d.Assignments.Detach(vol, req.GetNodeId()); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf(
			"ControllerpublishVolume failed for %s: %v", req.GetVolumeId(), err))
	}

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

	existingVolume, err := d.Storage.GetByID(req.VolumeId)
	if err != nil {
		return nil, status.Error(
			codes.Internal, fmt.Sprintf("ValidateVolumeCapabilities failed for %s: %v", req.VolumeId, err))
	}
	if existingVolume == nil {
		return nil, status.Error(
			codes.NotFound, fmt.Sprintf("ValidateVolumeCapabilities failed for %s: volume not present in storage backend", req.VolumeId))
	}
	d.log.WithFields(logrus.Fields{
		"existingVolume": fmt.Sprintf("%+v", existingVolume),
	}).Info("found existing volume")

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
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				}}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
				}}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
				}}},
			// TODO: Add ListVolumes.
		},
	}, nil
}

func (d Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.GetSourceVolumeId() == "" {
		return nil, missingAttr("CreateSnapshot", req.SourceVolumeId, "SourceVolumeId")
	}
	if req.GetName() == "" {
		return nil, missingAttr("CreateSnapshot", req.Name, "Name")
	}

	existingSnap, err := d.Snapshots.GetSnapByName(req.GetName())
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing snapshot: %v", err)
	}

	if existingSnap != nil {
		// Needed for idempotentcy.
		d.log.WithFields(logrus.Fields{
			"requestedSnapshotName":         req.GetName(),
			"requestedSnapshotSourceVolume": req.GetSourceVolumeId(),
			"existingSnapshot":              fmt.Sprintf("%+v", existingSnap),
		}).Info("found existing snapshot")
		if existingSnap.CsiSnap.SourceVolumeId == req.GetSourceVolumeId() {
			return &csi.CreateSnapshotResponse{Snapshot: existingSnap.CsiSnap}, nil
		}
		return nil, status.Errorf(codes.AlreadyExists, "can't use %q for snapshot name for volume %q, snapshot name is in use for volume %q",
			existingSnap.Name, req.GetSourceVolumeId(), existingSnap.CsiSnap.SourceVolumeId)
	}

	snap, err := d.Snapshots.SnapCreate(&volume.SnapInfo{
		Name:    d.Snapshots.CanonicalizeSnapshotName(req.GetName()),
		CsiSnap: &csi.Snapshot{SourceVolumeId: req.GetSourceVolumeId()},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %v", err)
	}

	return &csi.CreateSnapshotResponse{Snapshot: snap.CsiSnap}, nil
}

func (d Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, missingAttr("DeleteSnapshot", req.SnapshotId, "SnapshotId")
	}

	snap, err := d.Snapshots.GetSnapByID(req.GetSnapshotId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to find snapshot %s: %v",
			req.GetSnapshotId(), err)
	}
	if snap == nil {
		d.log.WithFields(logrus.Fields{
			"snapshotId": req.GetSnapshotId(),
		}).Info("unable to find snapshot, it may already be deleted")
		return &csi.DeleteSnapshotResponse{}, nil
	}

	if err := d.Snapshots.SnapDelete(snap); err != nil {
		return nil, status.Errorf(codes.Internal, "unable to delete snapshot %s: %v",
			req.GetSnapshotId(), err)
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots https://github.com/container-storage-interface/spec/blob/v1.1.0/spec.md#listsnapshots
func (d Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	snapshots := []*volume.SnapInfo{}

	// Handle case where a single snapshot is requsted.
	if req.GetSnapshotId() != "" {
		snap, err := d.Snapshots.GetSnapByID(req.GetSnapshotId())
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshots: %v", err)
		}
		if snap == nil {
			return &csi.ListSnapshotsResponse{}, nil
		}
		d.log.WithFields(logrus.Fields{
			"requestedSnapshot": req.GetSnapshotId(),
			"napshot":           fmt.Sprintf("%+v", snap),
		}).Info("found single snapshot")
		snapshots = []*volume.SnapInfo{snap}

		// Handle case where a single volumes snapshots are requested.
	} else if req.GetSourceVolumeId() != "" {
		vol, err := d.Storage.GetByID(req.GetSourceVolumeId())
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshots: %v", err)
		}
		if vol == nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		snapshots = vol.Snapshots

		// Regular list of all snapshots.
	} else {
		snaps, err := d.Snapshots.ListSnaps()
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshots: %v", err)
		}
		snapshots = snaps
	}

	// Handle pagination.
	var (
		start     int32
		end       int32
		nextToken string
	)
	totalSnaps := int32(len(snapshots))
	if req.GetMaxEntries() == 0 || (totalSnaps <= req.GetMaxEntries()) {
		end = totalSnaps
	} else {
		end = req.GetMaxEntries()
		nextToken = strconv.Itoa(int(end))
	}

	if req.GetStartingToken() != "" {
		i, err := strconv.ParseInt(req.GetStartingToken(), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed snapshot starting token: %v", err)
		}
		start = int32(i)
	} else {
		start = int32(0)
	}
	snapshots = snapshots[start:end]

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(snapshots))

	for _, snap := range snapshots {
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: snap.CsiSnap})
	}
	return &csi.ListSnapshotsResponse{Entries: entries, NextToken: nextToken}, nil
}

func (d Driver) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	panic("not implemented")
}
func (d Driver) ControllerExpandVolume(context.Context, *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	panic("not implemented")
}

// Run the server.
func (d Driver) Run() error {
	d.log.Debug("Preparing to start server")

	u, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("unable to parse address: %q", err)
	}

	addr := path.Join(u.Host, filepath.FromSlash(u.Path))
	if u.Host == "" {
		addr = filepath.FromSlash(u.Path)
	}

	// csi plugins talk only over unix sockets currently
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

	//type UnaryServerInterceptor func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (resp interface{}, err error)
	errHandler := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)

		if err == nil {
			d.log.WithFields(logrus.Fields{
				"method": info.FullMethod,
				"req":    fmt.Sprintf("%+v", req),
				"resp":   fmt.Sprintf("%+v", resp),
			}).Debug("method called")
		} else {
			d.log.WithFields(logrus.Fields{
				"method": info.FullMethod,
				"req":    fmt.Sprintf("%+v", req),
				"resp":   fmt.Sprintf("%+v", resp),
			}).WithError(err).Error("method failed")
		}

		return resp, err
	}

	d.srv = grpc.NewServer(grpc.UnaryInterceptor(errHandler))
	csi.RegisterIdentityServer(d.srv, d)
	csi.RegisterControllerServer(d.srv, d)
	csi.RegisterNodeServer(d.srv, d)

	d.log.WithFields(logrus.Fields{
		"address": addr,
	}).Info("server started")
	return d.srv.Serve(listener)
}

// Stop the server.
func (d Driver) Stop() error {
	return fmt.Errorf("Not implemented")
}

func (d Driver) createNewVolume(req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// volumeID is the "ID" this volume is referred to. It is part of the CreateVolumeResponse and is the final LINSTOR/DRBD resource name
	volumeID := d.Storage.CanonicalizeVolumeName(req.Name)

	requiredKiB, err := d.Storage.AllocationSizeKiB(req.CapacityRange.GetRequiredBytes(), req.CapacityRange.GetLimitBytes())
	if err != nil {
		return &csi.CreateVolumeResponse{}, status.Error(
			codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
	}

	volumeSize := data.NewKibiByte(data.KiB * data.ByteSize(requiredKiB))

	vol := &volume.Info{
		Name: req.Name, ID: volumeID, SizeBytes: int64(volumeSize.InclusiveBytes()), CreatedBy: d.name,
		CreationTime: time.Now(),
		Parameters:   make(map[string]string),
		Snapshots:    []*volume.SnapInfo{},
	}
	for k, v := range req.Parameters {
		vol.Parameters[k] = v
	}
	d.log.WithFields(logrus.Fields{
		"newVolume": fmt.Sprintf("%+v", vol),
		"size":      volumeSize,
	}).Debug("creating new volume")

	// We're cloning from a volume or snapshot.
	if req.GetVolumeContentSource() != nil {
		if req.GetVolumeContentSource().GetSnapshot() != nil {
			snapshotId := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
			if snapshotId == "" {
				return &csi.CreateVolumeResponse{}, status.Error(codes.InvalidArgument, fmt.Sprintf("CreateVolume failed for %s: empty snapshotId", req.Name))
			}

			snap, err := d.Snapshots.GetSnapByID(snapshotId)
			if err != nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
			}
			if snap == nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.NotFound, fmt.Sprintf("CreateVolume failed for %s: snapshot not found in storage backend", req.Name))
			}
			sourceVol, err := d.Storage.GetByID(snap.CsiSnap.SourceVolumeId)
			if err != nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
			}
			if sourceVol == nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.NotFound, fmt.Sprintf("CreateVolume failed for %s: source volume not found in storage backend", req.Name))
			}

			if err := d.Snapshots.VolFromSnap(snap, vol); err != nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
			}
			// We're cloning from a whole volume.
		} else if req.GetVolumeContentSource().GetVolume() != nil {
			sourceVol, err := d.Storage.GetByID(req.GetVolumeContentSource().GetVolume().GetVolumeId())
			if err != nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
			}
			if sourceVol == nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.NotFound, fmt.Sprintf("CreateVolume failed for %s: source volume not found in storage backend", req.Name))
			}
			if err := d.Snapshots.VolFromVol(sourceVol, vol); err != nil {
				return &csi.CreateVolumeResponse{}, status.Error(
					codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
			}
		} else {
			return &csi.CreateVolumeResponse{}, status.Error(
				codes.InvalidArgument, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
		}
		// Regular new volume.
	} else {
		if err := d.Storage.Create(vol); err != nil {
			return &csi.CreateVolumeResponse{}, status.Error(
				codes.Internal, fmt.Sprintf("CreateVolume failed for %s: %v", req.Name, err))
		}
	}

	topos, err := d.Storage.AccessibleTopologies(vol)
	if err != nil {
		return &csi.CreateVolumeResponse{}, status.Errorf(
			codes.Internal, "CreateVolume failed for %s: unable to determine volume topology: %v",
			req.Name, err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           volumeID,
			CapacityBytes:      int64(volumeSize.InclusiveBytes()),
			AccessibleTopology: topos,
		}}, nil
}

func missingAttr(methodCall, volumeID, attr string) error {
	if volumeID == "" {
		volumeID = "an unknown volume"
	}
	return status.Error(codes.InvalidArgument, fmt.Sprintf("%s failed for %s: it requires a %s and none was provided", methodCall, volumeID, attr))
}
