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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	lc "github.com/LINBIT/golinstor"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/haySwim/data"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Version is set via ldflags configued in the Makefile.
var Version = "UNKNOWN"

// Driver fullfils CSI controller, node, and indentity server interfaces.
type Driver struct {
	Storage       volume.CreateDeleter
	Assignments   volume.AttacherDettacher
	Mounter       volume.Mounter
	Snapshots     volume.SnapshotCreateDeleter
	VolumeStatter volume.VolumeStatter
	Expander      volume.Expander
	NodeInformer  volume.NodeInformer
	kubeClient    dynamic.Interface
	cancel        context.CancelFunc
	srv           *grpc.Server
	log           *logrus.Entry
	version       string
	// name distingushes the driver from other drivers and is used to mark
	// volumes so that volumes provisioned by another driver are not interfered with.
	name string
	// endpoint is the socket over which all CSI calls are requested and responded to.
	endpoint string
	// nodeID is the hostname of the node where this plugin is running locally.
	nodeID string
	// topologyPrefix is the name
	topologyPrefix string
	// resyncAfter is the interval after which reconciliations should be retried
	resyncAfter time.Duration

	// Embed for forward compatibility.
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
}

// NewDriver builds up a driver.
func NewDriver(options ...func(*Driver) error) (*Driver, error) {
	// Set up default noop-ish storage backend.
	mockStorage := client.NewMockStorage()

	d := &Driver{
		name:           linstor.DriverName,
		version:        Version,
		nodeID:         "localhost",
		Storage:        mockStorage,
		Assignments:    mockStorage,
		Mounter:        mockStorage,
		Snapshots:      mockStorage,
		Expander:       mockStorage,
		VolumeStatter:  mockStorage,
		NodeInformer:   mockStorage,
		log:            logrus.NewEntry(logrus.New()),
		topologyPrefix: lc.NamespcAuxiliary,
		resyncAfter:    5 * time.Minute,
	}

	d.log.Logger.SetOutput(ioutil.Discard)
	d.log.Logger.SetFormatter(&logrus.TextFormatter{})

	d.endpoint = fmt.Sprintf("unix:///var/lib/kubelet/plugins/%s/csi.sock", d.name)

	// Run options functions.
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

func Expander(s volume.Expander) func(*Driver) error {
	return func(d *Driver) error {
		d.Expander = s
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

// VolumeStatter configures the volume stats service backend.
func VolumeStatter(s volume.VolumeStatter) func(*Driver) error {
	return func(d *Driver) error {
		d.VolumeStatter = s
		return nil
	}
}

func NodeInformer(n volume.NodeInformer) func(*Driver) error {
	return func(d *Driver) error {
		d.NodeInformer = n
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

func TopologyPrefix(prefix string) func(*Driver) error {
	return func(d *Driver) error {
		d.topologyPrefix = prefix
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

// LogLevel sets the logging intensity. Debug additionally reports the function
// from which the logger was called.
func LogLevel(s string) func(*Driver) error {
	return func(d *Driver) error {
		level, err := logrus.ParseLevel(s)
		if err != nil {
			return fmt.Errorf("unable to use %s as a logging level: %v", s, err)
		}

		d.log.Logger.SetLevel(level)

		// logs function name from which the logger was called
		if level == logrus.DebugLevel {
			d.log.Logger.SetReportCaller(true)
		}
		return nil
	}
}

func ConfigureKubernetesIfAvailable() func(*Driver) error {
	return func(d *Driver) error {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			// Not running in kubernetes
			return nil
		}

		d.kubeClient, err = dynamic.NewForConfig(cfg)

		return err
	}
}

// ResyncAfter sets the interval in which certain resources should be synced.
//
// Currently, this only applies to VolumeSnapshotClassses.
// Set to 0 to disable syncing.
func ResyncAfter(resyncAfter time.Duration) func(*Driver) error {
	return func(d *Driver) error {
		d.resyncAfter = resyncAfter
		return nil
	}
}

// GetPluginInfo https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#getplugininfo
func (d Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          d.name,
		VendorVersion: d.version,
	}, nil
}

// GetPluginCapabilities https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#getplugincapabilities
func (d Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			}},
			{Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				},
			}},
			{Type: &csi.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
					Type: csi.PluginCapability_VolumeExpansion_ONLINE,
				},
			}},
		},
	}, nil
}

// Probe https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#probe
func (d Driver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// NodeStageVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodestagevolume
func (d Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// NodeUnstageVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodeunstagevolume
func (d Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// NodePublishVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodepublishvolume
func (d Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return &csi.NodePublishVolumeResponse{}, missingAttr("NodePublishVolume", req.GetVolumeId(), "VolumeId")
	}

	if req.GetTargetPath() == "" {
		return &csi.NodePublishVolumeResponse{}, missingAttr("NodePublishVolume", req.GetVolumeId(), "TargetPath")
	}

	if req.GetVolumeCapability() == nil {
		return &csi.NodePublishVolumeResponse{}, missingAttr("NodePublishVolume", req.GetVolumeId(), "VolumeCapability slice")
	}

	volCtx, err := VolumeContextFromMap(req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodePublishVolume failed for %s: invalid volume context: %v", req.GetVolumeId(), err)
	}

	if volCtx == nil {
		params, err := d.Storage.GetLegacyVolumeParameters(ctx, req.GetVolumeId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "NodePublishVolume failed for %s: could not find volume parameters in context or legacy LINSTOR property", req.GetVolumeId())
		}

		if params != nil {
			volCtx = VolumeContextFromParameters(params)
		} else {
			volCtx = NewVolumeContext()
		}
	}

	volCtx.MountOptions = append(volCtx.MountOptions, "_netdev")

	publishCtx := PublishContextFromMap(req.GetPublishContext())
	if publishCtx == nil {
		assignment, err := d.Assignments.FindAssignmentOnNode(ctx, req.GetVolumeId(), d.nodeID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "NodePublishVolume failed for %s: %v", req.GetVolumeId(), err)
		}

		if assignment == nil {
			return nil, status.Errorf(codes.NotFound, "NodePublishVolume failed for %s: assignment not found", req.GetVolumeId())
		}

		publishCtx = &PublishContext{
			DevicePath: assignment.Path,
		}
	}

	if block := req.GetVolumeCapability().GetBlock(); block != nil {
		volCtx.MountOptions = []string{"bind"}
	}

	var fsType string

	if mnt := req.GetVolumeCapability().GetMount(); mnt != nil {
		switch {
		case publishCtx.FsType != "":
			fsType = publishCtx.FsType
		case mnt.FsType != "":
			fsType = mnt.FsType
		default:
			fsType = "ext4"
		}

		volCtx.MountOptions = append(volCtx.MountOptions, mnt.GetMountFlags()...)
	}

	if fsType == "xfs" {
		// Restored snapshots inherit the XFS UUID of the original source. If mounted on the same node as the original
		// without this option, XFS will complain about a duplicate UUID and refuse to mount.
		volCtx.MountOptions = append(volCtx.MountOptions, "nouuid")
	}

	ro := req.GetReadonly() || req.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY

	err = d.Mounter.Mount(ctx, publishCtx.DevicePath, req.GetTargetPath(), fsType, ro, volCtx.MountOptions)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodePublishVolume failed for %s: %v", req.GetVolumeId(), err)
	}

	// Runs post-mount xfs_io if PostMountXfsOpts is configured
	if fsType == "xfs" && volCtx.PostMountXfsOptions != "" {
		d.log.WithFields(logrus.Fields{
			"XFS_IO":     volCtx.PostMountXfsOptions,
			"FSType":     fsType,
			"targetPath": req.GetTargetPath(),
		}).Debug("Post-mount XFS_io")

		_, err := exec.Command("xfs_io", "-c", volCtx.PostMountXfsOptions, req.GetTargetPath()).CombinedOutput()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "NodePublishVolume failed for %s: %v", req.GetVolumeId(), err)
		}
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodeunpublishvolume
func (d Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("NodeUnpublishVolume", req.GetVolumeId(), "VolumeId")
	}

	if req.GetTargetPath() == "" {
		return nil, missingAttr("NodeUnpublishVolume", req.GetVolumeId(), "TargetPath")
	}

	err := d.Mounter.Unmount(req.GetTargetPath())
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodegetvolumestats
func (d Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("NodeGetVolumeStats", req.GetVolumeId(), "VolumeId")
	}

	if req.GetVolumePath() == "" {
		return nil, missingAttr("NodeGetVolumeStats", req.GetVolumeId(), "VolumeId")
	}

	mounted, err := d.Mounter.IsMountPoint(req.GetVolumePath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodeGetVolumeStats failed for %s: failed to check if path %v is mounted: %v", req.GetVolumeId(), req.GetVolumePath(), err)
	}

	if !mounted {
		return nil, status.Errorf(codes.NotFound, "NodeGetVolumeStats failed for %s: path %v is not mounted", req.GetVolumeId(), req.GetVolumePath())
	}

	stats, err := d.VolumeStatter.GetVolumeStats(req.GetVolumePath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodeGetVolumeStats failed for %s: failed to get stats: %v", req.GetVolumeId(), err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: stats.AvailableBytes,
				Total:     stats.TotalBytes,
				Used:      stats.UsedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: stats.AvailableInodes,
				Total:     stats.TotalInodes,
				Used:      stats.UsedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

// NodeGetCapabilities https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodegetcapabilities
func (d Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
		},
	}, nil
}

// NodeGetInfo https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodegetinfo
func (d Driver) NodeGetInfo(ctx context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	topology, err := d.NodeInformer.GetNodeTopologies(ctx, d.nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve node topology: %w", err)
	}

	return &csi.NodeGetInfoResponse{
		NodeId:             d.nodeID,
		MaxVolumesPerNode:  1048576, // DRBD volumes per node limit.
		AccessibleTopology: topology,
	}, nil
}

// CreateVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#createvolume
func (d Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, missingAttr("CreateVolume", req.GetName(), "Name")
	}
	if req.GetVolumeCapabilities() == nil || len(req.GetVolumeCapabilities()) == 0 {
		return nil, missingAttr("ValidateVolumeCapabilities", req.GetName(), "VolumeCapabilities")
	}

	fsType, err := validateCapabilities(req.GetVolumeCapabilities())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateVolume failed for %s: %v", req.Name, err)
	}

	// Determine how much storage we need to actually allocate for a given number
	// of bytes.
	requiredKiB, err := d.Storage.AllocationSizeKiB(req.GetCapacityRange().GetRequiredBytes(), req.GetCapacityRange().GetLimitBytes(), fsType)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "CreateVolume failed for %s: %v", req.Name, err)
	}
	volumeSize := data.NewKibiByte(data.KiB * data.ByteSize(requiredKiB))

	params, err := volume.NewParameters(req.GetParameters(), d.topologyPrefix)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse parameters: %v", err)
	}

	var pvcNamespace, pvcName string
	if params.UsePvcName {
		pvcName = req.GetParameters()[ParameterCsiPvcName]
		pvcNamespace = req.GetParameters()[ParameterCsiPvcNamespace]
	}

	volId := d.Storage.CompatibleVolumeId(req.GetName(), pvcNamespace, pvcName)

	log := d.log.WithField("volume", volId)
	log.Infof("determined volume id for volume named '%s'", req.GetName())

	// Handle case were a volume of the same name is already present.
	existingVolume, err := d.Storage.FindByID(ctx, volId)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "CreateVolume failed for %s: %v", req.Name, err)
	}

	// Ignore this check for existing volumes with no FsType set: they may have been provisioned before we used the
	// LINSTOR built-in method. Also ignore if we have no fstype set. Using FS as block volume is fine.
	if existingVolume != nil && existingVolume.FsType != "" && fsType != "" && existingVolume.FsType != fsType {
		return nil, status.Errorf(codes.AlreadyExists, "FsType don't match: existing: '%s', requested: '%s'", existingVolume.FsType, fsType)
	}

	if existingVolume != nil && strings.HasPrefix(existingVolume.Properties[linstor.PropertyProvisioningCompletedBy], "linstor-csi") {
		log.WithField("existingVolume", existingVolume).Info("volume already present")

		if existingVolume.SizeBytes != int64(volumeSize.InclusiveBytes()) {
			return nil, status.Errorf(codes.AlreadyExists,
				"CreateVolume failed for %s: volume already present, but size differs (existing: %d, wanted: %d)",
				volId, existingVolume.SizeBytes, int64(volumeSize.InclusiveBytes()))
		}

		if existingVolume.ResourceGroup != params.ResourceGroup {
			return nil, status.Errorf(codes.AlreadyExists,
				"CreateVolume failed for %s: volume already present, but resource group differs (existing: %s, wanted: %s)",
				volId, existingVolume.ResourceGroup, params.ResourceGroup)
		}

		log.Info("existing volume matches request")

		log.Debug("check if source snapshot exists")

		snapId := d.Snapshots.CompatibleSnapshotId(snapshotForVolumeName(req.GetName()))
		leftoverSnap, _, err := d.Snapshots.FindSnapByID(ctx, snapId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to check on potential left-over source snapshot: %v", err)
		}

		if leftoverSnap != nil {
			log.Debug("found left-over source snapshot, removing...")

			err := d.Snapshots.SnapDelete(ctx, leftoverSnap)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to delete source snapshot for volume '%s': %v", volId, err)
			}
		}

		topos, err := d.Storage.AccessibleTopologies(ctx, existingVolume.ID, &params)
		if err != nil {
			return nil, status.Errorf(
				codes.Internal, "CreateVolume failed for %s: unable to determine volume topology: %v",
				volId, err)
		}

		volCtx, err := VolumeContextFromParameters(&params).ToMap()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: unable to encode volume context: %v", volId, err)
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:           existingVolume.ID,
				CapacityBytes:      existingVolume.SizeBytes,
				ContentSource:      req.GetVolumeContentSource(),
				AccessibleTopology: topos,
				VolumeContext:      volCtx,
			},
		}, nil
	}

	return d.createNewVolume(
		ctx,
		&volume.Info{
			ID:            volId,
			SizeBytes:     int64(volumeSize.InclusiveBytes()),
			ResourceGroup: params.ResourceGroup,
			FsType:        fsType,
			Properties:    map[string]string{linstor.PropertyProvisioningCompletedBy: "linstor-csi/" + Version},
		},
		&params,
		req,
	)
}

// DeleteVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#deletevolume
func (d Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("DeleteVolume", req.GetVolumeId(), "VolumeId")
	}

	err := d.Storage.Delete(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllerpublishvolume
func (d Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ControllerPublishVolume", req.GetVolumeId(), "VolumeId")
	}
	if req.GetNodeId() == "" {
		return nil, missingAttr("ControllerPublishVolume", req.GetVolumeId(), "NodeId")
	}
	if req.GetVolumeCapability() == nil {
		return nil, missingAttr("ControllerPublishVolume", req.GetVolumeId(), "VolumeCapability")
	}

	// Don't try to assign volumes that don't exist.
	existingVolume, err := d.Storage.FindByID(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerPublishVolume failed for %s: %v", req.GetVolumeId(), err)
	}
	if existingVolume == nil {
		return nil, status.Errorf(codes.NotFound,
			"ControllerPublishVolume failed for %s: volume not present in storage backend",
			req.GetVolumeId())
	}

	d.log.WithField("existingVolume", fmt.Sprintf("%+v", existingVolume)).Debug("found existing volume")

	// Don't even attempt to put it on nodes that aren't available.
	if err := d.Assignments.NodeAvailable(ctx, req.GetNodeId()); err != nil {
		return nil, status.Errorf(codes.NotFound,
			"ControllerPublishVolume failed for %s on node %s: %v", req.GetVolumeId(), req.GetNodeId(), err)
	}

	// ReadWriteMany block volume
	rwxBlock := req.VolumeCapability.AccessMode.GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER && req.VolumeCapability.GetBlock() != nil

	devPath, err := d.Assignments.Attach(ctx, req.GetVolumeId(), req.GetNodeId(), rwxBlock)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerPublishVolume failed for %s: %v", req.GetVolumeId(), err)
	}

	if devPath == "" {
		return nil, status.Errorf(codes.Internal, "ControllerPublishVolume failed for %s: could not determine device path", req.GetVolumeId())
	}

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: (&PublishContext{
			DevicePath: devPath,
			FsType:     existingVolume.FsType,
		}).ToMap(),
	}, nil
}

// ControllerUnpublishVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllerunpublishvolume
func (d Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ControllerUnpublishVolume", req.GetVolumeId(), "VolumeId")
	}
	if req.GetNodeId() == "" {
		return nil, missingAttr("ControllerUnpublishVolume", req.GetNodeId(), "NodeId")
	}

	if err := d.Assignments.Detach(ctx, req.GetVolumeId(), req.GetNodeId()); err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerUnpublishVolume failed for %s: %v", req.GetVolumeId(), err)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#validatevolumecapabilities
func (d Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ValidateVolumeCapabilities", req.GetVolumeId(), "volumeId")
	}

	if req.GetVolumeCapabilities() == nil {
		return nil, missingAttr("ValidateVolumeCapabilities", req.GetVolumeId(), "VolumeCapabilities")
	}

	existingVolume, err := d.Storage.FindByID(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"ValidateVolumeCapabilities failed for %s: %v", req.GetVolumeId(), err)
	}
	if existingVolume == nil {
		return nil, status.Errorf(codes.NotFound,
			"ValidateVolumeCapabilities failed for %s: volume not present in storage backend", req.GetVolumeId())
	}
	d.log.WithFields(logrus.Fields{
		"existingVolume": fmt.Sprintf("%+v", existingVolume),
	}).Debug("found existing volume")

	_, err = validateCapabilities(req.GetVolumeCapabilities())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ValidateVolumeCapabilities failed to validate capabilities for %s: %v", req.GetVolumeId(), err)
	}

	_, err = volume.NewParameters(req.GetParameters(), d.topologyPrefix)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ValidateVolumeCapabilities failed to validate parameters for %s: %v", req.GetVolumeId(), err)
	}

	_, err = VolumeContextFromMap(req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ValidateVolumeCapabilities failed to validate volume context for %s: %v", req.GetVolumeId(), err)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
			VolumeContext:      req.GetVolumeContext(),
		},
	}, nil
}

// ListVolumes https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#listvolumes
func (d Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	volumes, err := d.Storage.ListAllWithStatus(ctx)
	if err != nil {
		return &csi.ListVolumesResponse{}, status.Errorf(codes.Aborted, "ListVolumes failed: %v", err)
	}

	start, err := parseAsInt(req.GetStartingToken())
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "ListVolumes failed for: %v", err)
	}

	if start > len(volumes) {
		return &csi.ListVolumesResponse{}, nil
	}

	// Handle pagination.
	var (
		end       int32
		nextToken string
	)
	totalVols := int32(len(volumes))
	if req.GetMaxEntries() == 0 || (totalVols <= req.GetMaxEntries()) {
		end = totalVols
	} else {
		end = req.GetMaxEntries()
		nextToken = strconv.Itoa(int(end))
	}

	volumes = volumes[start:end]

	// Build up entries list from paginated volume slice.
	entries := make([]*csi.ListVolumesResponse_Entry, len(volumes))
	for i, vol := range volumes {
		entries[i] = &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      vol.ID,
				CapacityBytes: vol.SizeBytes,
				// NB: Topology is specifically excluded here. For topology we would need the volume context, which
				// we don't have here. This might not be strictly to spec, but current consumers don't do anything with
				// the information, so it should be fine.
			},
			Status: &csi.ListVolumesResponse_VolumeStatus{
				PublishedNodeIds: vol.Nodes,
				VolumeCondition:  vol.Conditions,
			},
		}
	}

	return &csi.ListVolumesResponse{NextToken: nextToken, Entries: entries}, nil
}

// GetCapacity https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#getcapacity
func (d Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	params, err := volume.NewParameters(req.GetParameters(), d.topologyPrefix)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parameters: %v", err)
	}

	d.log.WithFields(logrus.Fields{
		"parameters": params,
		"topology":   req.GetAccessibleTopology(),
	}).Debug("got capacity request")

	// Get the labels for nodes we are allowed to "share" per remote access policy.
	accessibleSegments := params.AllowRemoteVolumeAccess.AccessibleSegments(req.GetAccessibleTopology().GetSegments())

	d.log.WithField("accessible", accessibleSegments).Trace("got accessible segments for parameters")

	maxCap := int64(0)

	for _, segment := range accessibleSegments {
		d.log.WithField("segment", segment).Debug("Checking capacity of segment")

		bytes, err := d.Storage.CapacityBytes(ctx, params.StoragePools, params.OverProvision, segment)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}

		if bytes > maxCap {
			maxCap = bytes
		}
	}

	return &csi.GetCapacityResponse{AvailableCapacity: maxCap}, nil
}

// ControllerGetCapabilities https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllergetcapabilities
func (d Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			// Tell the CO we can create and delete volumes.
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				},
			}},
			// Tell the CO we can make volumes available on remote nodes.
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				},
			}},
			// Tell the CO we can list volumes.
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				},
			}},
			// Tell the CO we can create and delete snapshots.
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				},
			}},
			// Tell the CO we can create clones of volumes.
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
				},
			}},
			// Tell the CO we can list snapshots.
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
				},
			}},
			// Tell the CO we can query our storage space.
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_CAPACITY,
				},
			}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				},
			}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_VOLUME,
				},
			}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES,
				},
			}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
				},
			}},
			{Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
				},
			}},
		},
	}, nil
}

// CreateSnapshot https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#createsnapshot
func (d Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.GetSourceVolumeId() == "" {
		return nil, missingAttr("CreateSnapshot", req.GetSourceVolumeId(), "SourceVolumeId")
	}
	if req.GetName() == "" {
		return nil, missingAttr("CreateSnapshot", req.GetName(), "Name")
	}

	d.log.WithField("req.parameters", req.GetParameters()).Debug("parsing request")

	params, err := volume.NewSnapshotParameters(req.GetParameters(), req.GetSecrets())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid snapshot parameters: %v", err)
	}

	d.log.WithField("params", params).Debug("got snapshot parameters")

	id := d.Snapshots.CompatibleSnapshotId(req.GetName())

	d.log.WithField("snapshot id", id).Debug("using snapshot id")

	existingSnap, ok, err := d.Snapshots.FindSnapByID(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check for existing snapshot: %v", err)
	}

	if !ok {
		d.log.Debug("existing snapshot is in failed state, deleting")
		err := d.Snapshots.SnapDelete(ctx, &volume.Snapshot{
			Snapshot: csi.Snapshot{
				SourceVolumeId: req.GetSourceVolumeId(),
				SnapshotId:     req.GetName(),
			},
			Remote: params.RemoteName,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "tried deleting a leftover unsuccessful snapshot")
		}
	}

	if existingSnap != nil {
		// Needed for idempotency.
		d.log.WithFields(logrus.Fields{
			"requestedSnapshotName":         req.GetName(),
			"requestedSnapshotSourceVolume": req.GetSourceVolumeId(),
			"existingSnapshot":              fmt.Sprintf("%+v", existingSnap),
		}).Debug("found existing snapshot")

		if existingSnap.GetSourceVolumeId() == req.GetSourceVolumeId() {
			err = d.maybeDeleteLocalSnapshot(ctx, existingSnap, params)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to delete local snapshot: %s", err)
			}

			if existingSnap.ReadyToUse {
				d.log.WithField("snapshot id", id).Debug("snapshot ready, delete temporary ID mapping if it exists")

				err := d.Snapshots.DeleteTemporarySnapshotID(ctx, id, params)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "failed to delete temporary snapshot ID: %v", err)
				}
			}

			return &csi.CreateSnapshotResponse{Snapshot: &existingSnap.Snapshot}, nil
		}

		return nil, status.Errorf(codes.AlreadyExists, "can't use %q for snapshot name for volume %q, snapshot name is in use for volume %q",
			req.GetName(), req.GetSourceVolumeId(), existingSnap.GetSourceVolumeId())
	}

	snap, err := d.Snapshots.SnapCreate(ctx, id, req.GetSourceVolumeId(), params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}

	err = d.maybeDeleteLocalSnapshot(ctx, snap, params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete local snapshot: %s", err)
	}

	if snap.ReadyToUse {
		d.log.WithField("snapshot id", id).Debug("snapshot ready, delete temporary ID mapping if it exists")

		err := d.Snapshots.DeleteTemporarySnapshotID(ctx, id, params)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete temporary snapshot ID: %v", err)
		}
	}

	return &csi.CreateSnapshotResponse{Snapshot: &snap.Snapshot}, nil
}

// DeleteSnapshot https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#deletesnapshot
func (d Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, missingAttr("DeleteSnapshot", req.GetSnapshotId(), "SnapshotId")
	}

	snap, _, err := d.Snapshots.FindSnapByID(ctx, req.GetSnapshotId())
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

	if err := d.Snapshots.SnapDelete(ctx, snap); err != nil {
		return nil, status.Errorf(codes.Internal, "unable to delete snapshot %s: %v",
			req.GetSnapshotId(), err)
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#listsnapshots
func (d Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	limit := int(req.GetMaxEntries())
	start, err := parseAsInt(req.GetStartingToken())
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "Invalid starting token: %v", err)
	}

	var snapshots []*volume.Snapshot
	switch {
	// Handle case where a single snapshot is requested.
	case req.GetSnapshotId() != "":
		snap, _, err := d.Snapshots.FindSnapByID(ctx, req.GetSnapshotId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
		}

		if snap == nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		d.log.WithFields(logrus.Fields{
			"requestedSnapshot": req.GetSnapshotId(),
			"napshot":           fmt.Sprintf("%+v", snap),
		}).Debug("found single snapshot")

		snapshots = []*volume.Snapshot{snap}

		// Handle case where a single volumes snapshots are requested.
	case req.GetSourceVolumeId() != "":
		info, err := d.Storage.FindByID(ctx, req.GetSourceVolumeId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots for volume '%s': %v", req.GetSourceVolumeId(), err)
		}

		if info == nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		snaps, err := d.Snapshots.FindSnapsBySource(ctx, info, start, limit)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
		}

		snapshots = snaps

		// Regular list of all snapshots.
	default:
		snaps, err := d.Snapshots.ListSnaps(ctx, start, limit)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
		}
		snapshots = snaps
	}

	nextToken := ""
	if limit > 0 && len(snapshots) == limit {
		nextToken = fmt.Sprintf("%d", start+limit)
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, len(snapshots))
	for i, snap := range snapshots {
		entries[i] = &csi.ListSnapshotsResponse_Entry{Snapshot: &snap.Snapshot}
	}

	return &csi.ListSnapshotsResponse{Entries: entries, NextToken: nextToken}, nil
}

// NodeExpandVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodeexpandvolume
func (d Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	d.log.WithFields(logrus.Fields{
		"NodeExpandVolume": fmt.Sprintf("%+v", req),
	}).Debug("Node expand volume")

	if req.GetVolumeId() == "" {
		return nil, missingAttr("NodeExpandVolume", req.GetVolumeId(), "VolumeId")
	}

	if req.GetVolumePath() == "" {
		return nil, missingAttr("NodeExpandVolume", req.GetVolumeId(), "TargetPath")
	}

	err := d.Expander.NodeExpand(req.GetVolumePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "NodePublishVolume failed for %s: mount not found", req.GetVolumeId())
		}

		return nil, status.Errorf(codes.Internal, "NodeExpandVolume - expand volume failed for target %s, err: %v", req.GetVolumePath(), err)
	}

	return &csi.NodeExpandVolumeResponse{}, nil
}

// ControllerExpandVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllerexpandvolume
func (d Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ControllerExpandVolume", req.GetVolumeId(), "VolumeId")
	}

	existingVolume, err := d.Storage.FindByID(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ControllerExpandVolume - get resource-definitions %s failed: %v", req.GetVolumeId(), err)
	}
	if existingVolume == nil {
		// Volume doesn't exist, and that's the point of this call after all.
		return nil, status.Errorf(codes.Internal,
			"ControllerExpandVolume - resource-definitions %s not found", req.GetVolumeId())
	}
	d.log.WithFields(logrus.Fields{
		"existingVolume": fmt.Sprintf("%+v", existingVolume),
	}).Debug("found existing volume")

	requiredKiB, err := d.Storage.AllocationSizeKiB(req.CapacityRange.GetRequiredBytes(), req.CapacityRange.GetLimitBytes(), existingVolume.FsType)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ControllerExpandVolume - expand volume failed for volume id %s: %v", req.GetVolumeId(), err)
	}
	volumeSize := data.NewKibiByte(data.KiB * data.ByteSize(requiredKiB))
	existingVolume.SizeBytes = int64(volumeSize.InclusiveBytes())

	d.log.WithFields(logrus.Fields{
		"ControllerExpandVolume": fmt.Sprintf("%+v", req),
		"Size":                   volumeSize,
	}).Debug("controller expand volume")

	err = d.Expander.ControllerExpand(ctx, existingVolume)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerExpandVolume - expand volume failed for %v: %v", existingVolume, err)
	}

	isBlockMode := req.GetVolumeCapability().GetBlock() != nil

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes: existingVolume.SizeBytes, NodeExpansionRequired: !isBlockMode,
	}, nil
}

// ControllerGetVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllergetvolume
func (d Driver) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	vol, err := d.Storage.FindByID(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to find volume '%s': %v", req.GetVolumeId(), err)
	}

	if vol == nil {
		return nil, status.Errorf(codes.NotFound, "no volume '%s' found", req.GetVolumeId())
	}

	nodes, condition, err := d.Assignments.Status(ctx, vol.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to find nodes for volume '%s': %v", vol.ID, err)
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      vol.ID,
			CapacityBytes: vol.SizeBytes,
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			PublishedNodeIds: nodes,
			VolumeCondition:  condition,
		},
	}, nil
}

// ControllerModifyVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllermodifyvolume
func (d Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
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

	// type UnaryServerInterceptor func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (resp interface{}, err error)
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

	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	if d.kubeClient != nil && d.resyncAfter > 0 {
		err := ReconcileVolumeSnapshotClass(ctx, d.kubeClient, d.Snapshots, d.log, d.resyncAfter)
		if err != nil {
			return fmt.Errorf("failed to start volume snapshot class reconciler")
		}
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
	d.srv.GracefulStop()
	d.cancel()
	return nil
}

func (d Driver) createNewVolume(ctx context.Context, info *volume.Info, params *volume.Parameters, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	logger := d.log.WithFields(logrus.Fields{
		"volume": info.ID,
	})

	logger.WithFields(logrus.Fields{
		"size": info.SizeBytes,
	}).Debug("creating new volume")

	// We're cloning from a volume or snapshot.
	if req.GetVolumeContentSource() != nil {
		switch {
		case req.GetVolumeContentSource().GetSnapshot() != nil:
			snapshotID := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
			if snapshotID == "" {
				return nil, status.Errorf(codes.InvalidArgument,
					"CreateVolume failed for %s: empty snapshotId", req.GetName())
			}
			logger.Debugf("pre-populate volume from snapshot: %+v", snapshotID)

			snap, _, err := d.Snapshots.FindSnapByID(ctx, snapshotID)
			if err != nil {
				return nil, status.Errorf(
					codes.Internal, "CreateVolume failed for %s: %v", req.GetName(), err)
			}
			if snap == nil {
				return nil, status.Errorf(codes.NotFound,
					"CreateVolume failed for %s: snapshot not found in storage backend", req.GetName())
			}

			snapParams, err := d.maybeGetSnapshotParameters(ctx, snap)
			if err != nil {
				logger.WithError(err).Warn("failed to fetch snapshot parameters, continuing without it")

				snapParams = nil
			}

			if err := d.Snapshots.VolFromSnap(ctx, snap, info, params, snapParams, req.GetAccessibilityRequirements()); err != nil {
				d.failpathDelete(ctx, info.ID)
				return nil, status.Errorf(codes.Internal,
					"CreateVolume failed for %s: %v", req.GetName(), err)
			}
			// We're cloning from a whole volume.
		case req.GetVolumeContentSource().GetVolume() != nil:
			volumeId := req.GetVolumeContentSource().GetVolume().GetVolumeId()
			if volumeId == "" {
				return nil, status.Errorf(codes.InvalidArgument,
					"CreateVolume failed for %s: empty volumeId", req.GetName())
			}

			logger.Debugf("pre-populate volume from volume: %+v", volumeId)

			sourceVol, err := d.Storage.FindByID(ctx, volumeId)
			if err != nil {
				return nil, status.Errorf(codes.Internal,
					"CreateVolume failed for %s: %v", req.GetName(), err)
			}
			if sourceVol == nil {
				return nil, status.Errorf(codes.NotFound,
					"CreateVolume failed for %s: source volume not found in storage backend", req.GetName())
			}

			err = d.Storage.Clone(ctx, info, sourceVol, params, req.GetAccessibilityRequirements())
			if err != nil {
				d.failpathDelete(ctx, info.ID)
				return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: %v", info.ID, err)
			}
		default:
			return nil, status.Errorf(codes.InvalidArgument,
				"Unknown content source for %s: %v", info.ID, req.GetVolumeContentSource())
		}
		// Regular new volume.
	} else {
		err := d.Storage.Create(ctx, info, params, req.GetAccessibilityRequirements())
		if err != nil {
			d.failpathDelete(ctx, info.ID)
			return nil, status.Errorf(codes.Internal,
				"CreateVolume failed for %s: %v", info.ID, err)
		}
	}

	topos, err := d.Storage.AccessibleTopologies(ctx, info.ID, params)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "CreateVolume failed for %s: unable to determine volume topology: %v",
			info.ID, err)
	}

	volCtx, err := VolumeContextFromParameters(params).ToMap()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: %v", info.ID, err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           info.ID,
			ContentSource:      req.GetVolumeContentSource(),
			CapacityBytes:      info.SizeBytes,
			AccessibleTopology: topos,
			VolumeContext:      volCtx,
		},
	}, nil
}

func findMatchingSnapshotClassName(snap *volume.Snapshot, contents ...unstructured.Unstructured) string {
	for i := range contents {
		content := contents[i].Object
		if driver, _, _ := unstructured.NestedString(content, "spec", "driver"); driver != linstor.DriverName {
			continue
		}

		if handle, _, _ := unstructured.NestedString(content, "status", "snapshotHandle"); handle != snap.SnapshotId {
			continue
		}

		if readyToUse, _, _ := unstructured.NestedBool(content, "status", "readyToUse"); !readyToUse {
			continue
		}

		snapshotClass, _, _ := unstructured.NestedString(content, "spec", "volumeSnapshotClassName")

		return snapshotClass
	}

	return ""
}

func (d Driver) maybeGetSnapshotParameters(ctx context.Context, snap *volume.Snapshot) (*volume.SnapshotParameters, error) {
	if d.kubeClient == nil {
		return nil, nil
	}

	gv := schema.GroupVersion{Group: "snapshot.storage.k8s.io", Version: "v1"}
	contentGvr := gv.WithResource("volumesnapshotcontents")
	classGvr := gv.WithResource("volumesnapshotclasses")

	result, err := d.kubeClient.Resource(contentGvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch list of snapshot contents")
	}

	snapshotClassName := findMatchingSnapshotClassName(snap, result.Items...)
	if snapshotClassName == "" {
		return nil, fmt.Errorf("failed to determine snapshot class name")
	}

	class, err := d.kubeClient.Resource(classGvr).Get(ctx, snapshotClassName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch snapshot class: %w", err)
	}

	rawParams, _, err := unstructured.NestedStringMap(class.Object, "parameters")
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshot class: %w", err)
	}

	return volume.NewSnapshotParameters(rawParams, nil)
}

// maybeDeleteLocalSnapshot deletes the local portion of a snapshot according to their volume.SnapshotParameters.
// It will not delete a snapshot that is not ready, does not have a remote target, or where local deletion is disabled.
func (d Driver) maybeDeleteLocalSnapshot(ctx context.Context, snap *volume.Snapshot, params *volume.SnapshotParameters) error {
	if !snap.ReadyToUse {
		return nil
	}

	if params.Type == volume.SnapshotTypeInCluster {
		return nil
	}

	if !params.DeleteLocal {
		return nil
	}

	// Create a new, local only snapshot object (no remote set!), so we don't accidentally delete the remote backup
	return d.Snapshots.SnapDelete(ctx, &volume.Snapshot{Snapshot: snap.Snapshot})
}

func missingAttr(methodCall, volumeID, attr string) error {
	if volumeID == "" {
		volumeID = "an unknown volume"
	}
	return status.Errorf(codes.InvalidArgument,
		"%s failed for %s: it requires a %s and none was provided", methodCall, volumeID, attr)
}

func parseAsInt(s string) (int, error) {
	if s == "" {
		return 0, nil
	}

	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse starting token: %v", err)
	}
	return int(i), nil
}

// Returns the name of the snapshot used to populate data in volume-from-volume scenarios
func snapshotForVolumeName(name string) string {
	return fmt.Sprintf("for-%s", name)
}

// failpathDelete deletes volumes and logs if that fails. Mostly useful
// in error paths where we need to report the original error and not the
// possible error from trying to clean up from that original error.
func (d Driver) failpathDelete(ctx context.Context, volId string) {
	if err := d.Storage.Delete(ctx, volId); err != nil {
		d.log.WithFields(logrus.Fields{
			"volume": volId,
		}).WithError(err).Error("failed to clean up volume")
	}
}

func validateCapabilities(caps []*csi.VolumeCapability) (string, error) {
	var mountCaps, blockCaps []*csi.VolumeCapability

	for _, capability := range caps {
		if capability.GetMount() != nil {
			mountCaps = append(mountCaps, capability)
		} else {
			blockCaps = append(blockCaps, capability)
		}
	}

	if len(mountCaps) > 0 && len(blockCaps) > 0 {
		return "", fmt.Errorf("unsupported FileSystem and Block mode on the same volume")
	}

	if len(mountCaps) > 0 {
		fsType := ""

		for _, c := range mountCaps {
			fs := c.GetMount().GetFsType()
			if fs == "" {
				// Set default if non was given (sanity tests might complain otherwise)
				fs = "ext4"
			}

			if fsType == "" {
				fsType = fs
			}

			if fsType != fs {
				return "", fmt.Errorf("unsupported conflicting FS types: '%s' != '%s'", fsType, fs)
			}

			mode := c.GetAccessMode().GetMode()
			switch mode {
			case
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
				csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
			// These are all fine
			case csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER, csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
				return "", fmt.Errorf("unsupported access mode '%s' for FileSystem volumes", mode.String())
			default:
				return "", fmt.Errorf("unsupported access mode: '%s'", mode.String())
			}
		}

		return fsType, nil
	}

	if len(blockCaps) > 0 {
		// Nothing to check in this case, for block volumes we support all access modes
		return "", nil
	}

	return "", fmt.Errorf("unsupported volume without any capabilities")
}

const (
	ParameterCsiPvcName      = "csi.storage.k8s.io/pvc/name"
	ParameterCsiPvcNamespace = "csi.storage.k8s.io/pvc/namespace"
)
