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
	"slices"
	"strconv"
	"strings"

	lc "github.com/LINBIT/golinstor"
	"github.com/LINBIT/golinstor/devicelayerkind"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/piraeusdatastore/nri-volume-qos/pkg/meta"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/utils"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Version is set via ldflags configued in the Makefile.
var Version = "UNKNOWN"

// Driver fullfils CSI controller, node, and indentity server interfaces.
type Driver struct {
	linstorClient *client.Linstor
	nfsExporter   *NfsExporter
	log           *logrus.Entry
	version       string
	// name distingushes the driver from other drivers and is used to mark
	// volumes so that volumes provisioned by another driver are not interfered with.
	name string
	// nodeID is the hostname of the node where this plugin is running locally.
	nodeID string
	// topologyPrefix is the name
	topologyPrefix string
	// snapshotClient reads VolumeSnapshotClass/Content objects for snapshot-parameter lookups.
	snapshotClient dynamic.Interface
	// rwxBlockValidationClient, when set, enables KubeVirt VM ownership validation for RWX block volumes.
	rwxBlockValidationClient kubernetes.Interface
	// consistencyGroupClient, when set, places PVCs sharing a consistency-group label as volume numbers of one resource.
	consistencyGroupClient kubernetes.Interface

	// Embed for forward compatibility.
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
	csi.UnimplementedGroupControllerServer
}

// NewDriver builds up a driver, using the given LINSTOR client for all
// volume operations.
func NewDriver(linstorClient *client.Linstor, options ...func(*Driver) error) (*Driver, error) {
	d := &Driver{
		linstorClient:  linstorClient,
		name:           linstor.DriverName,
		version:        Version,
		nodeID:         "localhost",
		log:            logrus.NewEntry(logrus.New()),
		topologyPrefix: lc.NamespcAuxiliary,
	}

	d.log.Logger.SetOutput(ioutil.Discard)
	d.log.Logger.SetFormatter(&logrus.TextFormatter{})

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

// NodeID configures the driver node ID.
func NodeID(nodeID string) func(*Driver) error {
	return func(d *Driver) error {
		d.nodeID = nodeID
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

// SnapshotClient sets the client used to read VolumeSnapshotClass/Content objects when resolving
// snapshot parameters during snapshot operations.
func SnapshotClient(client dynamic.Interface) func(*Driver) error {
	return func(d *Driver) error {
		d.snapshotClient = client

		return nil
	}
}

// ConfigureConsistencyGroups enables consistency-group support: CreateVolume reads each PVC's group label and
// places grouped volumes in one shared resource. The client is used to read the PVCs.
func ConfigureConsistencyGroups(client kubernetes.Interface) func(*Driver) error {
	return func(d *Driver) error {
		d.consistencyGroupClient = client

		return nil
	}
}

func ConfigureRWX(cl kubernetes.Interface, namespace, reactorConfigMap string) func(*Driver) error {
	return func(d *Driver) error {
		d.nfsExporter = &NfsExporter{
			cl:               cl,
			namespace:        namespace,
			reactorConfigMap: reactorConfigMap,
			log:              d.log.WithField("component", "nfsExporter"),
		}

		return nil
	}
}

// EnableRWXBlockValidation enables the KubeVirt VM ownership validation for RWX block volumes.
// When enabled, the driver checks that multiple pods using the same RWX block volume belong to
// the same VM, guarding against misuse of allow-two-primaries. The client is used to read the
// PV/PVC objects.
func EnableRWXBlockValidation(client kubernetes.Interface) func(*Driver) error {
	return func(d *Driver) error {
		d.rwxBlockValidationClient = client
		return nil
	}
}

// GetPluginInfo https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#getplugininfo
func (d *Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          d.name,
		VendorVersion: d.version,
	}, nil
}

// GetPluginCapabilities https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#getplugincapabilities
func (d *Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
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
			{Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_GROUP_CONTROLLER_SERVICE,
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
func (d *Driver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// NodeStageVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodestagevolume
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// NodeUnstageVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodeunstagevolume
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// NodePublishVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodepublishvolume
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
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

	volId, err := volume.ParseVolumeId(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse volume id: %v", err)
	}

	if volCtx == nil {
		params, err := d.linstorClient.GetLegacyVolumeParameters(ctx, volId)
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
		assignment, err := d.linstorClient.FindAssignmentOnNode(ctx, volId, d.nodeID)
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

	err = d.linstorClient.Mount(ctx, publishCtx.DevicePath, req.GetTargetPath(), fsType, ro, volCtx.MountOptions)
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
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("NodeUnpublishVolume", req.GetVolumeId(), "VolumeId")
	}

	if req.GetTargetPath() == "" {
		return nil, missingAttr("NodeUnpublishVolume", req.GetVolumeId(), "TargetPath")
	}

	err := d.linstorClient.Unmount(req.GetTargetPath())
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodegetvolumestats
func (d *Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("NodeGetVolumeStats", req.GetVolumeId(), "VolumeId")
	}

	if req.GetVolumePath() == "" {
		return nil, missingAttr("NodeGetVolumeStats", req.GetVolumeId(), "VolumeId")
	}

	mounted, err := d.linstorClient.IsMountPoint(req.GetVolumePath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodeGetVolumeStats failed for %s: failed to check if path %v is mounted: %v", req.GetVolumeId(), req.GetVolumePath(), err)
	}

	if !mounted {
		return nil, status.Errorf(codes.NotFound, "NodeGetVolumeStats failed for %s: path %v is not mounted", req.GetVolumeId(), req.GetVolumePath())
	}

	stats, err := d.linstorClient.GetVolumeStats(req.GetVolumePath())
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
func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
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
func (d *Driver) NodeGetInfo(ctx context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	topology, err := d.linstorClient.GetNodeTopologies(ctx, d.nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve node topology: %w", err)
	}

	return &csi.NodeGetInfoResponse{
		NodeId:             d.nodeID,
		MaxVolumesPerNode:  1048576, // DRBD volumes per node limit.
		AccessibleTopology: topology,
	}, nil
}

// consistencyGroupForPVC returns the consistency group of a CreateVolume request from its PVC's label, or ""
// (ordinary volume) when the feature is off, no Kubernetes client is set, or the label is absent.
func (d *Driver) consistencyGroupForPVC(ctx context.Context, req *csi.CreateVolumeRequest) (string, error) {
	if d.consistencyGroupClient == nil {
		return "", nil
	}

	namespace := req.GetParameters()[ParameterCsiPvcNamespace]
	name := req.GetParameters()[ParameterCsiPvcName]

	if namespace == "" || name == "" {
		return "", nil
	}

	pvc, err := d.consistencyGroupClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("could not read PVC %s/%s for consistency-group label: %w", namespace, name, err)
	}

	return pvc.GetLabels()[linstor.ConsistencyGroupLabel], nil
}

// CreateVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#createvolume
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, missingAttr("CreateVolume", req.GetName(), "Name")
	}
	if req.GetVolumeCapabilities() == nil || len(req.GetVolumeCapabilities()) == 0 {
		return nil, missingAttr("ValidateVolumeCapabilities", req.GetName(), "VolumeCapabilities")
	}

	fsType, nfsExport, err := d.validateCapabilities(req.GetVolumeCapabilities())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateVolume failed for %s: %v", req.Name, err)
	}

	// Determine how much storage we need to actually allocate for a given number of bytes.
	requiredBytes, err := d.linstorClient.AllocationSize(req.GetCapacityRange().GetRequiredBytes(), req.GetCapacityRange().GetLimitBytes(), fsType)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "CreateVolume failed for %s: %v", req.Name, err)
	}

	volumeBytes := map[int]int64{
		0: requiredBytes,
	}

	params, err := volume.NewParameters(req.GetParameters(), d.topologyPrefix)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse parameters: %v", err)
	}

	if nfsExport {
		requiredBytes, err := d.linstorClient.AllocationSize(params.NfsRecoveryVolumeBytes, 0, fsType)
		if err != nil {
			return nil, status.Errorf(
				codes.Internal, "CreateVolume failed for %s: %v", req.Name, err)
		}

		volumeBytes[1] = requiredBytes
	}

	if len(params.LayerList) > 0 && params.LayerList[0] != devicelayerkind.Drbd {
		for _, c := range req.GetVolumeCapabilities() {
			if c.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
				return nil, status.Errorf(codes.InvalidArgument, "ReadWriteMany volumes require a DRBD layer")
			}
		}
	}

	pvcName := req.GetParameters()[ParameterCsiPvcName]
	pvcNamespace := req.GetParameters()[ParameterCsiPvcNamespace]

	// Consistency-group members follow a dedicated create path; unlabeled volumes take the ordinary one below.
	group, err := d.consistencyGroupForPVC(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: %v", req.GetName(), err)
	}

	if group != "" {
		return d.createConsistencyGroupVolume(ctx, req, &params, fsType, nfsExport, requiredBytes, pvcNamespace, group)
	}

	pvcNamespaceToUse := ""
	pvcNameToUse := ""
	if params.UsePvcName {
		pvcNamespaceToUse = pvcNamespace
		pvcNameToUse = pvcName
	}

	volId := d.linstorClient.CompatibleVolumeId(req.GetName(), pvcNamespaceToUse, pvcNameToUse)

	log := d.log.WithField("volume", volId)
	log.Infof("determined volume id for volume named '%s'", req.GetName())

	// Handle case were a volume of the same name is already present.
	existingVolume, err := d.linstorClient.FindByID(ctx, volId)
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

		for vnr, size := range volumeBytes {
			if existingVolume.DeviceBytes[vnr] != size {
				return nil, status.Errorf(codes.AlreadyExists,
					"CreateVolume failed for %s: volume already present, but size differs on volume %d (existing: %d, wanted: %d)",
					volId, vnr, existingVolume.DeviceBytes[vnr], size)
			}
		}

		if existingVolume.ResourceGroup != params.ResourceGroup {
			return nil, status.Errorf(codes.AlreadyExists,
				"CreateVolume failed for %s: volume already present, but resource group differs (existing: %s, wanted: %s)",
				volId, existingVolume.ResourceGroup, params.ResourceGroup)
		}

		log.Info("existing volume matches request")

		topos, err := d.linstorClient.AccessibleTopologies(ctx, existingVolume.ID, &params)
		if err != nil {
			return nil, status.Errorf(
				codes.Internal, "CreateVolume failed for %s: unable to determine volume topology: %v",
				volId, err)
		}

		volCtx, err := VolumeContextFromParameters(&params).ToMap()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: unable to encode volume context: %v", volId, err)
		}

		if nfsExport {
			export, err := d.nfsExporter.Export(ctx, existingVolume, &params)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: unable to export: %v", volId, err)
			}

			volCtx[NfsExport] = export.String()
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:           existingVolume.String(),
				CapacityBytes:      existingVolume.Size(),
				ContentSource:      req.GetVolumeContentSource(),
				AccessibleTopology: topos,
				VolumeContext:      volCtx,
			},
		}, nil
	}

	volumeProperties := map[string]string{
		linstor.PropertyProvisioningCompletedBy: "linstor-csi/" + Version,
	}
	if pvcName != "" && pvcNamespace != "" {
		volumeProperties[lc.NamespcAuxiliary+"/"+ParameterCsiPvcName] = pvcName
		volumeProperties[lc.NamespcAuxiliary+"/"+ParameterCsiPvcNamespace] = pvcNamespace
	}

	// Persist the IO QoS limits as auxiliary properties so ControllerPublishVolume can read them back.
	// The device path is per-node and therefore set at publish time, not here.
	for k, v := range params.IOQoSLimits.ToMap() {
		if k == meta.KeyDevice {
			continue
		}

		volumeProperties[lc.NamespcAuxiliary+"/"+k] = v
	}

	return d.createNewVolume(
		ctx,
		&volume.Info{
			ID:            volume.ID{ResourceName: volId, VolumeNumber: 0},
			DeviceBytes:   volumeBytes,
			ResourceGroup: params.ResourceGroup,
			FsType:        fsType,
			Properties:    volumeProperties,
		},
		&params,
		req,
		nfsExport,
	)
}

// createConsistencyGroupVolume provisions a CSI volume as one volume number of the shared resource derived
// from (namespace, group). Idempotency is keyed on the CSI volume name, so concurrent siblings don't interfere.
func (d *Driver) createConsistencyGroupVolume(ctx context.Context, req *csi.CreateVolumeRequest, params *volume.Parameters, fsType string, nfsExport bool, requiredBytes int64, namespace, group string) (*csi.CreateVolumeResponse, error) {
	csiName := req.GetName()

	if nfsExport {
		// RWX-over-NFS commandeers volume numbers within a resource, which collides with group membership.
		return nil, status.Errorf(codes.InvalidArgument,
			"CreateVolume failed for %s: consistency groups are not compatible with RWX-over-NFS volumes", csiName)
	}

	resource := volume.ConsistencyGroupResourceName(namespace, group)

	log := d.log.WithFields(logrus.Fields{"resource": resource, "group": group, "csiName": csiName})
	log.Info("creating consistency-group member volume")

	info := &volume.Info{
		ID:            volume.ID{ResourceName: resource},
		DeviceBytes:   map[int]int64{0: requiredBytes},
		ResourceGroup: params.ResourceGroup,
		FsType:        fsType,
		// Only resource-wide props go on the shared definition; per-member identity is the volume-definition tag.
		Properties: map[string]string{
			linstor.PropertyProvisioningCompletedBy:                   "linstor-csi/" + Version,
			lc.NamespcAuxiliary + "/" + linstor.ConsistencyGroupLabel: fmt.Sprintf("%s/%s", namespace, group),
		},
	}

	if req.GetVolumeContentSource() != nil {
		if err := d.restoreConsistencyGroupVolume(ctx, req, info, params, requiredBytes); err != nil {
			return nil, err
		}
	} else if err := d.linstorClient.CreateConsistencyGroupVolume(ctx, info, csiName, requiredBytes, params, req.GetAccessibilityRequirements()); err != nil {
		// A resource-group conflict is a caller error; anything else is internal.
		var rgConflict *client.ResourceGroupConflictError
		if errors.As(err, &rgConflict) {
			return nil, status.Errorf(codes.InvalidArgument, "CreateVolume failed for %s: all consistency-group members must use the same StorageClass: %v", csiName, err)
		}

		return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: %v", csiName, err)
	}

	topos, err := d.linstorClient.AccessibleTopologies(ctx, info.ID, params)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"CreateVolume failed for %s: unable to determine volume topology: %v", csiName, err)
	}

	volCtx, err := VolumeContextFromParameters(params).ToMap()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: unable to encode volume context: %v", csiName, err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           info.String(),
			CapacityBytes:      info.Size(),
			ContentSource:      req.GetVolumeContentSource(),
			AccessibleTopology: topos,
			VolumeContext:      volCtx,
		},
	}, nil
}

// restoreConsistencyGroupVolume populates a member from a group snapshot (the only supported source): it claims
// the volume number from its snapshot ID. Cloning a single volume into a group is rejected (restore is whole-resource).
func (d *Driver) restoreConsistencyGroupVolume(ctx context.Context, req *csi.CreateVolumeRequest, info *volume.Info, params *volume.Parameters, requiredBytes int64) error {
	snapSource := req.GetVolumeContentSource().GetSnapshot()
	if snapSource == nil {
		return status.Errorf(codes.InvalidArgument,
			"CreateVolume failed for %s: cloning a volume into a consistency group is not supported; restore from a group snapshot instead", req.GetName())
	}

	snapshotID := snapSource.GetSnapshotId()
	if snapshotID == "" {
		return status.Errorf(codes.InvalidArgument, "CreateVolume failed for %s: empty snapshotId", req.GetName())
	}

	snapID, err := volume.ParseSnapshotId(snapshotID)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "CreateVolume failed for %s: %v", req.GetName(), err)
	}

	snaps, err := d.linstorClient.FindSnapsByID(ctx, snapshotID)
	if err != nil {
		return status.Errorf(codes.Internal, "CreateVolume failed for %s: %v", req.GetName(), err)
	}

	if len(snaps) == 0 {
		return status.Errorf(codes.NotFound,
			"CreateVolume failed for %s: snapshot '%s' not found in storage backend", req.GetName(), snapshotID)
	} else if len(snaps) >= 2 {
		return status.Errorf(codes.NotFound,
			"CreateVolume failed for %s: found multiple snapshots matching ID '%s'", req.GetName(), snapshotID)
	}

	// Claim the source volume number from the snapshot ID - the join key across the snapshot boundary.
	info.VolumeNumber = snapID.VolumeNumber
	info.DeviceBytes = map[int]int64{snapID.VolumeNumber: requiredBytes}

	snapParams, err := d.maybeGetSnapshotParameters(ctx, snapshotID)
	if err != nil {
		d.log.WithError(err).Warn("failed to fetch snapshot parameters, continuing without it")

		snapParams = nil
	}

	if err := d.linstorClient.RestoreConsistencyGroupVolume(ctx, snaps[0], info, req.GetName(), params, snapParams, req.GetAccessibilityRequirements()); err != nil {
		// A resource-group conflict is a caller error; anything else is internal.
		var rgConflict *client.ResourceGroupConflictError
		if errors.As(err, &rgConflict) {
			return status.Errorf(codes.InvalidArgument, "CreateVolume failed for %s: all consistency-group members must use the same StorageClass: %v", req.GetName(), err)
		}

		return status.Errorf(codes.Internal, "CreateVolume failed for %s: %v", req.GetName(), err)
	}

	return nil
}

// DeleteVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#deletevolume
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("DeleteVolume", req.GetVolumeId(), "VolumeId")
	}

	volId, err := volume.ParseVolumeId(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse volume id: %v", err)
	}

	err = d.nfsExporter.Unexport(ctx, volId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete NFS export for %s: %v", req.GetVolumeId(), err)
	}

	err = d.linstorClient.Delete(ctx, volId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllerpublishvolume
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
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
	existingVolume, err := d.linstorClient.FindByID(ctx, req.GetVolumeId())
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

	if export, ok := req.GetVolumeContext()[NfsExport]; ok {
		return &csi.ControllerPublishVolumeResponse{
			PublishContext: (&PublishContext{
				DevicePath: export,
				FsType:     "nfs",
			}).ToMap(),
		}, nil
	}

	// Don't even attempt to put it on nodes that aren't available.
	if err := d.linstorClient.NodeAvailable(ctx, req.GetNodeId()); err != nil {
		return nil, status.Errorf(codes.NotFound,
			"ControllerPublishVolume failed for %s on node %s: %v", req.GetVolumeId(), req.GetNodeId(), err)
	}

	// ReadWriteMany block volume
	rwxBlock := req.VolumeCapability.AccessMode.GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER && req.VolumeCapability.GetBlock() != nil

	// Validate RWX block attachment to prevent misuse of allow-two-primaries
	if rwxBlock && d.rwxBlockValidationClient != nil {
		if _, err := utils.ValidateRWXBlockAttachment(ctx, d.rwxBlockValidationClient, d.log, req.GetVolumeId()); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition,
				"ControllerPublishVolume failed for %s: %v", req.GetVolumeId(), err)
		}
	}

	volId, err := volume.ParseVolumeId(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse volume id: %v", err)
	}

	devPath, err := d.linstorClient.Attach(ctx, volId, req.GetNodeId(), rwxBlock)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerPublishVolume failed for %s: %v", req.GetVolumeId(), err)
	}

	if devPath == "" {
		return nil, status.Errorf(codes.Internal, "ControllerPublishVolume failed for %s: could not determine device path", req.GetVolumeId())
	}

	// Recover the IO QoS limits stored as auxiliary properties at create time. meta.ParseLimits only
	// reads the qos.linbit.com/* limit keys and ignores everything else, so we strip the Aux/ prefix
	// off all auxiliary properties and let it pick out the relevant ones.
	auxPrefix := lc.NamespcAuxiliary + "/"
	qosParams := make(map[string]string)

	for k, v := range existingVolume.Properties {
		if rest, ok := strings.CutPrefix(k, auxPrefix); ok {
			qosParams[rest] = v
		}
	}

	limits, err := meta.ParseLimits(qosParams)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerPublishVolume failed for %s: invalid stored IO QoS limits: %v", req.GetVolumeId(), err)
	}

	// Only attach the device, and thus emit QoS metadata, when limits are actually configured.
	if limits != (meta.Limits{}) {
		limits.Device = devPath
	}

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: (&PublishContext{
			DevicePath:  devPath,
			FsType:      existingVolume.FsType,
			IOQoSLimits: limits,
		}).ToMap(),
	}, nil
}

// ControllerUnpublishVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllerunpublishvolume
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ControllerUnpublishVolume", req.GetVolumeId(), "VolumeId")
	}
	if req.GetNodeId() == "" {
		return nil, missingAttr("ControllerUnpublishVolume", req.GetNodeId(), "NodeId")
	}

	volId, err := volume.ParseVolumeId(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse volume id: %v", err)
	}

	if err := d.linstorClient.Detach(ctx, volId, req.GetNodeId()); err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerUnpublishVolume failed for %s: %v", req.GetVolumeId(), err)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#validatevolumecapabilities
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ValidateVolumeCapabilities", req.GetVolumeId(), "volumeId")
	}

	if req.GetVolumeCapabilities() == nil {
		return nil, missingAttr("ValidateVolumeCapabilities", req.GetVolumeId(), "VolumeCapabilities")
	}

	existingVolume, err := d.linstorClient.FindByID(ctx, req.GetVolumeId())
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

	_, _, err = d.validateCapabilities(req.GetVolumeCapabilities())
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
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	volumes, err := d.linstorClient.ListAllWithStatus(ctx)
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
				VolumeId:      vol.String(),
				CapacityBytes: vol.Size(),
				// NB: Topology is specifically excluded here. For topology we would need the volume context, which
				// we don't have here. This might not be strictly to spec, but current consumers don't do anything with
				// the information, so it should be fine.
			},
			Status: &csi.ListVolumesResponse_VolumeStatus{
				VolumeCondition: vol.Conditions,
			},
		}
	}

	return &csi.ListVolumesResponse{NextToken: nextToken, Entries: entries}, nil
}

// GetCapacity https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#getcapacity
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
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

		bytes, err := d.linstorClient.CapacityBytes(ctx, params.StoragePools, params.OverProvision, segment)
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
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
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
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	// Delegate to the group snapshot implementation
	resp, err := d.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
		Name:            req.GetName(),
		Parameters:      req.GetParameters(),
		Secrets:         req.GetSecrets(),
		SourceVolumeIds: []string{req.GetSourceVolumeId()},
	})
	if err != nil {
		return nil, err
	}

	if resp == nil || resp.GroupSnapshot == nil || len(resp.GroupSnapshot.Snapshots) != 1 {
		return nil, status.Errorf(codes.Internal, "got unexpected number of snapshots: %v", resp)
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: resp.GroupSnapshot.Snapshots[0],
	}, nil
}

// DeleteSnapshot https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#deletesnapshot
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, missingAttr("DeleteSnapshot", req.GetSnapshotId(), "SnapshotId")
	}

	snaps, err := d.linstorClient.FindSnapsByID(ctx, req.GetSnapshotId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to find snapshot %s: %v",
			req.GetSnapshotId(), err)
	}

	if len(snaps) > 1 {
		return nil, status.Errorf(codes.Internal, "snapshot ID '%s' matches multiple snapshots: %v", req.GetSnapshotId(), snaps)
	}

	for _, snap := range snaps {
		// A group snapshot is one shared LINSTOR snapshot; refuse per-member delete (use DeleteVolumeGroupSnapshot).
		if snap.PartOfConsistencyGroup {
			return nil, status.Errorf(codes.FailedPrecondition,
				"snapshot %s is part of a consistency group; delete it with DeleteVolumeGroupSnapshot", req.GetSnapshotId())
		}

		if err := d.linstorClient.SnapDelete(ctx, &snap.SnapshotId); err != nil {
			return nil, status.Errorf(codes.Internal, "unable to delete snapshot %s: %v",
				req.GetSnapshotId(), err)
		}
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#listsnapshots
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	limit := int(req.GetMaxEntries())
	start, err := parseAsInt(req.GetStartingToken())
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "Invalid starting token: %v", err)
	}

	var snapshots []*volume.Snapshot
	switch {
	// Handle case where a single snapshot is requested.
	case req.GetSnapshotId() != "":
		snaps, err := d.linstorClient.FindSnapsByID(ctx, req.GetSnapshotId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
		}

		snapshots = snaps

		// Handle case where a single volumes snapshots are requested.
	case req.GetSourceVolumeId() != "":
		info, err := d.linstorClient.FindByID(ctx, req.GetSourceVolumeId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots for volume '%s': %v", req.GetSourceVolumeId(), err)
		}

		if info == nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		snaps, err := d.linstorClient.FindSnapsBySource(ctx, info, start, limit)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
		}

		snapshots = snaps

		// Regular list of all snapshots.
	default:
		snaps, err := d.linstorClient.ListSnaps(ctx, start, limit)
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
		entries[i] = &csi.ListSnapshotsResponse_Entry{Snapshot: &csi.Snapshot{
			SnapshotId:     snap.String(),
			SourceVolumeId: snap.Source().String(),
			CreationTime:   timestamppb.New(snap.CreationTime),
			SizeBytes:      snap.SizeBytes,
			ReadyToUse:     snap.ReadyToUse,
		}}
	}

	return &csi.ListSnapshotsResponse{Entries: entries, NextToken: nextToken}, nil
}

// NodeExpandVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#nodeexpandvolume
func (d *Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	d.log.WithFields(logrus.Fields{
		"NodeExpandVolume": fmt.Sprintf("%+v", req),
	}).Debug("Node expand volume")

	if req.GetVolumeId() == "" {
		return nil, missingAttr("NodeExpandVolume", req.GetVolumeId(), "VolumeId")
	}

	if req.GetVolumePath() == "" {
		return nil, missingAttr("NodeExpandVolume", req.GetVolumeId(), "TargetPath")
	}

	err := d.linstorClient.NodeExpand(req.GetVolumePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "NodePublishVolume failed for %s: mount not found", req.GetVolumeId())
		}

		return nil, status.Errorf(codes.Internal, "NodeExpandVolume - expand volume failed for target %s, err: %v", req.GetVolumePath(), err)
	}

	return &csi.NodeExpandVolumeResponse{}, nil
}

// ControllerExpandVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllerexpandvolume
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, missingAttr("ControllerExpandVolume", req.GetVolumeId(), "VolumeId")
	}

	existingVolume, err := d.linstorClient.FindByID(ctx, req.GetVolumeId())
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

	requiredBytes, err := d.linstorClient.AllocationSize(req.CapacityRange.GetRequiredBytes(), req.CapacityRange.GetLimitBytes(), existingVolume.FsType)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ControllerExpandVolume - expand volume failed for volume id %s: %v", req.GetVolumeId(), err)
	}

	existingVolume.DeviceBytes[existingVolume.VolumeNumber] = requiredBytes

	d.log.WithFields(logrus.Fields{
		"ControllerExpandVolume": fmt.Sprintf("%+v", req),
		"Size":                   requiredBytes,
	}).Debug("controller expand volume")

	err = d.linstorClient.ControllerExpand(ctx, existingVolume)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"ControllerExpandVolume - expand volume failed for %v: %v", existingVolume, err)
	}

	isBlockMode := req.GetVolumeCapability().GetBlock() != nil
	isRwx := req.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER ||
		req.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes: existingVolume.Size(),
		// Skip node expansion for:
		// * block volumes: normal LINSTOR resize is enough
		// * NFS export, handled by the NFS exporter
		NodeExpansionRequired: !isBlockMode && !isRwx,
	}, nil
}

// ControllerGetVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllergetvolume
func (d *Driver) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	vol, err := d.linstorClient.FindByID(ctx, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to find volume '%s': %v", req.GetVolumeId(), err)
	}

	if vol == nil {
		return nil, status.Errorf(codes.NotFound, "no volume '%s' found", req.GetVolumeId())
	}

	condition, err := d.linstorClient.Status(ctx, vol.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to find nodes for volume '%s': %v", vol.ID, err)
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      vol.String(),
			CapacityBytes: vol.Size(),
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: condition,
		},
	}, nil
}

// ControllerModifyVolume https://github.com/container-storage-interface/spec/blob/v1.9.0/spec.md#controllermodifyvolume
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// GroupControllerGetCapabilities https://github.com/container-storage-interface/spec/blob/v1.11.0/spec.md#groupcontrollergetcapabilities
func (d *Driver) GroupControllerGetCapabilities(ctx context.Context, in *csi.GroupControllerGetCapabilitiesRequest) (*csi.GroupControllerGetCapabilitiesResponse, error) {
	return &csi.GroupControllerGetCapabilitiesResponse{
		Capabilities: []*csi.GroupControllerServiceCapability{
			{
				Type: &csi.GroupControllerServiceCapability_Rpc{
					Rpc: &csi.GroupControllerServiceCapability_RPC{
						Type: csi.GroupControllerServiceCapability_RPC_CREATE_DELETE_GET_VOLUME_GROUP_SNAPSHOT,
					},
				},
			},
		},
	}, nil
}

// CreateVolumeGroupSnapshot https://github.com/container-storage-interface/spec/blob/v1.11.0/spec.md#createvolumegroupsnapshot
func (d *Driver) CreateVolumeGroupSnapshot(ctx context.Context, req *csi.CreateVolumeGroupSnapshotRequest) (*csi.CreateVolumeGroupSnapshotResponse, error) {
	if req.GetName() == "" {
		return nil, missingAttr("CreateVolumeGroupSnapshot", req.GetName(), "Name")
	}

	if len(req.GetSourceVolumeIds()) == 0 {
		return nil, missingAttr("CreateVolumeGroupSnapshot", req.GetName(), "SourceVolumeIds")
	}

	if slices.ContainsFunc(req.GetSourceVolumeIds(), func(s string) bool {
		return s == ""
	}) {
		return nil, missingAttr("CreateVolumeGroupSnapshot", req.GetName(), "SourceVolumeIds")
	}

	d.log.WithField("req.parameters", req.GetParameters()).Debug("parsing request")

	params, err := volume.NewSnapshotParameters(req.GetParameters(), req.GetSecrets())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid snapshot parameters: %v", err)
	}

	d.log.WithField("params", params).Debug("got snapshot parameters")

	id := d.linstorClient.CompatibleSnapshotId(req.GetName())

	d.log.WithField("snapshot id", id).Debug("using snapshot id")

	sources := make([]volume.ID, 0, len(req.GetSourceVolumeIds()))

	for _, sourceID := range req.GetSourceVolumeIds() {
		volId, err := volume.ParseVolumeId(sourceID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to parse volume id: %v", err)
		}

		sources = append(sources, volId)
	}

	existingSnaps, err := d.linstorClient.FindSnapsByID(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check for existing snapshot: %v", err)
	}

	if len(existingSnaps) > 0 {
		d.log.WithField("existing snapshots", existingSnaps).Debug("found existing snapshots")

		existingSources := sets.New[volume.ID]()
		for _, snap := range existingSnaps {
			existingSources.Insert(snap.Source())
		}

		expectedSources := sets.New(sources...)

		if !existingSources.Equal(expectedSources) {
			return nil, status.Errorf(codes.AlreadyExists, "snapshot %s already exists, but differs on snapshot volumes: %v != %v", id, expectedSources, existingSnaps)
		}

		d.log.Debug("existing snapshots match expected sources")
	}

	if slices.ContainsFunc(existingSnaps, func(s *volume.Snapshot) bool {
		return s.Failed
	}) {
		d.log.Debug("existing snapshots have a failed snapshot, deleting to start a retry")

		var errs []error

		for _, snap := range existingSnaps {
			errs = append(errs, d.linstorClient.SnapDelete(ctx, &snap.SnapshotId))
		}

		if err := errors.Join(errs...); err != nil {
			return nil, fmt.Errorf("failed to clean up failed, incomplete snapshots: %w", err)
		}

		// Now safe to retry creation of the snapshot
	}

	if len(existingSnaps) == 0 {
		existingSnaps, err = d.linstorClient.SnapCreate(ctx, id, params, sources...)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
		}
	}

	groupSnap := AggregateGroupSnapshot(id, existingSnaps...)

	for _, snap := range existingSnaps {
		err = d.maybeDeleteLocalSnapshot(ctx, snap, params)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete local snapshot: %s", err)
		}

		if groupSnap.ReadyToUse {
			d.log.WithField("snapshot id", id).Debug("snapshot ready, delete temporary ID mapping if it exists")

			err := d.linstorClient.DeleteTemporarySnapshotID(ctx, id, params)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to delete temporary snapshot ID: %v", err)
			}
		}
	}

	return &csi.CreateVolumeGroupSnapshotResponse{GroupSnapshot: groupSnap}, nil
}

// DeleteVolumeGroupSnapshot https://github.com/container-storage-interface/spec/blob/v1.11.0/spec.md#deletevolumegroupsnapshot
func (d *Driver) DeleteVolumeGroupSnapshot(ctx context.Context, req *csi.DeleteVolumeGroupSnapshotRequest) (*csi.DeleteVolumeGroupSnapshotResponse, error) {
	if req.GetGroupSnapshotId() == "" {
		return nil, missingAttr("DeleteVolumeGroupSnapshot", req.GetGroupSnapshotId(), "GroupSnapshotId")
	}

	snaps, err := d.linstorClient.FindSnapsByID(ctx, req.GetGroupSnapshotId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots by group ID: %s", err)
	}

	for _, snap := range snaps {
		if err := d.linstorClient.SnapDelete(ctx, &snap.SnapshotId); err != nil {
			return nil, status.Errorf(codes.Internal, "unable to delete snapshot %s: %v", snap, err)
		}
	}

	return &csi.DeleteVolumeGroupSnapshotResponse{}, nil
}

// GetVolumeGroupSnapshot https://github.com/container-storage-interface/spec/blob/v1.11.0/spec.md#getvolumegroupsnapshot
func (d *Driver) GetVolumeGroupSnapshot(ctx context.Context, req *csi.GetVolumeGroupSnapshotRequest) (*csi.GetVolumeGroupSnapshotResponse, error) {
	if req.GetGroupSnapshotId() == "" {
		return nil, missingAttr("GetVolumeGroupSnapshot", req.GetGroupSnapshotId(), "GroupSnapshotId")
	}

	snaps, err := d.linstorClient.FindSnapsByID(ctx, req.GetGroupSnapshotId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots by group ID: %v", err)
	}

	if len(snaps) == 0 {
		return nil, status.Errorf(codes.NotFound, "snapshot %s not found", req.GetGroupSnapshotId())
	}

	return &csi.GetVolumeGroupSnapshotResponse{GroupSnapshot: AggregateGroupSnapshot(req.GetGroupSnapshotId(), snaps...)}, nil
}

func AggregateGroupSnapshot(id string, snapshots ...*volume.Snapshot) *csi.VolumeGroupSnapshot {
	creationTime := slices.MinFunc(snapshots, func(a, b *volume.Snapshot) int {
		return a.CreationTime.Compare(b.CreationTime)
	}).CreationTime

	allReady := true
	csiSnapshots := make([]*csi.Snapshot, 0, len(snapshots))

	for _, snap := range snapshots {
		if !snap.ReadyToUse {
			allReady = false
		}

		// CG members share one snapshot and can't be deleted independently, so bind them via GroupSnapshotId;
		// members of an ordinary group snapshot own their snapshot and stay unbound (individually deletable).
		groupSnapshotID := ""
		if snap.PartOfConsistencyGroup {
			groupSnapshotID = id
		}

		csiSnapshots = append(csiSnapshots, &csi.Snapshot{
			SnapshotId:      snap.String(),
			SourceVolumeId:  snap.Source().String(),
			GroupSnapshotId: groupSnapshotID,
			CreationTime:    timestamppb.New(snap.CreationTime),
			ReadyToUse:      snap.ReadyToUse,
			SizeBytes:       snap.SizeBytes,
		})
	}

	return &csi.VolumeGroupSnapshot{
		GroupSnapshotId: id,
		CreationTime:    timestamppb.New(creationTime),
		ReadyToUse:      allReady,
		Snapshots:       csiSnapshots,
	}
}

// Listen creates a listener for the given CSI endpoint, which must be a unix
// domain socket URL. A stale socket file from a previous run is removed first.
// Binding before Serve runs avoids racing clients against server startup.
func Listen(endpoint string) (net.Listener, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to parse address: %w", err)
	}

	addr := path.Join(u.Host, filepath.FromSlash(u.Path))
	if u.Host == "" {
		addr = filepath.FromSlash(u.Path)
	}

	// csi plugins talk only over unix sockets currently
	if u.Scheme != "unix" {
		return nil, fmt.Errorf("currently only unix domain sockets are supported, have: %s", u.Scheme)
	}
	if err = os.Remove(addr); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove previously used unix domain socket file %s, error: %w", addr, err)
	}

	return net.Listen(u.Scheme, addr)
}

// Serve handles CSI requests on listener, blocking until ctx is cancelled (the
// server is then stopped gracefully) or a fatal error occurs.
func (d *Driver) Serve(ctx context.Context, listener net.Listener) error {
	d.log.Debug("Preparing to start server")

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

	srv := grpc.NewServer(grpc.UnaryInterceptor(errHandler))
	csi.RegisterIdentityServer(srv, d)
	csi.RegisterControllerServer(srv, d)
	csi.RegisterNodeServer(srv, d)
	csi.RegisterGroupControllerServer(srv, d)

	// Stop the server gracefully once the caller cancels the context.
	stop := context.AfterFunc(ctx, srv.GracefulStop)
	defer stop()

	d.log.WithFields(logrus.Fields{
		"address": listener.Addr(),
	}).Info("server started")

	return srv.Serve(listener)
}

func (d *Driver) createNewVolume(ctx context.Context, info *volume.Info, params *volume.Parameters, req *csi.CreateVolumeRequest, nfsExport bool) (*csi.CreateVolumeResponse, error) {
	logger := d.log.WithFields(logrus.Fields{
		"volume": info.ID,
	})

	logger.WithFields(logrus.Fields{
		"size": info.Size(),
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

			snaps, err := d.linstorClient.FindSnapsByID(ctx, snapshotID)
			if err != nil {
				return nil, status.Errorf(
					codes.Internal, "CreateVolume failed for %s: %v", req.GetName(), err)
			}

			if len(snaps) == 0 {
				return nil, status.Errorf(codes.NotFound,
					"CreateVolume failed for %s: snapshot '%s' not found in storage backend", req.GetName(), snapshotID)
			}

			if len(snaps) > 1 {
				return nil, status.Errorf(codes.NotFound,
					"CreateVolume failed for %s: found multiple snapshots matching ID '%s'", req.GetName(), snapshotID)
			}

			snapParams, err := d.maybeGetSnapshotParameters(ctx, snapshotID)
			if err != nil {
				logger.WithError(err).Warn("failed to fetch snapshot parameters, continuing without it")

				snapParams = nil
			}

			if err := d.linstorClient.VolFromSnap(ctx, snaps[0], info, params, snapParams, req.GetAccessibilityRequirements()); err != nil {
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

			sourceVol, err := d.linstorClient.FindByID(ctx, volumeId)
			if err != nil {
				return nil, status.Errorf(codes.Internal,
					"CreateVolume failed for %s: %v", req.GetName(), err)
			}
			if sourceVol == nil {
				return nil, status.Errorf(codes.NotFound,
					"CreateVolume failed for %s: source volume not found in storage backend", req.GetName())
			}

			err = d.linstorClient.Clone(ctx, info, sourceVol, params, req.GetAccessibilityRequirements())
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
		err := d.linstorClient.Create(ctx, info, params, req.GetAccessibilityRequirements())
		if err != nil {
			d.failpathDelete(ctx, info.ID)
			return nil, status.Errorf(codes.Internal,
				"CreateVolume failed for %s: %v", info.ID, err)
		}
	}

	topos, err := d.linstorClient.AccessibleTopologies(ctx, info.ID, params)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal, "CreateVolume failed for %s: unable to determine volume topology: %v",
			info.ID, err)
	}

	volCtx, err := VolumeContextFromParameters(params).ToMap()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: %v", info.ID, err)
	}

	if nfsExport {
		export, err := d.nfsExporter.Export(ctx, info, params)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "CreateVolume failed for %s: unable to export: %v", info.ID, err)
		}

		volCtx[NfsExport] = export.String()
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           info.String(),
			ContentSource:      req.GetVolumeContentSource(),
			CapacityBytes:      info.Size(),
			AccessibleTopology: topos,
			VolumeContext:      volCtx,
		},
	}, nil
}

func findMatchingSnapshotClassName(snapId string, contents ...unstructured.Unstructured) string {
	for i := range contents {
		content := contents[i].Object
		if driver, _, _ := unstructured.NestedString(content, "spec", "driver"); driver != linstor.DriverName {
			continue
		}

		if handle, _, _ := unstructured.NestedString(content, "status", "snapshotHandle"); handle != snapId {
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

func (d *Driver) maybeGetSnapshotParameters(ctx context.Context, snapshotID string) (*volume.SnapshotParameters, error) {
	if d.snapshotClient == nil {
		return nil, nil
	}

	gv := schema.GroupVersion{Group: "snapshot.storage.k8s.io", Version: "v1"}
	contentGvr := gv.WithResource("volumesnapshotcontents")
	classGvr := gv.WithResource("volumesnapshotclasses")

	result, err := d.snapshotClient.Resource(contentGvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch list of snapshot contents")
	}

	snapshotClassName := findMatchingSnapshotClassName(snapshotID, result.Items...)
	if snapshotClassName == "" {
		return nil, fmt.Errorf("failed to determine snapshot class name")
	}

	class, err := d.snapshotClient.Resource(classGvr).Get(ctx, snapshotClassName, metav1.GetOptions{})
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
func (d *Driver) maybeDeleteLocalSnapshot(ctx context.Context, snap *volume.Snapshot, params *volume.SnapshotParameters) error {
	if !snap.ReadyToUse {
		return nil
	}

	if params.Type == volume.SnapshotTypeInCluster {
		return nil
	}

	if !params.DeleteLocal {
		return nil
	}

	// Create a new, local only snapshot ID (no remote set!), so we don't accidentally delete the remote backup
	return d.linstorClient.SnapDelete(ctx, &volume.SnapshotId{
		Type:         volume.SnapshotTypeInCluster,
		SnapshotName: snap.SnapshotName,
		SourceName:   snap.SourceName,
		VolumeNumber: snap.VolumeNumber,
	})
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

// failpathDelete deletes volumes and logs if that fails. Mostly useful
// in error paths where we need to report the original error and not the
// possible error from trying to clean up from that original error.
func (d *Driver) failpathDelete(ctx context.Context, volId volume.ID) {
	if err := d.linstorClient.Delete(ctx, volId); err != nil {
		d.log.WithFields(logrus.Fields{
			"volume": volId,
		}).WithError(err).Error("failed to clean up volume")
	}
}

// Validates the volume capabilities and returns:
// * The fsType for the volume, or "" for block volumes
// * If this volume should be exported via NFS or not
// * validation errors
func (d *Driver) validateCapabilities(caps []*csi.VolumeCapability) (string, bool, error) {
	var mountCaps, blockCaps []*csi.VolumeCapability

	for _, capability := range caps {
		if capability.GetMount() != nil {
			mountCaps = append(mountCaps, capability)
		} else {
			blockCaps = append(blockCaps, capability)
		}
	}

	if len(mountCaps) > 0 && len(blockCaps) > 0 {
		return "", false, fmt.Errorf("unsupported FileSystem and Block mode on the same volume")
	}

	if len(mountCaps) > 0 {
		fsType := ""
		nfsExport := false

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
				return "", false, fmt.Errorf("unsupported conflicting FS types: '%s' != '%s'", fsType, fs)
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
				if !d.nfsExporter.Enabled() {
					return "", false, fmt.Errorf("unsupported access mode '%s' for FileSystem volumes, NFS export not enabled", mode.String())
				}

				nfsExport = true
			default:
				return "", false, fmt.Errorf("unsupported access mode: '%s'", mode.String())
			}
		}

		return fsType, nfsExport, nil
	}

	if len(blockCaps) > 0 {
		// Nothing to check in this case, for block volumes we support all access modes
		return "", false, nil
	}

	return "", false, fmt.Errorf("unsupported volume without any capabilities")
}

const (
	ParameterCsiPvcName      = "csi.storage.k8s.io/pvc/name"
	ParameterCsiPvcNamespace = "csi.storage.k8s.io/pvc/namespace"
)
