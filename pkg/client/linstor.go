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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	lapiconsts "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/haySwim/data"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor/util"
	"github.com/piraeusdatastore/linstor-csi/pkg/slice"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/autoplace"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/autoplacetopology"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/balancer"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/followtopology"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/manual"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Linstor is a high-level client for use with CSI.
type Linstor struct {
	log            *logrus.Entry
	fallbackPrefix string
	client         *lc.HighLevelClient
	mounter        *mount.SafeFormatAndMount
	labelBySP      bool
}

// NewLinstor returns a high-level linstor client for CSI applications to interact with
// By default, it will try to connect with localhost:3370.
func NewLinstor(options ...func(*Linstor) error) (*Linstor, error) {
	// Set up zero values.
	c, err := lc.NewHighLevelClient()
	if err != nil {
		return nil, err
	}
	l := &Linstor{
		fallbackPrefix: "csi-",
		log:            logrus.NewEntry(logrus.New()),
		client:         c,
		labelBySP:      true,
	}

	l.client.PropertyNamespace = lapiconsts.NamespcAuxiliary

	// run all option functions.
	for _, opt := range options {
		err := opt(l)
		if err != nil {
			return nil, err
		}
	}

	// Add in fields that may have been configured above.
	l.log = l.log.WithFields(logrus.Fields{
		"linstorCSIComponent": "client",
	})

	l.mounter = &mount.SafeFormatAndMount{
		Interface: mount.New("/bin/mount"),
		Exec:      utilexec.New(),
	}

	l.log.WithFields(logrus.Fields{
		"APIClient":       fmt.Sprintf("%+v", l.client),
		"highLevelClient": fmt.Sprintf("%+v", l),
	}).Debug("generated new linstor client")

	return l, nil
}

// APIClient the configured LINSTOR API client that will be used to communicate
// with the LINSTOR cluster.
func APIClient(c *lc.HighLevelClient) func(*Linstor) error {
	return func(l *Linstor) error {
		cp := l.client
		*cp = *c
		return nil
	}
}

// LogOut sets the Linstor client to write logs to the provided io.Writer
// instead of discarding logs.
func LogOut(out io.Writer) func(*Linstor) error {
	return func(l *Linstor) error {
		l.log.Logger.SetOutput(out)
		return nil
	}
}

// LogFmt sets the format of the log outpout via the provided logrus.Formatter.
func LogFmt(fmt logrus.Formatter) func(*Linstor) error {
	return func(l *Linstor) error {
		l.log.Logger.SetFormatter(fmt)
		return nil
	}
}

func PropertyNamespace(ns string) func(*Linstor) error {
	return func(l *Linstor) error {
		l.client.PropertyNamespace = ns

		return nil
	}
}

func LabelBySP(b bool) func(*Linstor) error {
	return func(l *Linstor) error {
		l.labelBySP = b

		return nil
	}
}

// LogLevel sets the logging intensity. Debug additionally reports the function
// from which the logger was called.
func LogLevel(s string) func(*Linstor) error {
	return func(l *Linstor) error {
		level, err := logrus.ParseLevel(s)
		if err != nil {
			return fmt.Errorf("unable to use %s as a logging level: %v", s, err)
		}

		l.log.Logger.SetLevel(level)

		// Report function names on debug
		if level == logrus.DebugLevel {
			l.log.Logger.SetReportCaller(true)
		}
		return nil
	}
}

// ListAllWithStatus returns a sorted list of volume and their status.
func (s *Linstor) ListAllWithStatus(ctx context.Context) ([]volume.VolumeStatus, error) {
	var vols []volume.VolumeStatus

	resourcesByName := make(map[string][]lapi.Resource)

	resDefs, err := s.client.ResourceDefinitions.GetAll(ctx, lapi.RDGetAllRequest{WithVolumeDefinitions: true})
	if err != nil {
		return nil, err
	}

	allResources, err := s.client.Resources.GetResourceView(ctx)
	if err != nil {
		return nil, err
	}

	for i := range allResources {
		resourcesByName[allResources[i].Name] = append(resourcesByName[allResources[i].Name], allResources[i].Resource)
	}

	for _, rd := range resDefs {
		vol := s.resourceDefinitionToVolume(rd)
		if vol != nil {
			nodes, conds := NodesAndConditionFromResources(resourcesByName[vol.ID])
			vols = append(vols, volume.VolumeStatus{
				Info:       *vol,
				Nodes:      nodes,
				Conditions: conds,
			})
		}
	}

	sort.Slice(vols, func(i, j int) bool {
		return vols[i].ID < vols[j].ID
	})

	return vols, nil
}

// AllocationSizeKiB returns LINSTOR's smallest possible number of KiB that can
// satisfy the requiredBytes.
func (s *Linstor) AllocationSizeKiB(requiredBytes, limitBytes int64) (int64, error) {
	requestedSize := data.ByteSize(requiredBytes)
	minVolumeSize := data.ByteSize(4096)
	maxVolumeSize := data.ByteSize(limitBytes)
	unlimited := maxVolumeSize == 0
	if minVolumeSize > maxVolumeSize && !unlimited {
		return 0, fmt.Errorf("LINSTOR's minimum volume size exceeds the maximum size limit of the requested volume")
	}
	if requestedSize < minVolumeSize {
		requestedSize = minVolumeSize
	}

	// make sure there are enough KiBs to fit the required number of bytes,
	// e.g. 1025 bytes require 2 KiB worth of space to be allocated.
	volumeSize := data.NewKibiByte(data.NewKibiByte(requestedSize).InclusiveBytes())

	limit := data.NewByte(maxVolumeSize)

	if volumeSize.InclusiveBytes() > limit.InclusiveBytes() && !unlimited {
		return int64(volumeSize.Value()),
			fmt.Errorf("got request for %d bytes of storage, but needed to allocate %d more bytes than the %d byte limit",
				requiredBytes, int64(volumeSize.To(data.B)-limit.To(data.B)), int64(limit.To(data.B)))
	}
	return int64(volumeSize.Value()), nil
}

// resourceDefinitionToVolume reads the serialized volume info on the lapi.ResourceDefinitionWithVolumeDefinition
// and constructs a pointer to a volume.Info from it.
func (s *Linstor) resourceDefinitionToVolume(resDef lapi.ResourceDefinitionWithVolumeDefinition) *volume.Info {
	if len(resDef.VolumeDefinitions) != 1 {
		// Not a CSI enabled volume
		return nil
	}

	fsType := resDef.Props[lapiconsts.NamespcFilesystem+"/"+lapiconsts.KeyFsType]
	props := make(map[string]string)

	for k, v := range resDef.Props {
		if strings.HasPrefix(k, lapiconsts.NamespcAuxiliary+"/") {
			props[k] = v
		}
	}

	return &volume.Info{
		ID:            resDef.Name,
		SizeBytes:     int64(resDef.VolumeDefinitions[0].SizeKib << 10),
		ResourceGroup: resDef.ResourceGroupName,
		FsType:        fsType,
		Properties:    props,
		UseQuorum:     resDef.Props["DrbdOptions/Resource/quorum"] != "off",
	}
}

// FindByID retrieves a volume.Info that has an id that matches the CSI volume
// id. Matches the LINSTOR resource name.
func (s *Linstor) FindByID(ctx context.Context, id string) (*volume.Info, error) {
	s.log.WithFields(logrus.Fields{
		"csiVolumeID": id,
	}).Debug("looking up resource by CSI volume id")

	res, err := s.client.ResourceDefinitions.Get(ctx, id)
	if err != nil {
		return nil, nil404(err)
	}

	vds, err := s.client.ResourceDefinitions.GetVolumeDefinitions(ctx, id)
	if err != nil {
		return nil, nil404(err)
	}

	return s.resourceDefinitionToVolume(lapi.ResourceDefinitionWithVolumeDefinition{
		ResourceDefinition: res,
		VolumeDefinitions:  vds,
	}), nil
}

func (s *Linstor) CompatibleVolumeId(name, pvcNamespace, pvcName string) string {
	if pvcNamespace != "" && pvcName != "" {
		s.log.Debug("try creating valid volume name based on PVC")

		generatedName := fmt.Sprintf("%s-%s", pvcNamespace, pvcName)

		if validResourceName(generatedName) == nil {
			return generatedName
		}
	}

	invalid := validResourceName(name)
	if invalid == nil {
		return name
	}

	s.log.WithField("reason", invalid).Debug("volume name is invalid, will generate fallback")

	uuidv5 := uuid.NewSHA1([]byte(linstor.DriverName), []byte(name))

	return fmt.Sprintf("vol-%s", uuidv5.String())
}

// Create creates the resource definition, volume definition, and assigns the
// resulting resource to LINSTOR nodes.
func (s *Linstor) Create(ctx context.Context, vol *volume.Info, params *volume.Parameters, topologies *csi.TopologyRequirement) error {
	logger := s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	})

	logger.Debug("reconcile resource group from storage class")
	rGroup, err := s.reconcileResourceGroup(ctx, params)
	if err != nil {
		logger.Debugf("reconcile resource group failed: %v", err)
		return err
	}

	logger.Debug("reconcile resource definition for volume")

	_, err = s.reconcileResourceDefinition(ctx, vol.ID, rGroup.Name, vol.FsType, params.FSOpts)
	if err != nil {
		logger.Debugf("reconcile resource definition failed: %v", err)
		return err
	}

	logger.Debug("reconcile volume definition for volume")

	_, err = s.reconcileVolumeDefinition(ctx, vol)
	if err != nil {
		logger.Debugf("reconcile volume definition failed: %v", err)
		return err
	}

	logger.Debug("reconcile volume placement")

	err = s.reconcileResourcePlacement(ctx, vol, params, topologies)
	if err != nil {
		logger.Debugf("reconcile volume placement failed: %v", err)
		return err
	}

	logger.Debug("reconcile extra properties")

	err = s.client.ResourceDefinitions.Modify(ctx, vol.ID, lapi.GenericPropsModify{OverrideProps: vol.Properties})
	if err != nil {
		logger.Debugf("reconcile extra properties failed: %v", err)
		return err
	}

	return nil
}

// Delete removes a persistent volume from LINSTOR.
//
// In order to support Snapshots living longer than their volumes, we have to keep the resource definition around while
// the actual resources are gone. An elegant way to go about this is by simply deleting the volume definition. This
// hides
func (s *Linstor) Delete(ctx context.Context, volId string) error {
	s.log.WithFields(logrus.Fields{
		"volume": volId,
	}).Info("deleting volume")

	// Delete the volume definition. This marks a resources as being in the process of deletion.
	err := s.client.ResourceDefinitions.DeleteVolumeDefinition(ctx, volId, 0)
	if nil404(err) != nil {
		// We continue with the cleanup on 404, maybe the previous cleanup was interrupted
		return err
	}

	resources, err := s.client.Resources.GetAll(ctx, volId)
	if err != nil {
		return nil404(err)
	}

	// Need to ensure that diskless resources are always deleted first, otherwise the last diskfull resource won't be
	// deleted
	sort.Slice(resources, func(i, j int) bool {
		return !util.DeployedDiskfully(resources[i]) && util.DeployedDiskfully(resources[j])
	})

	for _, res := range resources {
		err := s.client.Resources.Delete(ctx, volId, res.NodeName)
		if err != nil {
			// If two deletions run in parallel, one could get a 404 message, which we treat as "everything finished"
			return nil404(err)
		}
	}

	err = s.deleteResourceDefinitionAndGroupIfUnused(ctx, volId)
	if err != nil {
		return err
	}

	return nil
}

// AccessibleTopologies returns a list of pointers to csi.Topology from where the
// volume is reachable, based on the localStoragePolicy reported by the volume.
func (s *Linstor) AccessibleTopologies(ctx context.Context, volId string, params *volume.Parameters) ([]*csi.Topology, error) {
	volumeScheduler, err := s.schedulerByPlacementPolicy(params.PlacementPolicy)
	if err != nil {
		return nil, err
	}

	return volumeScheduler.AccessibleTopologies(ctx, volId, params.AllowRemoteVolumeAccess)
}

func (s *Linstor) schedulerByPlacementPolicy(policy topology.PlacementPolicy) (scheduler.Interface, error) {
	switch policy {
	case topology.AutoPlace:
		return autoplace.NewScheduler(s.client), nil
	case topology.Manual:
		return manual.NewScheduler(s.client), nil
	case topology.FollowTopology:
		return followtopology.NewScheduler(s.client, s.log), nil
	case topology.Balanced:
		return balancer.NewScheduler(s.client, s.log)
	case topology.AutoPlaceTopology:
		return autoplacetopology.NewScheduler(s.client, s.log), nil
	default:
		return nil, fmt.Errorf("unsupported volume scheduler: %s", policy)
	}
}

func (s *Linstor) GetLegacyVolumeParameters(ctx context.Context, volId string) (*volume.Parameters, error) {
	rd, err := s.client.ResourceDefinitions.Get(ctx, volId)
	if err != nil {
		return nil, err
	}

	raw, ok := rd.Props[linstor.LegacyParameterPassKey]
	if !ok {
		return nil, nil
	}

	decoded := struct {
		Parameters map[string]string `json:"parameters"`
	}{}

	err = json.Unmarshal([]byte(raw), &decoded)
	if err != nil {
		return nil, err
	}

	params, err := volume.NewParameters(decoded.Parameters, s.client.PropertyNamespace)
	if err != nil {
		return nil, err
	}

	return &params, nil
}

// Attach idempotently creates a resource on the given node.
func (s *Linstor) Attach(ctx context.Context, volId, node string, rwxBlock bool) error {
	s.log.WithFields(logrus.Fields{
		"volume":     volId,
		"targetNode": node,
	}).Info("attaching volume")

	ress, err := s.client.Resources.GetAll(ctx, volId)
	if nil404(err) != nil {
		return err
	}

	var existingRes *lapi.Resource

	disklessFlag := ""
	otherResInUse := 0

	for i := range ress {
		flag := getDisklessFlag(&ress[i])
		if flag != "" && disklessFlag == "" {
			disklessFlag = flag
		}

		if ress[i].NodeName == node {
			existingRes = &ress[i]
		} else if ress[i].State != nil && ress[i].State.InUse != nil && *ress[i].State.InUse {
			otherResInUse++
		}
	}

	if otherResInUse >= 2 {
		return fmt.Errorf("two other resources already InUse")
	}

	if otherResInUse > 0 && rwxBlock {
		rdPropsModify := lapi.GenericPropsModify{OverrideProps: map[string]string{
			linstor.PropertyAllowTwoPrimaries: "yes",
		}}

		err = s.client.ResourceDefinitions.Modify(ctx, volId, rdPropsModify)
		if err != nil {
			return err
		}
	}

	propsModify := lapi.GenericPropsModify{OverrideProps: map[string]string{}}

	// If the resource is already on the node, don't worry about attaching, unless we also need to activate it.
	if existingRes == nil || slice.ContainsString(existingRes.Flags, lapiconsts.FlagRscInactive) {
		err := s.client.Resources.MakeAvailable(ctx, volId, node, lapi.ResourceMakeAvailable{Diskful: false})

		if errors.Is(err, lapi.NotFoundError) {
			// Make-available honors replica-on-same and replicas-on-different. We do not, as the import parts of that
			// are already covered in the allowed topology bits.
			s.log.WithError(err).Info("fall back to manual diskless creation after make-available refused")

			rCreate := lapi.ResourceCreate{Resource: lapi.Resource{
				Name:     volId,
				NodeName: node,
				Flags:    []string{disklessFlag},
			}}

			err = s.client.Resources.Create(ctx, rCreate)
		}

		if err != nil {
			return err
		}

		if existingRes == nil {
			propsModify.OverrideProps[linstor.PropertyCreatedFor] = linstor.CreatedForTemporaryDisklessAttach
		}

		newRsc, err := s.client.Resources.Get(ctx, volId, node)
		if err != nil {
			return err
		}

		existingRes = &newRsc
	}

	err = s.client.Resources.ModifyVolume(ctx, volId, node, 0, propsModify)
	if err != nil {
		return err
	}

	if slice.ContainsString(existingRes.Flags, lapiconsts.FlagDelete) {
		return &DeleteInProgressError{
			Operation: "attach volume",
			Kind:      "resource",
			Name:      volId,
		}
	}

	return nil
}

// getDisklessFlag inspects a resource to determine the right diskless flag to use.
func getDisklessFlag(resource *lapi.Resource) string {
	layer := &resource.LayerObject

	for layer != nil {
		switch layer.Type {
		case devicelayerkind.Drbd:
			return lapiconsts.FlagDrbdDiskless
		case devicelayerkind.Nvme:
			return lapiconsts.FlagNvmeInitiator
		default:
			// recurse deeper
		}

		if len(layer.Children) != 1 {
			// No idea how to deal with layer depending on children anyways, so we just ignore those
			break
		}

		layer = &layer.Children[0]
	}

	return ""
}

// Detach removes a volume from the node.
func (s *Linstor) Detach(ctx context.Context, volId, node string) error {
	log := s.log.WithFields(logrus.Fields{
		"volume":     volId,
		"targetNode": node,
	})

	ress, err := s.client.Resources.GetAll(ctx, volId)
	if err != nil {
		return nil404(err)
	}

	resInUse := 0

	for i := range ress {
		if ress[i].State != nil && ress[i].State.InUse != nil && *ress[i].State.InUse {
			resInUse++
		}
	}

	if resInUse <= 1 {
		rdPropsModify := lapi.GenericPropsModify{DeleteProps: []string{
			linstor.PropertyAllowTwoPrimaries,
		}}

		err = s.client.ResourceDefinitions.Modify(ctx, volId, rdPropsModify)
		if err != nil {
			return err
		}
	}

	vol, err := s.client.Resources.GetVolume(ctx, volId, node, 0)
	if err != nil {
		return nil404(err)
	}

	createdFor, ok := vol.Props[linstor.PropertyCreatedFor]
	if !ok || createdFor != linstor.CreatedForTemporaryDisklessAttach {
		log.Info("resource not temporary (not created by Attach) not deleting")
		return nil
	}

	if vol.ProviderKind != lapi.DISKLESS {
		log.Info("temporary resource created by Attach is now diskfull, not deleting")
		return nil
	}

	log.Info("removing temporary resource")

	return nil404(s.client.Resources.Delete(ctx, volId, node))
}

// CapacityBytes returns the amount of free space in the storage pool specified by the params and topology.
func (s *Linstor) CapacityBytes(ctx context.Context, storagePool string, overProvision *float64, segments map[string]string) (int64, error) {
	log := s.log.WithField("storage-pool", storagePool).WithField("segments", segments)

	var requestedStoragePools []string

	for k := range segments {
		if strings.HasPrefix(k, topology.LinstorStoragePoolKeyPrefix) {
			requestedStoragePools = append(requestedStoragePools, k[len(topology.LinstorStoragePoolKeyPrefix):])
		}
	}

	log.Trace("request nodes for segments")

	requestedNodes, err := s.client.NodesForTopology(ctx, segments)
	if err != nil {
		return 0, fmt.Errorf("unable to get capacity: %w", err)
	}

	log.WithField("nodes", requestedNodes).Trace("got nodes")
	log.Trace("get storage pools")

	cached := true
	pools, err := s.client.Nodes.GetStoragePoolView(ctx, &lapi.ListOpts{Cached: &cached})
	if err != nil {
		return 0, fmt.Errorf("unable to get capacity: %w", err)
	}

	allNodes, err := s.client.Nodes.GetAll(ctx)
	if err != nil {
		return 0, fmt.Errorf("unable to get nodes: %w", err)
	}

	var filteredNodes []string

	for i := range allNodes {
		node := &allNodes[i]

		if !slice.ContainsString(requestedNodes, node.Name) {
			continue
		}

		if slice.ContainsString(node.Flags, lapiconsts.FlagEvicted) || slice.ContainsString(node.Flags, lapiconsts.FlagEvacuate) {
			continue
		}

		if node.Props[lapiconsts.KeyAutoplaceAllowTarget] == "false" {
			continue
		}

		filteredNodes = append(filteredNodes, node.Name)
	}

	requestedNodes = filteredNodes

	var total int64
	for _, sp := range pools {
		log := log.WithField("pool-to-check", sp.StoragePoolName).WithField("node", sp.NodeName)

		if !slice.ContainsString(requestedNodes, sp.NodeName) {
			log.Trace("not an allowed node")
			continue
		}

		if len(requestedStoragePools) > 0 && !slice.ContainsString(requestedStoragePools, sp.StoragePoolName) {
			log.Trace("not an allowed storage pool")
			continue
		}

		if sp.ProviderKind == lapi.DISKLESS {
			log.Trace("not adding diskless pool")
			continue
		}

		if storagePool != "" && storagePool != sp.StoragePoolName {
			log.Trace("not a requested storage pool")
			continue
		}

		if overProvision != nil {
			virtualCapacity := float64(sp.TotalCapacity) * *overProvision

			reservedCapacity, err := s.client.ReservedCapacity(ctx, sp.NodeName, sp.StoragePoolName)
			if err != nil {
				return 0, fmt.Errorf("failed to fetch reserved capacity: %w", err)
			}

			if reservedCapacity > int64(virtualCapacity) {
				log.Trace("ignoring pool with exhausted capacity")
				continue
			}

			log.WithField("add-capacity", int64(virtualCapacity)-reservedCapacity).Trace("adding storage pool capacity")
			total += int64(virtualCapacity) - reservedCapacity
		} else {
			log.WithField("add-capacity", sp.FreeCapacity).Trace("adding storage pool capacity")
			total += sp.FreeCapacity
		}
	}

	return int64(data.NewKibiByte(data.KiB * data.ByteSize(total)).To(data.B)), nil
}

func (s *Linstor) CompatibleSnapshotId(name string) string {
	invalid := validResourceName(name)
	if invalid == nil {
		return name
	}

	s.log.WithField("reason", invalid).Debug("snapshot name is invalid, will generate fallback")

	uuidv5 := uuid.NewSHA1([]byte(linstor.DriverName), []byte(name))

	return fmt.Sprintf("snapshot-%s", uuidv5.String())
}

// SnapCreate calls LINSTOR to create a new snapshot name "id" or backup of the volume "sourceVolId".
func (s *Linstor) SnapCreate(ctx context.Context, id, sourceVolId string, params *volume.SnapshotParameters) (*volume.Snapshot, error) {
	var lsnap *lapi.Snapshot

	var err error

	if params.Type == volume.SnapshotTypeInCluster {
		lsnap, err = s.createInClusterSnapshot(ctx, id, sourceVolId)
	} else {
		lsnap, err = s.reconcileBackup(ctx, id, sourceVolId, params)
	}

	if err != nil {
		return nil, err
	}

	return linstorSnapshotToCSI(lsnap)
}

func (s *Linstor) createInClusterSnapshot(ctx context.Context, id, sourceVolId string) (*lapi.Snapshot, error) {
	log := s.log.WithField("resource", sourceVolId).WithField("id", id)

	log.Debug("Creating in-cluster snapshot")

	ress, err := s.client.Resources.GetAll(ctx, sourceVolId)
	if err != nil {
		return nil, fmt.Errorf("failed to list source resources: %w", err)
	}

	for i := range ress {
		if ress[i].State == nil {
			return nil, fmt.Errorf("resource in unknown state, refusing to snapshot")
		}
	}

	snapConfig := lapi.Snapshot{
		Name:         id,
		ResourceName: sourceVolId,
	}

	err = s.client.Resources.CreateSnapshot(ctx, snapConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %v", err)
	}

	log.Debug("Fetch newly created snapshot")

	lsnap, err := s.client.Resources.GetSnapshot(ctx, sourceVolId, id)
	if err != nil {
		return nil, err
	}

	return &lsnap, nil
}

// reconcileBackup ensure a backup exists at the given remote
func (s *Linstor) reconcileBackup(ctx context.Context, id, sourceVolId string, params *volume.SnapshotParameters) (*lapi.Snapshot, error) {
	log := s.log.WithField("resource", sourceVolId).WithField("id", id)

	log.Debug("reconcile remote")

	err := s.reconcileRemote(ctx, params)
	if err != nil {
		return nil, err
	}

	switch params.Type {
	case volume.SnapshotTypeS3:
		info, err := s.client.Backup.Info(ctx, params.RemoteName, lapi.BackupInfoRequest{
			SrcRscName:  sourceVolId,
			SrcSnapName: id,
		})
		if nil404(err) != nil {
			return nil, fmt.Errorf("error checking for existing S3 backup: %w", err)
		}

		if info == nil {
			_, err := s.client.Backup.Create(ctx, params.RemoteName, lapi.BackupCreate{
				RscName:     sourceVolId,
				SnapName:    id,
				Incremental: params.AllowIncremental,
			})
			if err != nil {
				return nil, fmt.Errorf("error creating S3 backup: %w", err)
			}
		}

		snap, err := s.client.Resources.GetSnapshot(ctx, sourceVolId, id)
		if err != nil {
			return nil, fmt.Errorf("error fetching snapshot for backup: %w", err)
		}

		return &snap, nil
	case volume.SnapshotTypeLinstor:
		return nil, fmt.Errorf("linstor-to-linstor snapshots not implemented")
	default:
		return nil, fmt.Errorf("unsupported snapshot type '%s', don't know how to create a backup", params.Type)
	}
}

func (s *Linstor) reconcileRemote(ctx context.Context, params *volume.SnapshotParameters) error {
	log := s.log.WithField("remote-name", params.RemoteName)

	switch params.Type {
	case volume.SnapshotTypeS3:
		log.Debug("search for S3 remote with matching name")

		remotes, err := s.client.Remote.GetAllS3(ctx)
		if err != nil {
			return fmt.Errorf("failed to list existing remotes: %w", err)
		}

		for _, r := range remotes {
			if r.RemoteName == params.RemoteName {
				log.WithField("remote", r).Debug("found existing S3 remote with matching name")
				return nil
			}
		}

		log.Debug("No existing remote found, creating a new one")

		err = s.client.Remote.CreateS3(ctx, lapi.S3Remote{
			RemoteName:   params.RemoteName,
			Bucket:       params.S3Bucket,
			Region:       params.S3SigningRegion,
			Endpoint:     params.S3Endpoint,
			AccessKey:    params.S3AccessKey,
			SecretKey:    params.S3SecretKey,
			UsePathStyle: params.S3UsePathStyle,
		})
		if err != nil {
			return fmt.Errorf("failed to create new S3 remote: %w", err)
		}

		return nil
	case volume.SnapshotTypeLinstor:
		return fmt.Errorf("Linstor-to-Linstor snapshots not implemented")
	default:
		return fmt.Errorf("unsupported snapshot type '%s', don't know how to configure remote", params.Type)
	}
}

// SnapDelete calls LINSTOR to delete the snapshot based on the CSI Snapshot ID.
func (s *Linstor) SnapDelete(ctx context.Context, snap *volume.Snapshot) error {
	log := s.log.WithField("snapshot", snap)

	if snap.Remote != "" {
		log.WithField("remote", snap.Remote).Debug("deleting backup from remote")

		backups, err := s.client.Backup.GetAll(ctx, snap.Remote, snap.GetSourceVolumeId(), snap.GetSnapshotId())
		if nil404(err) != nil {
			return fmt.Errorf("failed to list backups for snapshot %s: %w", snap.GetSnapshotId(), err)
		}

		if backups != nil {
			for id := range backups.Linstor {
				// LINSTOR may return some extra results
				if backups.Linstor[id].OriginRsc != snap.GetSourceVolumeId() || backups.Linstor[id].OriginSnap != snap.GetSnapshotId() {
					log.WithField("remote_id", id).Debug("Skipping backup with wrong resource or snapshot name")
					continue
				}

				err := s.client.Backup.DeleteAll(ctx, snap.Remote, lapi.BackupDeleteOpts{
					ID: id,
				})
				if nil404(err) != nil {
					return fmt.Errorf("failed to delete backup %s: %w", id, err)
				}
			}
		}
	}

	log.Debug("deleting local snapshot")

	err := s.client.Resources.DeleteSnapshot(ctx, snap.GetSourceVolumeId(), snap.SnapshotId)
	if nil404(err) != nil {
		return fmt.Errorf("failed to remove snapshot: %w", err)
	}

	err = s.deleteResourceDefinitionAndGroupIfUnused(ctx, snap.GetSourceVolumeId())

	return nil
}

// VolFromSnap creates the volume using the data contained within the snapshot.
func (s *Linstor) VolFromSnap(ctx context.Context, snap *volume.Snapshot, vol *volume.Info, params *volume.Parameters, snapParams *volume.SnapshotParameters, topologies *csi.TopologyRequirement) error {
	logger := s.log.WithFields(logrus.Fields{
		"volume":   fmt.Sprintf("%+v", vol),
		"snapshot": fmt.Sprintf("%+v", snap),
	})

	logger.Debug("find requisite nodes")

	nodes, err := s.client.GetAllTopologyNodes(ctx, params.AllowRemoteVolumeAccess, topologies.GetRequisite())
	if err != nil {
		return err
	}

	logger.Debug("sort nodes by preference")

	preferredNodes, err := s.SortByPreferred(ctx, nodes, params.AllowRemoteVolumeAccess, topologies.GetPreferred())
	if err != nil {
		return err
	}

	logger.Debug("reconcile resource group from storage class")

	rGroup, err := s.reconcileResourceGroup(ctx, params)
	if err != nil {
		return err
	}

	logger.Debug("reconcile resource definition for volume")

	rDef, err := s.reconcileResourceDefinition(ctx, vol.ID, rGroup.Name, vol.FsType, params.FSOpts)
	if err != nil {
		return err
	}

	logger.Debug("reconcile snapshot")

	err = s.reconcileSnapshot(ctx, snap.SourceVolumeId, snap.SnapshotId, preferredNodes, params.StoragePool)
	if err != nil {
		return err
	}

	logger.Debug("reconcile volume definition from snapshot")

	err = s.reconcileSnapshotVolumeDefinitions(ctx, snap, rDef.Name)
	if err != nil {
		return err
	}

	logger.Debug("reconcile resources from snapshot")

	err = s.reconcileSnapshotResources(ctx, snap, rDef.Name, preferredNodes)
	if err != nil {
		return err
	}

	logger.Debug("reconcile volume definition from request (may expand volume)")

	_, err = s.reconcileVolumeDefinition(ctx, vol)
	if err != nil {
		return err
	}

	logger.Debug("reconcile resource placement after restore")

	err = s.reconcileResourcePlacement(ctx, vol, params, topologies)
	if err != nil {
		return err
	}

	logger.Debug("reconcile extra properties")

	err = s.client.ResourceDefinitions.Modify(ctx, vol.ID, lapi.GenericPropsModify{OverrideProps: vol.Properties})
	if err != nil {
		logger.Debugf("reconcile extra properties failed: %v", err)
		return err
	}

	if snap.Remote != "" && snapParams != nil && snapParams.DeleteLocal {
		logger.Info("deleting local copy of backup")

		err := s.client.Resources.DeleteSnapshot(ctx, snap.SourceVolumeId, snap.SnapshotId)
		if err != nil {
			logger.WithError(err).Warn("deleting local copy of backup failed")
		}

		err = s.deleteResourceDefinitionAndGroupIfUnused(ctx, snap.GetSourceVolumeId())
		if err != nil {
			logger.WithError(err).Warn("deleting local RD of backup failed")
		}
	}

	logger.Debug("success")
	return nil
}

// reconcileSnapshot ensures that the snapshot exists on a node in the cluster.
func (s *Linstor) reconcileSnapshot(ctx context.Context, sourceVolId, snapId string, nodes []string, targetPool string) error {
	logger := s.log.WithField("snapshot", snapId).WithField("source", sourceVolId)

	logger.Debug("checking for existing local snapshot")

	snap, err := s.client.Resources.GetSnapshot(ctx, sourceVolId, snapId)
	if nil404(err) != nil {
		return fmt.Errorf("failed to check for presence of local snapshot '%s': %w", snapId, err)
	}

	if err == nil {
		return s.waitLocalSnapshotSuccessful(ctx, &snap)
	}

	logger.Debug("no local snapshot present, search backups")

	remote, info, err := s.findBackupInfo(ctx, sourceVolId, snapId)
	if err != nil {
		return fmt.Errorf("failed to check for presence of remote snapshot '%s': %w", snapId, err)
	}

	if info == nil {
		return fmt.Errorf("snapshot '%s' not found", snapId)
	}

	sourcePool := info.Storpools[0].TargetName

	for _, node := range nodes {
		var rename map[string]string
		if targetPool != "" && sourcePool != targetPool {
			rename = map[string]string{sourcePool: targetPool}
		}

		err = s.client.Backup.Restore(ctx, remote, lapi.BackupRestoreRequest{
			LastBackup:    info.Latest,
			SrcRscName:    info.Rsc,
			SrcSnapName:   info.Snap,
			NodeName:      node,
			DownloadOnly:  true,
			StorPoolMap:   rename,
			TargetRscName: info.Rsc,
		})
		if err == nil {
			break
		}

		logger.WithError(err).WithField("node", node).Info("failed to restore backup to node")
	}

	if err != nil {
		return fmt.Errorf("failed to restore backup to any node (%v), last error: %w", nodes, err)
	}

	snap, err = s.client.Resources.GetSnapshot(ctx, sourceVolId, snapId)
	if nil404(err) != nil {
		return fmt.Errorf("failed to check for presence of restored snapshot: %w", err)
	}

	return s.waitLocalSnapshotSuccessful(ctx, &snap)
}

func (s *Linstor) waitLocalSnapshotSuccessful(ctx context.Context, snap *lapi.Snapshot) error {
	logger := s.log.WithField("snap", snap.Name)

	snapshotReady := func(snap *lapi.Snapshot) bool {
		if !slice.ContainsString(snap.Flags, lapiconsts.FlagSuccessful) {
			return false
		}

		if !slice.ContainsString(snap.Flags, lapiconsts.FlagBackup) {
			// not a backup -> successful == everything ready
			return true
		}

		// Shipped is also used to imply "shipped back"
		return slice.ContainsString(snap.Flags, lapiconsts.FlagShipped)
	}

	for {
		if snapshotReady(snap) {
			logger.Debug("local snapshot present and ready, nothing to do")
			return nil
		}

		logger.Debug("local snapshot present but not ready, wait 5 seconds")

		time.Sleep(5 * time.Second)

		updatedSnap, err := s.client.Resources.GetSnapshot(ctx, snap.ResourceName, snap.Name)
		if err != nil {
			return fmt.Errorf("failed to wait for snapshot success: %w", err)
		}

		snap = &updatedSnap
	}
}

func (s *Linstor) findBackupInfo(ctx context.Context, sourceVolId, snapId string) (string, *lapi.BackupInfo, error) {
	s3remotes, err := s.client.Remote.GetAllS3(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("failed to fetch all s3 remotes")
	}

	for i := range s3remotes {
		remote := s3remotes[i].RemoteName

		info, err := s.client.Backup.Info(ctx, remote, lapi.BackupInfoRequest{SrcRscName: sourceVolId, SrcSnapName: snapId})
		if nil404(err) != nil {
			return "", nil, fmt.Errorf("failed to check remote '%s' for presence of snapshot '%s'", remote, snapId)
		}

		if info == nil {
			continue
		}

		if len(info.Storpools) < 1 {
			return "", nil, fmt.Errorf("backup has no associated storage pool information")
		}

		if info != nil {
			return remote, info, nil
		}
	}

	return "", nil, nil
}

func (s *Linstor) reconcileSnapshotVolumeDefinitions(ctx context.Context, snapshot *volume.Snapshot, targetRD string) error {
	logger := s.log.WithFields(logrus.Fields{"snapshot": snapshot, "target": targetRD})

	logger.Debug("checking for existing volume definitions")
	vdefs, err := s.client.ResourceDefinitions.GetVolumeDefinitions(ctx, targetRD)
	if err != nil {
		return fmt.Errorf("could not fetch volume definitions: %w", err)
	}

	if len(vdefs) == 0 {
		logger.Debug("restoring volume definitions from snapshot")

		restoreConf := lapi.SnapshotRestore{ToResource: targetRD}
		err := s.client.Resources.RestoreVolumeDefinitionSnapshot(ctx, snapshot.GetSourceVolumeId(), snapshot.GetSnapshotId(), restoreConf)
		if err != nil {
			return fmt.Errorf("could not restore volume definitions: %w", err)
		}
	}

	return nil
}

func (s *Linstor) reconcileSnapshotResources(ctx context.Context, snapshot *volume.Snapshot, targetRD string, preferredNodes []string) error {
	logger := s.log.WithFields(logrus.Fields{"snapshot": snapshot, "target": targetRD})

	logger.Debug("checking for existing resources")
	resources, err := s.client.Resources.GetAll(ctx, targetRD)
	if err != nil {
		return fmt.Errorf("could not fetch resources: %w", err)
	}

	if len(resources) != 0 {
		logger.Debug("resource already exists, skipping restore")
		return nil
	}

	logger.Debug("checking where the snapshot is deployed")

	snap, err := s.client.Resources.GetSnapshot(ctx, snapshot.GetSourceVolumeId(), snapshot.GetSnapshotId())
	if err != nil {
		return fmt.Errorf("could not check existing snapshots: %w", err)
	}

	if len(snap.Nodes) == 0 {
		return fmt.Errorf("snapshot '%s' not deployed on any node", snap.Name)
	}

	// Optimize the node we use to restore. It should be one of the preferred nodes, or just the first with a snapshot
	// if no preferred nodes match.
	selectedNode := snap.Nodes[0]

	for _, snapNode := range snap.Nodes {
		if slice.ContainsString(preferredNodes, snapNode) {
			selectedNode = snapNode
			break
		}
	}

	logger.WithField("selected node", selectedNode).Debug("restoring snapshot on one node")

	restoreConf := lapi.SnapshotRestore{ToResource: targetRD, Nodes: []string{selectedNode}}

	err = s.client.Resources.RestoreSnapshot(ctx, snapshot.GetSourceVolumeId(), snapshot.GetSnapshotId(), restoreConf)
	if err != nil {
		return fmt.Errorf("could not restore resources: %w", err)
	}

	return nil
}

func (s *Linstor) fallbackNameUUIDNew() string {
	return s.fallbackPrefix + uuid.New()
}

// Reconcile a ResourceGroup based on the values passed to the StorageClass
func (s *Linstor) reconcileResourceGroup(ctx context.Context, params *volume.Parameters) (*lapi.ResourceGroup, error) {
	logger := s.log.WithFields(logrus.Fields{
		"params": params,
	})

	rgName := params.ResourceGroup
	if rgName == "" {
		logger.Warn("Storage Class without 'resourceGroup'. Retire this SC now!")
		rgName = s.fallbackNameUUIDNew()
	}

	// does the RG already exist?
	rg, err := s.client.ResourceGroups.Get(ctx, rgName)
	if err == lapi.NotFoundError {
		// just create the minimal RG/VG, we sync all the props then anyways.
		resourceGroup := lapi.ResourceGroup{
			Name:         rgName,
			Props:        make(map[string]string),
			SelectFilter: lapi.AutoSelectFilter{},
		}
		volumeGroup := lapi.VolumeGroup{}
		if err := s.client.ResourceGroups.Create(ctx, resourceGroup); err != nil {
			return nil, err
		}
		if err := s.client.ResourceGroups.CreateVolumeGroup(ctx, rgName, volumeGroup); err != nil {
			return nil, err
		}
		rg, err = s.client.ResourceGroups.Get(ctx, rgName)
		if err != nil {
			return nil, err
		}
	}

	rgModify, changed, err := params.ToResourceGroupModify(&rg)
	if err != nil {
		return nil, err
	}

	if changed {
		err := s.client.ResourceGroups.Modify(ctx, rgName, rgModify)
		if err != nil {
			return nil, err
		}
	}

	rg, err = s.client.ResourceGroups.Get(ctx, rgName)
	if err != nil {
		return nil, err
	}

	return &rg, nil
}

func (s *Linstor) reconcileResourceDefinition(ctx context.Context, volId, rgName, fsType, mkfsOpts string) (*lapi.ResourceDefinition, error) {
	logger := s.log.WithFields(logrus.Fields{
		"volume": volId,
	})
	logger.Info("reconcile resource definition for volume")

	logger.Debugf("check if resource definition already exists")

	rd, err := s.client.ResourceDefinitions.Get(ctx, volId)
	if errors.Is(err, lapi.NotFoundError) {
		logger.Debugf("resource definition does not exist, create now")

		rdCreate := lapi.ResourceDefinitionCreate{
			ResourceDefinition: lapi.ResourceDefinition{
				Name:              volId,
				ResourceGroupName: rgName,
			},
		}

		if fsType != "" {
			rdCreate.ResourceDefinition.Props = map[string]string{
				lapiconsts.NamespcFilesystem + "/" + lapiconsts.KeyFsType:           fsType,
				lapiconsts.NamespcFilesystem + "/" + lapiconsts.KeyFsMkfsparameters: mkfsOpts,
			}
		}

		err = s.client.ResourceDefinitions.Create(ctx, rdCreate)
		if err != nil {
			return nil, err
		}

		rd, err = s.client.ResourceDefinitions.Get(ctx, volId)
	}

	if err != nil {
		return nil, err
	}

	if slice.ContainsString(rd.Flags, lapiconsts.FlagDelete) {
		return nil, &DeleteInProgressError{
			Operation: "reconcile resource definition",
			Kind:      "resource definition",
			Name:      volId,
		}
	}

	return &rd, nil
}

func (s *Linstor) reconcileVolumeDefinition(ctx context.Context, info *volume.Info) (*lapi.VolumeDefinition, error) {
	logger := s.log.WithFields(logrus.Fields{
		"volume": info.ID,
	})
	logger.Info("reconcile volume definition for volume")

	expectedSizeKiB := uint64(data.NewKibiByte(data.ByteSize(info.SizeBytes)).Value())

	logger.Debug("check if volume definition already exists")
	vDef, err := s.client.Client.ResourceDefinitions.GetVolumeDefinition(ctx, info.ID, 0)
	if err == lapi.NotFoundError {
		zero := int32(0)
		vdCreate := lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{
				VolumeNumber: &zero,
				SizeKib:      expectedSizeKiB,
			},
		}

		err = s.client.ResourceDefinitions.CreateVolumeDefinition(ctx, info.ID, vdCreate)
		if err != nil {
			return nil, err
		}

		vDef, err = s.client.Client.ResourceDefinitions.GetVolumeDefinition(ctx, info.ID, 0)
	}

	if err != nil {
		return nil, err
	}

	// We don't support shrinking. This is mostly covered by the provisioner, but with backups there may be
	// edge cases where the "no shrinking" rule cannot be enforced. So we only allow volume growth here.
	if vDef.SizeKib < expectedSizeKiB {
		err := s.client.Client.ResourceDefinitions.ModifyVolumeDefinition(ctx, info.ID, 0, lapi.VolumeDefinitionModify{
			SizeKib: expectedSizeKiB,
		})
		if err != nil {
			return nil, err
		}
	}

	return &vDef, nil
}

func (s *Linstor) reconcileResourcePlacement(ctx context.Context, vol *volume.Info, params *volume.Parameters, topologies *csi.TopologyRequirement) error {
	logger := s.log.WithFields(logrus.Fields{
		"volume": vol.ID,
	})
	logger.Info("reconcile resource placement for volume")

	// Luckily for us, all the resource schedulers are idempotent
	volumeScheduler, err := s.schedulerByPlacementPolicy(params.PlacementPolicy)
	if err != nil {
		return err
	}

	err = volumeScheduler.Create(ctx, vol.ID, params, topologies)
	if err != nil {
		return err
	}

	return nil
}

// FindSnapByID searches the snapshot in the backend
// It returns:
// * the snapshot, nil if not found
// * true, if the snapshot is either in progress or successful
// * any error encountered
func (s *Linstor) FindSnapByID(ctx context.Context, id string) (*volume.Snapshot, bool, error) {
	matchingSnap, matchingBackup, err := s.snapOrBackupById(ctx, id)
	if err != nil {
		return nil, false, err
	}

	switch {
	case matchingSnap != nil:
		if linstorSnapshotHasError(matchingSnap) {
			return nil, false, nil
		}

		csiSnap, err := linstorSnapshotToCSI(matchingSnap)
		if err != nil {
			return nil, false, fmt.Errorf("failed to convert LINSTOR to CSI snapshot: %w", err)
		}

		return csiSnap, true, nil
	case matchingBackup != nil:
		return &volume.Snapshot{
			Snapshot: csi.Snapshot{
				SnapshotId:     matchingBackup.OriginSnap,
				SourceVolumeId: matchingBackup.OriginRsc,
				CreationTime:   timestamppb.New(matchingBackup.StartTimestamp.Time),
				ReadyToUse:     matchingBackup.Restorable,
			},
			Remote: matchingBackup.Remote,
		}, true, nil
	default:
		return nil, true, nil
	}
}

type BackupWithRemote struct {
	lapi.Backup
	Remote string
}

func (s *Linstor) snapOrBackupById(ctx context.Context, id string) (*lapi.Snapshot, *BackupWithRemote, error) {
	log := s.log.WithField("id", id)

	log.Debug("getting snapshot view")

	// LINSTOR currently does not support fetching a specific snapshot directly. You would need the resource definition
	// for that, which is not available at this stage
	snaps, err := s.client.Resources.GetSnapshotView(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find snapshots: %w", err)
	}

	for i := range snaps {
		if snaps[i].Name == id {
			log.WithField("snapshot", snaps[i]).Debug("found snapshot with matching id")

			return &snaps[i], nil, nil
		}
	}

	log.Debug("no snapshot matching id found, trying backups")

	s3remotes, err := s.client.Remote.GetAllS3(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list remotes: %w", err)
	}

	for i := range s3remotes {
		remote := s3remotes[i].RemoteName
		log := log.WithField("remote", remote)
		log.Debug("listing backups in remote")

		info, err := s.client.Backup.GetAll(ctx, remote, "", id)
		if nil404(err) != nil {
			return nil, nil, fmt.Errorf("failed to check remote '%s' for backups of id '%s': %w", remote, id, err)
		}

		if info == nil {
			continue
		}

		bMap := info.Linstor

		for k := range bMap {
			if bMap[k].OriginSnap != id {
				log.WithField("backup", bMap[k].Id).Trace("skipping backup with wrong snapshot ID")
				continue
			}

			if len(bMap[k].Vlms) != 1 {
				log.WithField("backup", bMap[k].Id).Trace("skipping backup with wrong number of volumes")
				continue
			}

			if bMap[k].StartTimestamp == nil {
				log.WithField("backup", bMap[k].Id).Trace("skipping backup without start time")
				continue
			}

			bCopy := bMap[k]

			return nil, &BackupWithRemote{
				Remote: remote,
				Backup: bCopy,
			}, nil
		}
	}

	return nil, nil, nil
}

func (s *Linstor) FindSnapsBySource(ctx context.Context, sourceVol *volume.Info, start, limit int) ([]*volume.Snapshot, error) {
	log := s.log.WithFields(logrus.Fields{"start": start, "limit": limit, "sourceVol": sourceVol})

	log.Debug("fetching snapshots for resource definition")

	if limit == 0 {
		// if set to 0 the Offset parameter is ignored, so we use MaxInt as substitute for "unlimited"
		limit = math.MaxInt32
	}

	opts := &lapi.ListOpts{
		Offset: start,
		Limit:  limit,
	}

	snaps, err := s.client.Resources.GetSnapshots(ctx, sourceVol.ID, opts)
	if nil404(err) != nil {
		return nil, fmt.Errorf("failed to fetch list of snapshots: %w", err)
	}

	var result []*volume.Snapshot
	for _, lsnap := range snaps {
		log := log.WithField("snap", lsnap)

		log.Debug("converting LINSTOR to CSI snapshot")
		csiSnap, err := linstorSnapshotToCSI(&lsnap)
		if err != nil {
			log.WithError(err).Warn("failed to convert LINSTOR to CSI snapshot, skipping...")
			continue
		}
		result = append(result, csiSnap)
	}

	return result, nil
}

// ListSnaps returns list of snapshots available in the cluster, including those available in remote (S3) locations
func (s *Linstor) ListSnaps(ctx context.Context, start, limit int) ([]*volume.Snapshot, error) {
	log := s.log.WithFields(logrus.Fields{"start": start, "limit": limit})

	log.Debug("getting snapshot view")

	if limit == 0 {
		// if set to 0 the Offset parameter is ignored, so we use MaxInt as substitute for "unlimited"
		limit = math.MaxInt32
	}

	opts := &lapi.ListOpts{
		Offset: start,
		Limit:  limit,
	}

	snaps, err := s.client.Resources.GetSnapshotView(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch list of snapshots: %w", err)
	}

	log.WithField("snaps", snaps).Trace("got snapshots")

	var result []*volume.Snapshot

	foundIds := make(map[string]struct{})

	for _, lsnap := range snaps {
		log := log.WithField("snap", lsnap)

		log.Debug("converting LINSTOR to CSI snapshot")
		csiSnap, err := linstorSnapshotToCSI(&lsnap)
		if err != nil {
			log.WithError(err).Warn("failed to convert LINSTOR to CSI snapshot, skipping...")
			continue
		}

		foundIds[csiSnap.SnapshotId] = struct{}{}
		result = append(result, csiSnap)
	}

	log.Debug("getting snapshots from remotes")

	s3remotes, err := s.client.Remote.GetAllS3(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list available s3 remotes: %w", err)
	}

	for i := range s3remotes {
		log := log.WithField("remote", s3remotes[i].RemoteName)
		log.Debug("listing backups from remote")

		list, err := s.client.Backup.GetAll(ctx, s3remotes[i].RemoteName, "", "")
		if err != nil {
			return nil, fmt.Errorf("failed to list backups in remote '%s': %w", s3remotes[i].RemoteName, err)
		}

		if list == nil {
			continue
		}

		bMap := list.Linstor

		for k := range bMap {
			if _, ok := foundIds[bMap[k].OriginSnap]; ok {
				log.WithField("backup", bMap[k].Id).Trace("skipping backup already in cluster")
				continue
			}

			if len(bMap[k].Vlms) != 1 {
				log.WithField("backup", bMap[k].Id).Trace("skipping backup with wrong number of volumes")
				continue
			}

			if bMap[k].StartTimestamp == nil {
				log.WithField("backup", bMap[k].Id).Trace("skipping backup without start time")
				continue
			}

			result = append(result, &volume.Snapshot{
				Snapshot: csi.Snapshot{
					SnapshotId:     bMap[k].OriginSnap,
					SourceVolumeId: bMap[k].OriginRsc,
					CreationTime:   timestamppb.New(bMap[k].StartTimestamp.Time),
					// At this point, we don't know the size of the snapshot, that would require a separate REST call,
					// which we skip for performance reasons. This field is optional.
					SizeBytes: 0,
				},
				Remote: s3remotes[i].RemoteName,
			})
		}
	}

	return result, nil
}

func linstorSnapshotToCSI(lsnap *lapi.Snapshot) (*volume.Snapshot, error) {
	if len(lsnap.VolumeDefinitions) == 0 {
		return nil, fmt.Errorf("missing volume definitions")
	}

	if len(lsnap.Snapshots) == 0 {
		return nil, fmt.Errorf("missing snapshots")
	}

	if slice.ContainsString(lsnap.Flags, lapiconsts.FlagDelete) {
		return nil, &DeleteInProgressError{
			Operation: "csi snapshot from linstor",
			Kind:      "snapshot",
			Name:      lsnap.Name,
		}
	}

	snapSizeBytes := lsnap.VolumeDefinitions[0].SizeKib * uint64(data.KiB)
	creationTimeMicroSecs := lsnap.Snapshots[0].CreateTimestamp

	ready := slice.ContainsString(lsnap.Flags, lapiconsts.FlagSuccessful)
	if slice.ContainsString(lsnap.Flags, lapiconsts.FlagBackup) {
		ready = slice.ContainsString(lsnap.Flags, lapiconsts.FlagShipped)
	}

	if creationTimeMicroSecs == nil {
		// Some failed snapshots do not show any time values, set a default value.
		creationTimeMicroSecs = &lapi.TimeStampMs{}
	}

	remote := lsnap.Props["BackupShipping/BackupTargetRemote"]

	return &volume.Snapshot{
		Snapshot: csi.Snapshot{
			SnapshotId:     lsnap.Name,
			SourceVolumeId: lsnap.ResourceName,
			SizeBytes:      int64(snapSizeBytes),
			CreationTime:   timestamppb.New(creationTimeMicroSecs.Time),
			ReadyToUse:     ready,
		},
		Remote: remote,
	}, nil
}

func linstorSnapshotHasError(lsnap *lapi.Snapshot) bool {
	return slice.ContainsString(lsnap.Flags, lapiconsts.FlagFailedDisconnect) || slice.ContainsString(lsnap.Flags, lapiconsts.FlagFailedDeployment)
}

// NodeAvailable makes sure that LINSTOR considers that the node is in an ONLINE
// state.
func (s *Linstor) NodeAvailable(ctx context.Context, node string) error {
	n, err := s.client.Nodes.Get(ctx, node)
	if err != nil {
		return err
	}

	if n.ConnectionStatus != "ONLINE" {
		return fmt.Errorf("node %s", n.ConnectionStatus)
	}

	return nil
}

// FindAssignmentOnNode returns a pointer to a volume.Assignment for a given node.
func (s *Linstor) FindAssignmentOnNode(ctx context.Context, volId, node string) (*volume.Assignment, error) {
	s.log.WithFields(logrus.Fields{
		"volume":     volId,
		"targetNode": node,
	}).Debug("getting assignment info")

	linVol, err := s.client.Resources.GetVolume(ctx, volId, node, 0)
	if err != nil {
		return nil, nil404(err)
	}

	va := &volume.Assignment{
		Node: node,
		Path: linVol.DevicePath,
	}

	s.log.WithFields(logrus.Fields{
		"volumeAssignment": fmt.Sprintf("%+v", va),
	}).Debug("found assignment info")

	return va, nil
}

func (s *Linstor) Status(ctx context.Context, volId string) ([]string, *csi.VolumeCondition, error) {
	s.log.WithFields(logrus.Fields{
		"volume": volId,
	}).Debug("getting assignments")

	ress, err := s.client.Resources.GetAll(ctx, volId)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list resources for '%s': %w", volId, err)
	}

	nodes, conds := NodesAndConditionFromResources(ress)

	return nodes, conds, nil
}

func NodesAndConditionFromResources(ress []lapi.Resource) ([]string, *csi.VolumeCondition) {
	var allNodes, abnormalNodes []string

	for i := range ress {
		res := &ress[i]

		allNodes = append(allNodes, res.NodeName)

		if res.State == nil {
			abnormalNodes = append(abnormalNodes, res.NodeName)
			continue
		}

		drbd := util.GetDrbdLayer(&res.LayerObject)
		if drbd != nil && drbd.PromotionScore == 0 {
			abnormalNodes = append(abnormalNodes, res.NodeName)
		}
	}

	condition := &csi.VolumeCondition{
		Abnormal: false,
		Message:  "Volume healthy",
	}

	if len(abnormalNodes) > 0 {
		condition.Abnormal = true
		condition.Message = fmt.Sprintf("Resource with issues on node(s): %s", strings.Join(abnormalNodes, ","))
	}

	sort.Strings(allNodes)

	return allNodes, condition
}

// Mount makes volumes consumable from the source to the target.
// Filesystems are formatted and block devices are bind mounted.
// Operates locally on the machines where it is called.
func (s *Linstor) Mount(ctx context.Context, source, target, fsType string, readonly bool, mntOpts []string) error {
	// If there is no fsType, then this is a block mode volume.
	var block bool
	if fsType == "" {
		block = true
	}

	s.log.WithFields(logrus.Fields{
		"source":          source,
		"target":          target,
		"mountOpts":       mntOpts,
		"filesystem":      fsType,
		"blockAccessMode": block,
	}).Info("mounting volume")

	info, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("failed to stat source device: %w", err)
	}

	if (info.Mode() & os.ModeDevice) != os.ModeDevice {
		return fmt.Errorf("path %s is not a device", source) //nolint:goerr113
	}

	if readonly {
		mntOpts = append(mntOpts, "ro")

		// This requires DRBD 9.0.26+, older versions ignore this flag
		err := s.setDevReadOnly(ctx, source)
		if err != nil {
			return fmt.Errorf("failed to set source device readonly: %w", err)
		}
	} else {
		// We might be re-using an existing device that was set RO previously
		err = s.setDevReadWrite(ctx, source)
		if err != nil {
			return fmt.Errorf("failed to set source device readwrite: %w", err)
		}
	}

	// This is a regular filesystem so format the device and create the mountpoint.
	if !block {
		if err := os.MkdirAll(target, os.FileMode(0755)); err != nil {
			return fmt.Errorf("could not create target directory %s, %v", target, err)
		}
		// This is a block volume so create a file to bindmount to.
	} else {
		f, err := os.OpenFile(target, os.O_CREATE, os.FileMode(0644))
		if err != nil {
			if !os.IsExist(err) {
				return fmt.Errorf("could not create bind target for block volume %s, %w", target, err)
			}
		} else {
			_ = f.Close()
		}
	}

	needsMount, err := mount.IsNotMountPoint(s.mounter, target)
	if err != nil {
		return fmt.Errorf("unable to determine mount status of %s %v", target, err)
	}

	if !needsMount {
		return nil
	}

	err = s.mounter.Mount(source, target, fsType, mntOpts)
	if err != nil {
		return err
	}

	resizerFs := mount.NewResizeFs(s.mounter.Exec)

	resize, err := resizerFs.NeedResize(source, target)
	if err != nil {
		return fmt.Errorf("unable to determine if resize required: %w", err)
	}

	if !block && resize {
		_, err := resizerFs.Resize(source, target)
		if err != nil {
			unmountErr := s.Unmount(target)
			if unmountErr != nil {
				return fmt.Errorf("unable to unmount volume after failed resize (%s): %w", unmountErr, err)
			}

			return fmt.Errorf("unable to resize volume: %w", err)
		}
	}

	return nil
}

func (s *Linstor) setDevReadOnly(ctx context.Context, srcPath string) error {
	_, err := s.mounter.Exec.CommandContext(ctx, "blockdev", "--setro", srcPath).CombinedOutput()
	return err
}

func (s *Linstor) setDevReadWrite(ctx context.Context, srcPath string) error {
	_, err := s.mounter.Exec.CommandContext(ctx, "blockdev", "--setrw", srcPath).CombinedOutput()
	return err
}

// IsNotMountPoint determines if a directory is a mountpoint.
//
// Non-existent paths return (true, nil).
func (s *Linstor) IsNotMountPoint(target string) (bool, error) {
	notMounted, err := mount.IsNotMountPoint(s.mounter, target)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}

		return false, err
	}

	return notMounted, nil
}

// Unmount unmounts the target. Operates locally on the machines where it is called.
func (s *Linstor) Unmount(target string) error {
	s.log.WithFields(logrus.Fields{
		"target": target,
	}).Info("unmounting volume")

	err := mount.CleanupMountPoint(target, s.mounter, true)
	if err != nil {
		return fmt.Errorf("unable to cleanup mount point: %w", err)
	}

	return nil
}

// validResourceName returns an error if the input string is not a valid LINSTOR name
func validResourceName(resName string) error {
	if resName == "all" {
		return errors.New("not allowed to use 'all' as resource name")
	}

	b, err := regexp.MatchString("[[:alpha:]]", resName)
	if err != nil {
		return err
	} else if !b {
		return errors.New("resource name did not contain at least one alphabetic (A-Za-z) character")
	}

	re := "^[A-Za-z_][A-Za-z0-9\\-_]{1,47}$"
	b, err = regexp.MatchString(re, resName)
	if err != nil {
		return err
	} else if !b {
		// without open coding it (ugh!) as good as it gets
		return fmt.Errorf("resource name did not match: '%s'", re)
	}

	return nil
}

// linstorifyResourceName tries to generate a valid LINSTOR name if the input currently is not.
// If the input is already valid, it just returns this name.
// This tries to preserve the original meaning as close as possible, but does not try extra hard.
// Do *not* expect this function to be injective.
// Do *not* expect this function to be stable. This means you need to save the output, the output of the function might change without notice.
func linstorifyResourceName(name string) (string, error) {
	if err := validResourceName(name); err == nil {
		return name, nil
	}

	re := regexp.MustCompile(`[^A-Za-z0-9\-_]`)
	newName := re.ReplaceAllLiteralString(name, "_")
	if err := validResourceName(newName); err == nil {
		return newName, err
	}

	// fulfill at least the minimal requirement
	newName = "LS_" + newName
	if err := validResourceName(newName); err == nil {
		return newName, nil
	}

	return "", fmt.Errorf("could not linstorify name (%s)", name)
}

func nil404(e error) error {
	if e == lapi.NotFoundError {
		return nil
	}
	return e
}

// GetVolumeStats determines filesystem usage.
func (s *Linstor) GetVolumeStats(path string) (volume.VolumeStats, error) {
	var statfs unix.Statfs_t
	err := unix.Statfs(path, &statfs)
	if err != nil {
		return volume.VolumeStats{}, err
	}

	return volume.VolumeStats{
		AvailableBytes:  int64(statfs.Bavail) * int64(statfs.Bsize),
		TotalBytes:      int64(statfs.Blocks) * int64(statfs.Bsize),
		UsedBytes:       (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize),
		AvailableInodes: int64(statfs.Ffree),
		TotalInodes:     int64(statfs.Files),
		UsedInodes:      int64(statfs.Files) - int64(statfs.Ffree),
	}, nil
}

func (s *Linstor) NodeExpand(source, target string) error {
	resizer := mount.NewResizeFs(s.mounter.Exec)
	_, err := resizer.Resize(source, target)
	return err
}

func (s *Linstor) ControllerExpand(ctx context.Context, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("controller expand volume")

	volumeDefinitionModify := lapi.VolumeDefinitionModify{
		SizeKib: uint64(data.NewKibiByte(data.ByteSize(vol.SizeBytes)).Value()),
	}

	volumeDefinitions, err := s.client.ResourceDefinitions.GetVolumeDefinitions(ctx, vol.ID)
	if err != nil || len(volumeDefinitions) < 1 {
		return fmt.Errorf("get volumeDefinitions failed volumeInfo: %v, err: %v", vol, err)
	}

	if volumeDefinitionModify.SizeKib == volumeDefinitions[0].SizeKib {
		return nil
	}

	if volumeDefinitionModify.SizeKib < volumeDefinitions[0].SizeKib {
		return fmt.Errorf("storage only support expand does not support reduce.volumeInfo: %v old: %d, new: %d", vol, volumeDefinitions[0].SizeKib, volumeDefinitionModify.SizeKib)
	}

	err = s.client.ResourceDefinitions.ModifyVolumeDefinition(ctx, vol.ID, int(*volumeDefinitions[0].VolumeNumber), volumeDefinitionModify)
	if err != nil {
		return err
	}

	return nil
}

func (s *Linstor) GetNodeTopologies(ctx context.Context, nodename string) (*csi.Topology, error) {
	topo := &csi.Topology{
		Segments: map[string]string{
			topology.LinstorNodeKey: nodename,
		},
	}

	var pools []lapi.StoragePool

	if s.labelBySP {
		cached := true

		p, err := s.client.Nodes.GetStoragePools(ctx, nodename, &lapi.ListOpts{Cached: &cached})
		if err != nil {
			return nil, fmt.Errorf("failed to get storage pools for node: %w", err)
		}

		pools = p
	}

	// Ideally we could pass the configured storage pools as a single "linbit.com/storage-pool = [pool1, pool2, ...]".
	// Unfortunately, setting a list for topologies is not supported. Instead we create a unique key for each storage
	// pool.
	// See https://github.com/container-storage-interface/spec/issues/255
	for i := range pools {
		label := topology.ToStoragePoolLabel(pools[i].StoragePoolName)
		topo.Segments[label] = "true"
	}

	node, err := s.client.Nodes.Get(ctx, nodename)
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	auxPrefix := s.client.PropertyNamespace + "/"
	for k, v := range node.Props {
		if strings.HasPrefix(k, auxPrefix) {
			topo.Segments[k[len(auxPrefix):]] = v
		}
	}

	return topo, nil
}

// Delete the given resource definition and linked resource group, but only if:
// * No resources are placed
// * No snapshots of the resource exist
func (s *Linstor) deleteResourceDefinitionAndGroupIfUnused(ctx context.Context, rdName string) error {
	log := s.log.WithField("rd", rdName)
	log.Debug("checking for undeleted resources")

	resources, err := s.client.Resources.GetAll(ctx, rdName)
	if err != nil {
		// Returns nil if api call returned 404, i.e. someone else already deleted the RD
		return nil404(err)
	}

	for _, res := range resources {
		if !slice.ContainsString(res.Flags, lapiconsts.FlagDelete) {
			log.WithField("resource", res).Debug("not deleting resource definition, found undeleted resource")
			return nil
		}
	}

	log.Debug("checking for undeleted snapshots")

	snapshots, err := s.client.Resources.GetSnapshots(ctx, rdName)
	if err != nil {
		// Returns nil if api call returned 404, i.e. someone else already deleted the RD
		return nil404(err)
	}

	for _, snap := range snapshots {
		if !slice.ContainsString(snap.Flags, lapiconsts.FlagDelete) {
			log.WithField("snapshot", snap).Debug("not deleting resource definition, found undeleted snapshot")
			return nil
		}
	}

	log.Debug("fetching resource definition, then deleting it")

	// Our last chance to get the resource group from the RD
	rDef, err := s.client.ResourceDefinitions.Get(ctx, rdName)
	if err != nil {
		// if no RD was found, returns nil
		return nil404(err)
	}

	err = s.client.ResourceDefinitions.Delete(ctx, rdName)
	if err != nil {
		// Returns nil if api call returned 404, i.e. someone else already deleted the RD
		return nil404(err)
	}

	log.Info("checking RG for removal")

	if rDef.ResourceGroupName == "DfltRscGrp" {
		// Don't try to delete the default resource group
		return nil
	}

	err = s.client.ResourceGroups.Delete(ctx, rDef.ResourceGroupName)
	if err != nil {
		if lapi.IsApiCallError(err, lapiconsts.FailExistsRscDfn) {
			return nil
		}

		return nil404(err)
	}

	return nil
}

// SortByPreferred sorts nodes based on the given topology preferences.
//
// The resulting list of nodes contains the same nodes as in the input, but sorted by the preferred topologies.
// A node matching the first preferred topology will be first in the output, after that nodes that match the first
// preferred topology via volume.RemoteAccessPolicy, after that nodes for the second preferred topology, until
// at the end all nodes that don't match any preferred topology are listed.
func (s *Linstor) SortByPreferred(ctx context.Context, nodes []string, remotePolicy volume.RemoteAccessPolicy, preferred []*csi.Topology) ([]string, error) {
	prefMap := make(map[string]int)

	for _, node := range nodes {
		prefMap[node] = math.MaxInt
	}

	order := 0

	for _, pref := range preferred {
		// First add the original node directly
		nodes, err := s.client.NodesForTopology(ctx, pref.GetSegments())
		if err != nil {
			return nil, fmt.Errorf("failed to fetch nodes for topology: %w", err)
		}

		for _, node := range nodes {
			if prefMap[node] == math.MaxInt {
				prefMap[node] = order
			}
		}

		order++

		for _, seg := range remotePolicy.AccessibleSegments(pref.GetSegments()) {
			nodes, err = s.client.NodesForTopology(ctx, seg)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch nodes for topology: %w", err)
			}

			for _, node := range nodes {
				if prefMap[node] == math.MaxInt {
					prefMap[node] = order
				}
			}

			order++
		}
	}

	orderedNodes := make([]string, len(nodes))
	copy(orderedNodes, nodes)

	sort.SliceStable(orderedNodes, func(i, j int) bool {
		return prefMap[orderedNodes[i]] < prefMap[orderedNodes[j]]
	})

	return orderedNodes, nil
}
