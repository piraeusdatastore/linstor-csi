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
	"strconv"
	"strings"

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
	}

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

// ListAll returns a sorted list of pointers to volume.Info. Only the LINSTOR
// volumes that can be serialized into a volume.Info are included.
func (s *Linstor) ListAll(ctx context.Context) ([]*volume.Info, error) {
	vols := make([]*volume.Info, 0)

	resDefs, err := s.client.ResourceDefinitions.GetAll(ctx, lapi.RDGetAllRequest{WithVolumeDefinitions: true})
	if err != nil {
		return vols, nil
	}

	for _, rd := range resDefs {
		vol := s.resourceDefinitionToVolume(rd)
		if vol != nil {
			vols = append(vols, vol)
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
		Properties:    props,
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

	uuidv5 := uuid.NewSHA1([]byte("linstor.csi.linbit.com"), []byte(name))

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

	_, err = s.reconcileResourceDefinition(ctx, vol.ID, rGroup.Name)
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

	params, err := volume.NewParameters(decoded.Parameters)
	if err != nil {
		return nil, err
	}

	return &params, nil
}

// Attach idempotently creates a resource on the given node.
func (s *Linstor) Attach(ctx context.Context, volId, node string, readOnly bool) error {
	s.log.WithFields(logrus.Fields{
		"volume":     volId,
		"targetNode": node,
	}).Info("attaching volume")

	ress, err := s.client.Resources.GetResourceView(ctx, &lapi.ListOpts{Resource: []string{volId}})
	if nil404(err) != nil {
		return err
	}

	var existingRes *lapi.Resource

	existingSharedName := ""

	for i := range ress {
		if ress[i].NodeName == node {
			existingRes = &ress[i].Resource
			existingSharedName = ress[i].SharedName
			break
		}
	}

	propsModify := lapi.GenericPropsModify{OverrideProps: map[string]string{
		linstor.PublishedReadOnlyKey: strconv.FormatBool(readOnly),
	}}

	// If the resource is already on the node, don't worry about attaching.
	if existingRes == nil {
		// In certain circumstances it is necessary to create a diskfull resource to make it usable.
		// The bug report that introduced this variable is a good example:
		// * Our cluster has 4 identical nodes called A, B, C, D.
		// * A resource was placed in a typical 2 + 1 Tiebreaker configuration, let's say the diskfull resources are
		//   on A and B, the Tiebreaker on C.
		// * A Pod attaches on node A.
		// * Node A goes down/becomes unreachable/etc. That leaves us with 1 diskfull resource on B + 1 diskless on C.
		// * The Pod is deleted and a replacement scheduled on node D (i.e. after deletion by the HA Controller).
		// * Now, this method will be called, since we need to Attach on node D, and there is currently no resource on that
		//   node.
		// * Using a diskless resource will not work, as the newly created resource would not get quorum.
		// * Using a diskfull resource will work and sync up to the remaining diskfull resource on B.
		availableDiskfullResources := 0
		unavailableDiskfullResources := 0
		disklessCreateFlag := ""

		for i := range ress {
			drbdDiskfull, flag := inspectExistingResource(&ress[i].Resource)

			if disklessCreateFlag == "" {
				disklessCreateFlag = flag
			}

			if drbdDiskfull != nil {
				if *drbdDiskfull {
					availableDiskfullResources++
				} else {
					unavailableDiskfullResources++
				}
			}
		}

		s.log.Infof("volume %s does not exist on node %s, creating new resource", volId, node)

		// If only half of the expected resources are available, we need a diskfull deployment to have any hope
		// of achieving quorum on the node. See the comment above availableDiskfullResources.
		shouldDeployDiskful := availableDiskfullResources > 0 && unavailableDiskfullResources >= availableDiskfullResources

		if shouldDeployDiskful {
			s.log.Infof("%d replicas of %d are apparently not reachable, create a new diskfull resource for quorum", unavailableDiskfullResources, unavailableDiskfullResources+availableDiskfullResources)

			err = s.client.Resources.MakeAvailable(ctx, volId, node, lapi.ResourceMakeAvailable{Diskful: true})
		} else {
			err = s.client.Resources.MakeAvailable(ctx, volId, node, lapi.ResourceMakeAvailable{Diskful: false})
		}

		if errors.Is(err, lapi.NotFoundError) {
			// Make-available honors replica-on-same and replicas-on-different. We do not, as the import parts of that
			// are already covered in the allowed topology bits.
			s.log.WithError(err).Info("fall back to manual diskless creation after make-available refused")

			rCreate := lapi.ResourceCreate{Resource: lapi.Resource{
				Name:     volId,
				NodeName: node,
			}}

			if !shouldDeployDiskful {
				rCreate.Resource.Flags = append(rCreate.Resource.Flags, disklessCreateFlag)
			}

			err = s.client.Resources.Create(ctx, rCreate)
		}

		propsModify.OverrideProps[linstor.PropertyCreatedFor] = linstor.CreatedForTemporaryDisklessAttach

		if err != nil {
			return err
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

	if slice.ContainsString(existingRes.Flags, lapiconsts.FlagRscInactive) {
		for i := range ress {
			res := &ress[i]

			if res.SharedName == "" {
				continue
			}

			if res.SharedName != existingSharedName {
				continue
			}

			if slice.ContainsString(res.Flags, lapiconsts.FlagRscInactive) {
				continue
			}

			err := s.client.Resources.Deactivate(ctx, res.Name, res.NodeName)
			if err != nil {
				return fmt.Errorf("failed to deactivate node %s in shared storage pool %s for volume %s: %w", res.NodeName, res.SharedName, volId, err)
			}
		}

		return s.client.Resources.Activate(ctx, volId, node)
	}

	return nil
}

// inspectExistingResource inspects a resource to determine the right diskless
func inspectExistingResource(resource *lapi.Resource) (*bool, string) {
	layer := &resource.LayerObject

	isAvailableDiskful := true
	isUnavailableDiskful := false

	for layer != nil {
		if layer.Type == devicelayerkind.Drbd {
			if slice.ContainsString(layer.Drbd.Flags, lapiconsts.FlagDiskless) {
				return nil, lapiconsts.FlagDrbdDiskless
			}

			if layer.Drbd.PromotionScore != 0 {
				return &isAvailableDiskful, lapiconsts.FlagDrbdDiskless
			} else {
				return &isUnavailableDiskful, lapiconsts.FlagDrbdDiskless
			}
		}

		if layer.Type == devicelayerkind.Nvme {
			return nil, lapiconsts.FlagNvmeInitiator
		}

		if len(layer.Children) != 1 {
			// No idea how to deal with layer depending on children anyways, so we just ignore those
			break
		}

		layer = &layer.Children[0]
	}

	return nil, ""
}

// Detach removes a volume from the node.
func (s *Linstor) Detach(ctx context.Context, volId, node string) error {
	log := s.log.WithFields(logrus.Fields{
		"volume":     volId,
		"targetNode": node,
	})

	vols, err := s.client.Resources.GetVolumes(ctx, volId, node)
	if err != nil {
		return nil404(err)
	}

	if len(vols) != 1 {
		return fmt.Errorf("expected exactly 1 volume, got %d instead", len(vols))
	}

	createdFor, ok := vols[0].Props[linstor.PropertyCreatedFor]
	if !ok || createdFor != linstor.CreatedForTemporaryDisklessAttach {
		log.Info("resource not temporary (not created by Attach) not deleting")
		return nil
	}

	if vols[0].ProviderKind != lapi.DISKLESS {
		log.Info("temporary resource created by Attach is now diskfull, not deleting")
		return nil
	}

	log.Info("removing temporary resource")

	return nil404(s.client.Resources.Delete(ctx, volId, node))
}

// CapacityBytes returns the amount of free space in the storage pool specified by the params and topology.
func (s *Linstor) CapacityBytes(ctx context.Context, storagePool string, segments map[string]string) (int64, error) {
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

	pools, err := s.client.Nodes.GetStoragePoolView(ctx)
	if err != nil {
		return 0, fmt.Errorf("unable to get capacity: %w", err)
	}

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

		if storagePool == "" || storagePool == sp.StoragePoolName {
			log.Trace("adding storage pool capacity")
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

	uuidv5 := uuid.NewSHA1([]byte("linstor.csi.linbit.com"), []byte(name))

	return fmt.Sprintf("snapshot-%s", uuidv5.String())
}

// SnapCreate calls linstor to create a new snapshot on the volume indicated by
// the SourceVolumeId contained in the CSI Snapshot.
func (s *Linstor) SnapCreate(ctx context.Context, id string, sourceVol *volume.Info) (*csi.Snapshot, error) {
	snapConfig := lapi.Snapshot{
		Name:         id,
		ResourceName: sourceVol.ID,
	}

	err := s.client.Resources.CreateSnapshot(ctx, snapConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %v", err)
	}

	lsnap, err := s.client.Resources.GetSnapshot(ctx, sourceVol.ID, id)
	if err != nil {
		return nil, err
	}

	return linstorSnapshotToCSI(&lsnap)
}

// SnapDelete calls LINSTOR to delete the snapshot based on the CSI Snapshot ID.
func (s *Linstor) SnapDelete(ctx context.Context, snap *csi.Snapshot) error {
	log := s.log.WithField("snapshot", snap)

	log.Debug("deleting snapshot")

	err := s.client.Resources.DeleteSnapshot(ctx, snap.GetSourceVolumeId(), snap.SnapshotId)
	if nil404(err) != nil {
		return fmt.Errorf("failed to remove snaphsot: %v", err)
	}

	err = s.deleteResourceDefinitionAndGroupIfUnused(ctx, snap.GetSourceVolumeId())

	return nil
}

// VolFromSnap creates the volume using the data contained within the snapshot.
func (s *Linstor) VolFromSnap(ctx context.Context, snap *csi.Snapshot, vol *volume.Info, params *volume.Parameters, topologies *csi.TopologyRequirement) error {
	logger := s.log.WithFields(logrus.Fields{
		"volume":   fmt.Sprintf("%+v", vol),
		"snapshot": fmt.Sprintf("%+v", snap),
	})

	logger.Debug("find requisite nodes")

	nodes, err := s.client.GetAllTopologyNodes(ctx, params.AllowRemoteVolumeAccess, topologies.GetRequisite())
	if err != nil {
		return err
	}

	logger.Debug("reconcile resource group from storage class")

	rGroup, err := s.reconcileResourceGroup(ctx, params)
	if err != nil {
		return err
	}

	logger.Debug("reconcile resource definition for volume")

	rDef, err := s.reconcileResourceDefinition(ctx, vol.ID, rGroup.Name)
	if err != nil {
		return err
	}

	logger.Debug("reconcile volume definition from snapshot")

	err = s.reconcileSnapshotVolumeDefinitions(ctx, snap, rDef.Name)
	if err != nil {
		return err
	}

	logger.Debug("reconcile resources from snapshot")

	err = s.reconcileSnapshotResources(ctx, snap, rDef.Name, nodes)
	if err != nil {
		return err
	}

	logger.Debug("reconcile resource placement after restore")

	err = s.reconcileResourcePlacement(ctx, vol, params, topologies)
	if err != nil {
		return err
	}

	logger.Debug("reconcile volume definition from request (may expand volume)")

	_, err = s.reconcileVolumeDefinition(ctx, vol)
	if err != nil {
		return err
	}

	logger.Debug("reconcile extra properties")

	err = s.client.ResourceDefinitions.Modify(ctx, vol.ID, lapi.GenericPropsModify{OverrideProps: vol.Properties})
	if err != nil {
		logger.Debugf("reconcile extra properties failed: %v", err)
		return err
	}

	logger.Debug("success")
	return nil
}

func (s *Linstor) reconcileSnapshotVolumeDefinitions(ctx context.Context, snapshot *csi.Snapshot, targetRD string) error {
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

func (s *Linstor) reconcileSnapshotResources(ctx context.Context, snapshot *csi.Snapshot, targetRD string, preferredNodes []string) error {
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

func (s *Linstor) reconcileResourceDefinition(ctx context.Context, volId, rgName string) (*lapi.ResourceDefinition, error) {
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
		vdCreate := lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{
				VolumeNumber: 0,
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

	if vDef.SizeKib != expectedSizeKiB {
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

func (s *Linstor) FindSnapByID(ctx context.Context, id string) (*csi.Snapshot, bool, error) {
	log := s.log.WithField("id", id)

	log.Debug("getting snapshot view")

	// LINSTOR currently does not support fetching a specific snapshot directly. You would need the resource definition
	// for that, which is not available at this stage
	snaps, err := s.client.Resources.GetSnapshotView(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find snapshots: %w", err)
	}

	var matchingSnap *lapi.Snapshot
	for _, lsnap := range snaps {
		if lsnap.Name == id {
			matchingSnap = &lsnap
			break
		}
	}

	if matchingSnap == nil {
		return nil, true, nil
	}

	if linstorSnapshotHasError(matchingSnap) {
		return nil, false, nil
	}

	log.Debug("converting LINSTOR snapshot to CSI snapshot")
	csiSnap, err := linstorSnapshotToCSI(matchingSnap)
	if err != nil {
		return nil, false, fmt.Errorf("failed to convert LINSTOR to CSI snapshot: %w", err)
	}

	return csiSnap, true, nil
}

func (s *Linstor) FindSnapsBySource(ctx context.Context, sourceVol *volume.Info, start, limit int) ([]*csi.Snapshot, error) {
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

	var result []*csi.Snapshot
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

// ListSnaps returns list of pointers to volume.SnapInfo based off of the
// serialized snapshot info stored in resource definitions.
func (s *Linstor) ListSnaps(ctx context.Context, start, limit int) ([]*csi.Snapshot, error) {
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

	var result []*csi.Snapshot
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

func linstorSnapshotToCSI(lsnap *lapi.Snapshot) (*csi.Snapshot, error) {
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

	if creationTimeMicroSecs == nil {
		// Some failed snapshots do not show any time values, set a default value.
		creationTimeMicroSecs = &lapi.TimeStampMs{}
	}

	return &csi.Snapshot{
		SnapshotId:     lsnap.Name,
		SourceVolumeId: lsnap.ResourceName,
		SizeBytes:      int64(snapSizeBytes),
		CreationTime:   timestamppb.New(creationTimeMicroSecs.Time),
		ReadyToUse:     ready,
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

	var readOnly *bool

	if linVol.Props != nil {
		b, err := strconv.ParseBool(linVol.Props[linstor.PublishedReadOnlyKey])
		if err == nil {
			readOnly = &b
		}
	}

	va := &volume.Assignment{
		Node:     node,
		Path:     linVol.DevicePath,
		ReadOnly: readOnly,
	}

	s.log.WithFields(logrus.Fields{
		"volumeAssignment": fmt.Sprintf("%+v", va),
	}).Debug("found assignment info")

	return va, nil
}

// Mount makes volumes consumable from the source to the target.
// Filesystems are formatted and block devices are bind mounted.
// Operates locally on the machines where it is called.
func (s *Linstor) Mount(ctx context.Context, source, target, fsType string, readonly bool, mntOpts, fsOpts []string) error {
	// If there is no fsType, then this is a block mode volume.
	var block bool
	if fsType == "" {
		block = true
	}

	s.log.WithFields(logrus.Fields{
		"source":          source,
		"target":          target,
		"mountOpts":       mntOpts,
		"fsOpts":          fsOpts,
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
		if err := s.formatDevice(ctx, source, fsType, fsOpts); err != nil {
			return fmt.Errorf("mounting volume failed: %v", err)
		}

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

	if block {
		return s.mounter.Mount(source, target, fsType, mntOpts)
	}

	err = s.mounter.FormatAndMount(source, target, fsType, mntOpts)
	if err != nil {
		return err
	}

	resizerFs := mount.NewResizeFs(s.mounter.Exec)

	resize, err := resizerFs.NeedResize(source, target)
	if err != nil {
		return fmt.Errorf("unable to determine if resize required: %w", err)
	}

	if resize {
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

func (s *Linstor) formatDevice(ctx context.Context, source, fsType string, mkfsArgs []string) error {
	// Format device with Storage Class's filesystem options.
	deviceFS, err := s.mounter.GetDiskFormat(source)
	if err != nil {
		return fmt.Errorf("unable to determine filesystem type of %s: %v", source, err)
	}

	// Device is formatted correctly already.
	if deviceFS == fsType {
		s.log.WithFields(logrus.Fields{
			"deviceFS":    deviceFS,
			"requestedFS": fsType,
			"device":      source,
		}).Debug("device already formatted with requested filesystem")
		return nil
	}

	if deviceFS != "" && deviceFS != fsType {
		return fmt.Errorf("device %q already formatted with %q filesystem, refusing to overwrite with %q filesystem", source, deviceFS, fsType)
	}

	args := append(mkfsArgs, source)
	cmd := "mkfs." + fsType

	s.log.WithFields(logrus.Fields{
		"command": cmd,
		"args":    args,
	}).Debug("creating filesystem")

	out, err := s.mounter.Exec.CommandContext(ctx, cmd, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("couldn't create %s filesystem on %s: %v: %q", fsType, source, err, out)
	}

	return nil
}

// Build mkfs args in the form [opt1, opt2, opt3..., source].
func mkfsArgs(opts, source string) []string {
	if opts == "" {
		return []string{source}
	}

	return append(strings.Split(opts, " "), source)
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

	err = s.client.ResourceDefinitions.ModifyVolumeDefinition(ctx, vol.ID, int(volumeDefinitions[0].VolumeNumber), volumeDefinitionModify)
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

	pools, err := s.client.Nodes.GetStoragePools(ctx, nodename)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage pools for node: %w", err)
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

	const auxPrefix = lapiconsts.NamespcAuxiliary + "/"
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
