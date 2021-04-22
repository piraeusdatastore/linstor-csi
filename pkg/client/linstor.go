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

	lapiconsts "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/haySwim/data"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/resizefs"
	"k8s.io/kubernetes/pkg/util/slice"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor/util"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/autoplace"
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
		Exec:      mount.NewOsExec(),
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
	var vols = make([]*volume.Info, 0)

	resDefs, err := s.client.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return vols, nil
	}

	for _, rd := range resDefs {
		vol, err := s.resourceDefinitionToVolume(rd)
		if err != nil {
			// Not a volume created by us, apparently.
			continue
		}

		vols = append(vols, vol)
	}
	volume.Sort(vols)

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

// resourceDefinitionToVolume reads the serialized volume info on the lapi.ResourceDefinition
// and contructs a pointer to a volume.Info from it.
func (s *Linstor) resourceDefinitionToVolume(resDef lapi.ResourceDefinition) (*volume.Info, error) {
	csiVolumeAnnotation, ok := resDef.Props[linstor.AnnotationsKey]
	if !ok {
		return nil, fmt.Errorf("unable to find CSI volume annotation on resource %+v", resDef)
	}
	vol := &volume.Info{
		Parameters: make(map[string]string),
	}
	if err := json.Unmarshal([]byte(csiVolumeAnnotation), vol); err != nil {
		return nil, fmt.Errorf("failed to unmarshal annotations for ResDef %+v", resDef)
	}

	if vol.Name == "" {
		return nil, fmt.Errorf("failed to extract resource name from %+v", vol)
	}

	s.log.WithFields(logrus.Fields{
		"resourceDefinition": fmt.Sprintf("%+v", resDef),
		"volume":             fmt.Sprintf("%+v", vol),
	}).Debug("converted resource definition to volume")

	return vol, nil
}

// FindByID retrives a volume.Info that has a name that matches the CSI volume
// Name, not nessesarily the LINSTOR resource name or UUID.
func (s *Linstor) FindByName(ctx context.Context, name string) (*volume.Info, error) {
	s.log.WithFields(logrus.Fields{
		"csiVolumeName": name,
	}).Debug("looking up resource by CSI volume name")

	list, err := s.client.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return nil, nil404(err)
	}

	for _, rd := range list {
		vol, err := s.resourceDefinitionToVolume(rd)
		// Probably found a resource we didn't create.
		if err != nil || vol == nil {
			continue
		}

		if vol.Name == name {
			return vol, nil
		}
	}
	return nil, nil
}

// FindByID retrives a volume.Info that has an id that matches the CSI volume
// id. Matches the LINSTOR resource name.
func (s *Linstor) FindByID(ctx context.Context, id string) (*volume.Info, error) {
	s.log.WithFields(logrus.Fields{
		"csiVolumeID": id,
	}).Debug("looking up resource by CSI volume id")

	res, err := s.client.ResourceDefinitions.Get(ctx, id)
	if err != nil {
		return nil, nil404(err)
	}

	return s.resourceDefinitionToVolume(res)
}

// Create creates the resource definition, volume definition, and assigns the
// resulting resource to LINSTOR nodes.
func (s *Linstor) Create(ctx context.Context, vol *volume.Info, req *csi.CreateVolumeRequest) error {
	logger := s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	})

	logger.Debug("convert volume parameters")
	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		logger.Debugf("conversion failed: %v", err)
		return err
	}

	logger.Debug("reconcile resource group from storage class")
	rGroup, err := s.reconcileResourceGroup(ctx, params)
	if err != nil {
		logger.Debugf("reconcile resource group failed: %v", err)
		return err
	}

	logger.Debug("reconcile resource definition for volume")
	rDef, err := s.reconcileResourceDefinition(ctx, vol, rGroup.Name)
	if err != nil {
		logger.Debugf("reconcile resource definition failed: %v", err)
		return err
	}

	vol.ID = rDef.Name

	logger.Debug("reconcile volume definition for volume")
	_, err = s.reconcileVolumeDefinition(ctx, vol)
	if err != nil {
		logger.Debugf("reconcile volume definition failed: %v", err)
		return err
	}

	logger.Debug("reconcile volume placement")
	err = s.reconcileResourcePlacement(ctx, vol, &params, req)
	if err != nil {
		logger.Debugf("reconcile volume placement failed: %v", err)
		return err
	}

	// saveVolumeMetadata() makes the volume eligible for further use, i.e. this method will not be called again once saveVolumeMetadata
	// succeeded.
	logger.Debug("update volume information with resource definition information")
	if err := s.saveVolumeMetadata(ctx, vol); err != nil {
		return err
	}

	return nil
}

// Delete removes a resource, all of its volumes from LINSTOR.
// Only removes the ResourceDefinition if no snapshots of the resource exist.
func (s *Linstor) Delete(ctx context.Context, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("deleting volume")

	resources, err := s.client.Resources.GetAll(ctx, vol.ID)
	if err != nil {
		return nil404(err)
	}

	// Need to ensure that diskless resources are always deleted first, otherwise the last diskfull resource won't be
	// deleted
	sort.Slice(resources, func(i, j int) bool {
		return !util.DeployedDiskfully(resources[i]) && util.DeployedDiskfully(resources[j])
	})

	for _, res := range resources {
		err := s.client.Resources.Delete(ctx, vol.ID, res.NodeName)
		if err != nil {
			// If two deletions run in parallel, one could get a 404 message, which we treat as "everything finished"
			return nil404(err)
		}
	}

	err = s.deleteResourceDefinitionAndGroupIfUnused(ctx, vol.ID)
	if err != nil {
		return err
	}

	// After this call succeeds, we won't be able to recover the Info struct. To keep this function idempotent, this
	// is the last call in the function.
	return s.deleteVolumeMetadata(ctx, vol)
}

// AccessibleTopologies returns a list of pointers to csi.Topology from where the
// volume is reachable, based on the localStoragePolicy reported by the volume.
func (s *Linstor) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	volumeScheduler, err := s.schedulerByPlacementPolicy(vol)
	if err != nil {
		return nil, err
	}
	return volumeScheduler.AccessibleTopologies(ctx, vol)
}

func (s *Linstor) schedulerByPlacementPolicy(vol *volume.Info) (scheduler.Interface, error) {
	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return nil, err
	}

	switch params.PlacementPolicy {
	case topology.AutoPlace:
		return autoplace.NewScheduler(s.client), nil
	case topology.Manual:
		return manual.NewScheduler(s.client), nil
	case topology.FollowTopology:
		return followtopology.NewScheduler(s.client, s.log), nil
	case topology.Balanced:
		return balancer.NewScheduler(s.client, s.log)
	default:
		return nil, fmt.Errorf("unsupported volume scheduler: %s", params.PlacementPolicy)
	}
}

// Attach idempotently creates a resource on the given node disklessly.
func (s *Linstor) Attach(ctx context.Context, vol *volume.Info, node string) error {
	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	}).Info("attaching volume")

	// If the resource is already on the node, don't worry about attaching.
	res, err := s.client.Resources.Get(ctx, vol.ID, node)
	if nil404(err) != nil {
		return err
	}

	if slice.ContainsString(res.Flags, lapiconsts.FlagDelete, nil) {
		return fmt.Errorf("failed to attach volume %s, to node %s: delete in progress", vol.ID, node)
	}

	if res.NodeName == node {
		s.log.WithFields(logrus.Fields{
			"volume":     fmt.Sprintf("%+v", vol),
			"resource":   fmt.Sprintf("%+v", res),
			"targetNode": node,
		}).Info("volume already attached")
		return nil
	}

	rc, err := vol.ToDisklessResourceCreate(node)
	if err != nil {
		return err
	}

	rc.Resource.Props[linstor.PropertyCreatedFor] = linstor.CreatedForTemporaryDisklessAttach
	return s.client.Resources.Create(ctx, rc)
}

// Detach removes a volume from the node.
func (s *Linstor) Detach(ctx context.Context, vol *volume.Info, node string) error {
	log := s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	})

	res, err := s.client.Resources.Get(ctx, vol.ID, node)
	if err != nil {
		// if the resource could not be found there is nothing to detach
		return nil404(err)
	}

	createdFor, ok := res.Props[linstor.PropertyCreatedFor]
	if !ok || createdFor != linstor.CreatedForTemporaryDisklessAttach {
		log.Info("resource not temporary (not created by Attach) not deleting")
		return nil
	}

	if util.DeployedDiskfully(res) {
		log.Info("temporary resource created by Attach is now diskfull, not deleting")
		return nil
	}
	
	log.Info("removing temporary resource")
	return s.client.Resources.Delete(ctx, vol.ID, node)
}

// CapacityBytes returns the amount of free space in the storage pool specified
// the the params.
func (s *Linstor) CapacityBytes(ctx context.Context, parameters map[string]string) (int64, error) {
	params, err := volume.NewParameters(parameters)
	if err != nil {
		return 0, fmt.Errorf("unable to get capacity: %v", err)
	}

	pools, err := s.client.Nodes.GetStoragePoolView(ctx)
	if err != nil {
		return 0, fmt.Errorf("unable to get capacity for storage pool %s: %v", params.StoragePool, err)
	}

	var total int64
	for _, sp := range pools {
		if params.StoragePool == sp.StoragePoolName || params.StoragePool == "" {
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
func (s *Linstor) VolFromSnap(ctx context.Context, snap *csi.Snapshot, vol *volume.Info) error {
	logger := s.log.WithFields(logrus.Fields{
		"volume":   fmt.Sprintf("%+v", vol),
		"snapshot": fmt.Sprintf("%+v", snap),
	})

	logger.Debug("convert volume parameters")
	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		logger.Debugf("conversion failed: %v", err)
		return err
	}

	logger.Debug("reconcile resource group from storage class")
	rGroup, err := s.reconcileResourceGroup(ctx, params)
	if err != nil {
		logger.Debugf("reconcile resource group failed: %v", err)
		return err
	}

	logger.Debug("reconcile resource definition for volume")
	rDef, err := s.reconcileResourceDefinition(ctx, vol, rGroup.Name)
	if err != nil {
		logger.Debugf("reconcile resource definition failed: %v", err)
		return err
	}

	logger.Debug("reconcile volume definition from snapshot")
	err = s.reconcileSnapshotVolumeDefinitions(ctx, snap, rDef.Name)
	if err != nil {
		return err
	}

	logger.Debug("reconcile resources from snapshot")
	err = s.reconcileSnapshotResources(ctx, snap, rDef.Name)
	if err != nil {
		return err
	}

	// Note: saveVolumeMetadata() sets annotations that are:
	// 1. needed to determine which volume to mount in the CSINode code
	// 2. overwritten on restore from snapshot
	// to make sure its not overwritten, this is the last step in this method.
	logger.Debug("update volume information with resource definition information")
	vol.ID = rDef.Name
	if err := s.saveVolumeMetadata(ctx, vol); err != nil {
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

func (s *Linstor) reconcileSnapshotResources(ctx context.Context, snapshot *csi.Snapshot, targetRD string) error {
	logger := s.log.WithFields(logrus.Fields{"snapshot": snapshot, "target": targetRD})

	logger.Debug("checking for existing resources")
	resources, err := s.client.Resources.GetAll(ctx, targetRD)
	if err != nil {
		return fmt.Errorf("could not fetch resources: %w", err)
	}

	if len(resources) == 0 {
		logger.Debug("restoring resources from snapshot")

		restoreConf := lapi.SnapshotRestore{ToResource: targetRD}
		err := s.client.Resources.RestoreSnapshot(ctx, snapshot.GetSourceVolumeId(), snapshot.GetSnapshotId(), restoreConf)
		if err != nil {
			return fmt.Errorf("could not restore resources: %w", err)
		}
	}

	return nil
}

func (s *Linstor) fallbackNameUUIDNew() string {
	return s.fallbackPrefix + uuid.New()
}

// Reconcile a ResourceGroup based on the values passed to the StorageClass
func (s *Linstor) reconcileResourceGroup(ctx context.Context, params volume.Parameters) (*lapi.ResourceGroup, error) {
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

	rgModify, changed := params.ToResourceGroupModify(&rg)
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

func (s *Linstor) reconcileResourceDefinition(ctx context.Context, info *volume.Info, rgName string) (*lapi.ResourceDefinition, error) {
	logger := s.log.WithFields(logrus.Fields{
		"volume": info.Name,
	})
	logger.Info("reconcile resource definition for volume")

	logger.Debugf("check if resource definition already exists")
	allRds, err := s.client.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	for _, rd := range allRds {
		if rd.ExternalName != info.Name {
			continue
		}

		if slice.ContainsString(rd.Flags, lapiconsts.FlagDelete, nil) {
			return nil, fmt.Errorf("resource definition %s exists, but deletion is in progress", info.Name)
		}

		logger.Debugf("resource definition already exists")
		return &rd, nil
	}

	logger.Debugf("resource definition does not exist, create now")
	rdCreate := lapi.ResourceDefinitionCreate{
		ResourceDefinition: lapi.ResourceDefinition{
			ExternalName:      info.Name,
			ResourceGroupName: rgName,
		},
	}
	err = s.client.ResourceDefinitions.Create(ctx, rdCreate)
	if err != nil {
		return nil, err
	}

	logger.Debugf("find newly created ResourceDefinition")
	allRds, err = s.client.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	for _, rd := range allRds {
		if rd.ExternalName == info.Name {
			return &rd, nil
		}
	}

	logger.Warn("could not find created ResourceDefinition")
	return nil, fmt.Errorf("could not find ResourceDefinition, but it was just created without error: %s", info.Name)
}

func (s *Linstor) reconcileVolumeDefinition(ctx context.Context, info *volume.Info) (*lapi.VolumeDefinition, error) {
	logger := s.log.WithFields(logrus.Fields{
		"volume": info.Name,
	})
	logger.Info("reconcile volume definition for volume")

	logger.Debug("check if volume definition already exists")
	vDef, err := s.client.Client.ResourceDefinitions.GetVolumeDefinition(ctx, info.ID, 0)
	if err == lapi.NotFoundError {
		vdCreate := lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{
				VolumeNumber: 0,
				SizeKib:      uint64(data.NewKibiByte(data.ByteSize(info.SizeBytes)).Value()),
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

	return &vDef, nil
}

func (s *Linstor) reconcileResourcePlacement(ctx context.Context, vol *volume.Info, volParams *volume.Parameters, req *csi.CreateVolumeRequest) error {
	logger := s.log.WithFields(logrus.Fields{
		"volume": vol.Name,
	})
	logger.Info("reconcile resource placement for volume")

	resources, err := s.client.Resources.GetAll(ctx, vol.ID)
	if err != nil {
		return err
	}

	// Check that at least as many resources are placed as specified in the volume parameters
	expectedAutoResources := int(volParams.PlacementCount)
	expectedManualResources := len(volParams.ClientList) + len(volParams.NodeList)
	if len(resources) >= expectedAutoResources && len(resources) >= expectedManualResources {
		logger.Debug("resources already placed")
		return nil
	}

	volumeScheduler, err := s.schedulerByPlacementPolicy(vol)
	if err != nil {
		return err
	}

	err = volumeScheduler.Create(ctx, vol, req)
	if err != nil {
		return err
	}

	return nil
}

// store a representation of a volume into the aux props of a resource definition.
func (s *Linstor) saveVolumeMetadata(ctx context.Context, vol *volume.Info) error {
	serializedVol, err := json.Marshal(vol)
	if err != nil {
		return err
	}

	return s.client.ResourceDefinitions.Modify(ctx, vol.ID, lapi.GenericPropsModify{
		OverrideProps: map[string]string{linstor.AnnotationsKey: string(serializedVol)},
	})
}

// Delete the representation of a volume from the aux props of a resource definition.
// Returns nil if:
// * Metadata was removed
// * RD didn't exist to start with
func (s *Linstor) deleteVolumeMetadata(ctx context.Context, vol *volume.Info) error {
	err := s.client.ResourceDefinitions.Modify(ctx, vol.ID, lapi.GenericPropsModify{
		DeleteProps: []string{linstor.AnnotationsKey},
	})
	if err != nil {
		return nil404(err)
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
		if lsnap.Name == id  {
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
		Limit: limit,
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
		Limit: limit,
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

	if slice.ContainsString(lsnap.Flags, lapiconsts.FlagDelete, nil) {
		return nil, fmt.Errorf("snapshot is being deleted")
	}

	snapSizeBytes := lsnap.VolumeDefinitions[0].SizeKib * uint64(data.KiB)
	creationTimeMicroSecs := lsnap.Snapshots[0].CreateTimestamp
	ready := slice.ContainsString(lsnap.Flags, lapiconsts.FlagSuccessful, nil)

	return &csi.Snapshot{
		SnapshotId:     lsnap.Name,
		SourceVolumeId: lsnap.ResourceName,
		SizeBytes:      int64(snapSizeBytes),
		CreationTime:   &timestamp.Timestamp{Seconds: creationTimeMicroSecs / 1000},
		ReadyToUse:     ready,
	}, nil
}

func linstorSnapshotHasError(lsnap *lapi.Snapshot) bool {
	return slice.ContainsString(lsnap.Flags, lapiconsts.FlagFailedDisconnect, nil) || slice.ContainsString(lsnap.Flags, lapiconsts.FlagFailedDeployment, nil)
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

// GetAssignmentOnNode returns a pointer to a volume.Assignment for a given node.
func (s *Linstor) GetAssignmentOnNode(ctx context.Context, vol *volume.Info, node string) (*volume.Assignment, error) {
	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	}).Debug("getting assignment info")

	linVol, err := s.client.Resources.GetVolume(ctx, vol.ID, node, 0)
	if err != nil {
		return nil, err
	}

	va := &volume.Assignment{
		Vol:  vol,
		Node: node,
		Path: linVol.DevicePath,
	}

	s.log.WithFields(logrus.Fields{
		"volumeAssignment": fmt.Sprintf("%+v", va),
	}).Debug("found assignment info")

	return va, nil
}

// Mount makes volumes consumable from the source to the target.
// Filesystems are formatted and block devices are bind mounted.
// Operates locally on the machines where it is called.
func (s *Linstor) Mount(vol *volume.Info, source, target, fsType string, readonly bool, options []string) error {
	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("mounting volume failed: %v", err)
	}

	// If there is no fsType, then this is a block mode volume.
	var block bool
	if fsType == "" {
		block = true
	}

	// Merge mount options from Storage Classes and CSI calls.
	options = append(options, params.MountOpts)

	s.log.WithFields(logrus.Fields{
		"volume":          fmt.Sprintf("%+v", vol),
		"source":          source,
		"target":          target,
		"options":         options,
		"filesystem":      fsType,
		"blockAccessMode": block,
	}).Info("mounting volume")

	// Check if the path is a device
	isDevice, err := s.mounter.PathIsDevice(source)
	if err != nil {
		return fmt.Errorf("checking device path failed: %v", err)
	}
	if !isDevice {
		return fmt.Errorf("%s does not appear to be a block device", source)
	}

	// Determine if we have exclusive access to the device. This is mostly
	// a way to determine if a disklessly attached device's connection is down.
	_, err = s.mounter.DeviceOpened(source)
	if err != nil {
		return fmt.Errorf("checking for exclusive open failed: %v, check device health", err)
	}

	if readonly {
		options = append(options, "ro")

		// This requires DRBD 9.0.26+, older versions ignore this flag
		err := s.setDevReadOnly(source)
		if err != nil {
			return fmt.Errorf("failed to set source device readonly: %w", err)
		}
	} else {
		// We might be re-using an existing device that was set RO previously
		err = s.setDevReadWrite(source)
		if err != nil {
			return fmt.Errorf("failed to set source device readwrite: %w", err)
		}
	}

	// This is a regular filesystem so format the device and create the mountpoint.
	if !block {
		if err := s.formatDevice(vol, source, fsType); err != nil {
			return fmt.Errorf("mounting volume failed: %v", err)
		}
		if err := s.mounter.MakeDir(target); err != nil {
			return fmt.Errorf("could not create target directory %s, %v", target, err)
		}
		// This is a block volume so create a file to bindmount to.
	} else {
		err := s.mounter.MakeFile(target)
		if err != nil {
			return fmt.Errorf("could not create bind target for block volume %s, %v", target, err)
		}
	}

	needsMount, err := s.mounter.IsNotMountPoint(target)
	if err != nil {
		return fmt.Errorf("unable to determine mount status of %s %v", target, err)
	}

	if !needsMount {
		return nil
	}

	if block {
		return s.mounter.Mount(source, target, fsType, options)
	}

	return s.mounter.FormatAndMount(source, target, fsType, options)
}

func (s *Linstor) formatDevice(vol *volume.Info, source, fsType string) error {
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

	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("formatting device failed: %v", err)
	}

	args := mkfsArgs(params.FSOpts, source)
	cmd := "mkfs." + fsType

	s.log.WithFields(logrus.Fields{
		"command": cmd,
		"args":    args,
	}).Debug("creating filesystem")

	out, err := s.mounter.Exec.Run(cmd, args...)
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

func (s *Linstor) setDevReadOnly(srcPath string) error {
	_, err := s.mounter.Exec.Run("blockdev", "--setro", srcPath)
	return err
}

func (s *Linstor) setDevReadWrite(srcPath string) error {
	_, err := s.mounter.Exec.Run("blockdev", "--setrw", srcPath)
	return err
}

// IsNotMountPoint determines if a directory is a mountpoint.
//
// Non-existent paths return (true, nil).
func (s *Linstor) IsNotMountPoint(target string) (bool, error) {
	notMounted, err := s.mounter.IsNotMountPoint(target)
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

	notMounted, err := s.IsNotMountPoint(target)
	if err != nil {
		return fmt.Errorf("unable to determine mount status of %s %v", target, err)
	}

	if !notMounted {
		// Still needs to be unmounted
		err := s.mounter.Unmount(target)
		if err != nil {
			return fmt.Errorf("unable to unmount target '%s': %w", target, err)
		}
	}

	// Remove the file/directory created in Mount()
	err = os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unable to remove target '%s': %w", target, err)
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
	resizer := resizefs.NewResizeFs(s.mounter)
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
		if !slice.ContainsString(res.Flags, lapiconsts.FlagDelete, nil) {
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
		if !slice.ContainsString(snap.Flags, lapiconsts.FlagDelete, nil) {
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
		apiErr, ok := err.(lapi.ApiCallError)
		if ok && apiErr.Is(lapiconsts.FailExistsRscDfn) {
			// Do not delete the RG if other RDs are still present
			return nil
		}

		return nil404(err)
	}

	return nil
}
