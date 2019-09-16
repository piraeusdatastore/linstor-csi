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
	"regexp"
	"strings"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/linstor-csi/pkg/linstor"
	lc "github.com/LINBIT/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/LINBIT/linstor-csi/pkg/linstor/util"
	"github.com/LINBIT/linstor-csi/pkg/topology"
	"github.com/LINBIT/linstor-csi/pkg/topology/scheduler"
	"github.com/LINBIT/linstor-csi/pkg/topology/scheduler/autoplace"
	"github.com/LINBIT/linstor-csi/pkg/topology/scheduler/balancer"
	"github.com/LINBIT/linstor-csi/pkg/topology/scheduler/followtopology"
	"github.com/LINBIT/linstor-csi/pkg/topology/scheduler/manual"
	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	ptypes "github.com/golang/protobuf/ptypes"
	"github.com/haySwim/data"
	"github.com/pborman/uuid"
	logrus "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"k8s.io/kubernetes/pkg/util/mount"
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
		Snapshots:  make([]*volume.SnapInfo, 0),
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

// GetByName retrives a volume.Info that has a name that matches the CSI volume
// Name, not nessesarily the LINSTOR resource name or UUID.
func (s *Linstor) GetByName(ctx context.Context, name string) (*volume.Info, error) {
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

// GetByID retrives a volume.Info that has an id that matches the CSI volume
// id. Matches the LINSTOR resource name.
func (s *Linstor) GetByID(ctx context.Context, id string) (*volume.Info, error) {
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
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("creating volume")

	if err := s.createResourceDefinition(ctx, vol); err != nil {
		return err
	}

	// Create the volume definition, now that vol has been updated with its ID.
	if err := s.client.ResourceDefinitions.CreateVolumeDefinition(ctx, vol.ID,
		lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{SizeKib: uint64(data.NewKibiByte(data.ByteSize(vol.SizeBytes)).Value())}}); err != nil {
		return err
	}

	volumeScheduler, err := s.schedulerByPlacementPolicy(vol)
	if err != nil {
		return err
	}
	return volumeScheduler.Create(ctx, vol, req)
}

// Delete removes a resource, all of its volumes, and snapshots from LINSTOR.
func (s *Linstor) Delete(ctx context.Context, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("deleting volume")

	// Resources with snapshots cannot be deleted so we have to remove those first.
	snaps, err := s.client.Resources.GetSnapshots(ctx, vol.ID)
	if nil404(err) != nil {
		return err
	}

	g, egctx := errgroup.WithContext(ctx)
	for _, snap := range snaps {
		ss := snap.Name
		g.Go(func() error {
			if err := s.client.Resources.DeleteSnapshot(egctx, vol.ID, ss); nil404(err) != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// No snapshots, remove the resource.
	return nil404(s.client.ResourceDefinitions.Delete(ctx, vol.ID))
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
	return s.client.Resources.Create(ctx, rc)
}

// Detach removes a volume from the node.
func (s *Linstor) Detach(ctx context.Context, vol *volume.Info, node string) error {
	res, err := s.client.Resources.Get(ctx, vol.ID, node)
	if err != nil {
		return err
	}
	s.log.WithFields(logrus.Fields{
		"resource":   fmt.Sprintf("%+v", res),
		"targetNode": node,
	}).Info("detaching volume")

	if util.DeployedDiskfully(res) {
		s.log.WithFields(logrus.Fields{
			"resource":   fmt.Sprintf("%+v", res),
			"targetNode": node,
		}).Info("volume is diskfull on node, refusing to detach")
		return nil
	}

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

// SnapCreate calls linstor to create a new snapshot on the volume indicated by
// the SourceVolumeId contained in the CSI Snapshot.
func (s *Linstor) SnapCreate(ctx context.Context, snap *volume.SnapInfo) (*volume.SnapInfo, error) {
	vol, err := s.GetByID(ctx, snap.CsiSnap.SourceVolumeId)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve volume info from id %s", snap.CsiSnap.SourceVolumeId)
	}

	if err := s.client.Resources.CreateSnapshot(ctx, lapi.Snapshot{
		Name:         snap.Name,
		ResourceName: vol.ID,
	}); err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %v", err)
	}

	linSnap, err := s.client.Resources.GetSnapshot(ctx, vol.ID, snap.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %v", err)
	}

	// Fill in missing snapshot fields on creation, keep original SourceVolumeId.
	snap.CsiSnap = &csi.Snapshot{
		SnapshotId:     linSnap.Name,
		SourceVolumeId: snap.CsiSnap.SourceVolumeId,
		SizeBytes:      int64(data.NewKibiByte(data.KiB * data.ByteSize(linSnap.VolumeDefinitions[0].SizeKib)).InclusiveBytes()),
		CreationTime:   ptypes.TimestampNow(),
		ReadyToUse:     true,
	}

	s.log.WithFields(logrus.Fields{
		"linstorSnapshot": fmt.Sprintf("%+v", linSnap),
		"csiSnapshot":     fmt.Sprintf("%+v", *snap),
	}).Debug("created new snapshot")

	// Update volume information to reflect the newly-added snapshot.
	vol.Snapshots = append(vol.Snapshots, snap)
	if err := s.saveVolume(ctx, vol); err != nil {
		// We should at least try to delete the snapshot here, even though it succeeded
		// without error it's going be unregistered as far as the CO is concerned.
		if err := s.client.Resources.DeleteSnapshot(ctx, vol.ID, snap.Name); nil404(err) != nil {
			s.log.WithError(err).Error("failed to clean up snapshot after recording its metadata failed")
		}
		return nil, fmt.Errorf("unable to record new snapshot metadata: %v", err)
	}

	return snap, nil
}

// SnapDelete calls LINSTOR to delete the snapshot based on the CSI Snapshot ID.
func (s *Linstor) SnapDelete(ctx context.Context, snap *volume.SnapInfo) error {
	vol, err := s.GetByID(ctx, snap.CsiSnap.SourceVolumeId)
	if err != nil {
		return fmt.Errorf("failed to retrieve volume info from id %s", snap.CsiSnap.SourceVolumeId)
	}

	s.log.WithFields(logrus.Fields{
		"snapshot": fmt.Sprintf("%+v", snap),
	}).Info("deleting snapshot")

	if err := s.client.Resources.DeleteSnapshot(ctx, vol.Name, snap.Name); nil404(err) != nil {
		return fmt.Errorf("failed to remove snaphsot: %v", err)
	}

	// Record the changes to the volume's snaphots
	updatedSnaps := make([]*volume.SnapInfo, 0)
	for _, s := range vol.Snapshots {
		if s.CsiSnap.SourceVolumeId != snap.CsiSnap.SourceVolumeId {
			updatedSnaps = append(updatedSnaps, s)
		}
	}
	vol.Snapshots = updatedSnaps
	if err := s.saveVolume(ctx, vol); err != nil {
		if err := s.client.Resources.DeleteSnapshot(ctx, vol.ID, snap.Name); nil404(err) != nil {
			s.log.WithError(err).Error("failed to update snapshot list after recording its metadata failed")
		}
		return fmt.Errorf("unable to record new snapshot metadata: %v", err)
	}

	return nil
}

// VolFromSnap creates the volume using the data contained within the snapshot.
func (s *Linstor) VolFromSnap(ctx context.Context, snap *volume.SnapInfo, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume":   fmt.Sprintf("%+v", vol),
		"snapshot": fmt.Sprintf("%+v", snap),
	}).Info("creating volume from snapshot")

	if err := s.createResourceDefinition(ctx, vol); err != nil {
		return err
	}

	r, err := s.client.Resources.GetAll(ctx, vol.ID)
	if err != nil {
		return err
	}

	snapRestore := lapi.SnapshotRestore{
		ToResource: vol.ID,
		Nodes:      util.DeployedDiskfullyNodes(r),
	}

	if err := s.client.Resources.RestoreVolumeDefinitionSnapshot(ctx, snap.CsiSnap.SourceVolumeId, snap.Name, snapRestore); err != nil {
		return err
	}

	if err := s.client.Resources.RestoreSnapshot(ctx, snap.CsiSnap.SourceVolumeId, snap.Name, snapRestore); err != nil {
		return err
	}

	return nil
}

// VolFromVol creates the volume using the data contained within the source volume.
func (s *Linstor) VolFromVol(ctx context.Context, sourceVol, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume":       fmt.Sprintf("%+v", vol),
		"sourceVolume": fmt.Sprintf("%+v", sourceVol),
	}).Info("creating volume from snapshot")

	tmpName := s.fallbackPrefix + uuid.New()
	if err := s.client.Resources.CreateSnapshot(ctx,
		lapi.Snapshot{
			Name:         tmpName,
			ResourceName: sourceVol.ID,
		}); err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	return s.VolFromSnap(
		ctx,
		&volume.SnapInfo{Name: tmpName, CsiSnap: &csi.Snapshot{SourceVolumeId: sourceVol.ID}},
		vol,
	)
}

// Creates a resourceDefinition, updating the vol.ID if successful.
func (s *Linstor) createResourceDefinition(ctx context.Context, vol *volume.Info) error {
	resDefCreate, err := vol.ToResourceDefinitionCreate()
	if err != nil {
		return err
	}

	if err := s.client.ResourceDefinitions.Create(ctx, resDefCreate); err != nil {
		return err
	}

	// Find the volume ID of the volume we just created.
	rds, err := s.client.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return err
	}

	for _, rd := range rds {
		if rd.ExternalName == vol.Name {
			vol.ID = rd.Name
		}
	}

	// Annotate resource definition with updated volume ID.
	if err := s.saveVolume(ctx, vol); err != nil {
		return err
	}

	return nil
}

// store a representation of a volume into the aux props of a resource definition.
func (s *Linstor) saveVolume(ctx context.Context, vol *volume.Info) error {
	serializedVol, err := json.Marshal(vol)
	if err != nil {
		return err
	}
	return s.setProps(ctx, vol, map[string]string{linstor.AnnotationsKey: string(serializedVol)})
}

func (s *Linstor) setProps(ctx context.Context, vol *volume.Info, props map[string]string) error {
	return s.client.ResourceDefinitions.Modify(ctx, vol.ID,
		lapi.GenericPropsModify{
			OverrideProps: props,
		})
}

// CanonicalizeSnapshotName makes sure that the snapshot name meets LINSTOR's
// naming conventions.
func (s *Linstor) CanonicalizeSnapshotName(ctx context.Context, suggestedName string) string {
	// TODO: Snapshots actually have different naming requirements, it might
	// be nice to conform to those eventually.
	name, err := linstorifyResourceName(suggestedName)
	if err != nil {
		return s.fallbackPrefix + uuid.New()
	}
	// We already handled the idempotency/existing case
	// This is to make sure that nobody else created a snapshot with that name (e.g., another user/plugin)
	existingSnap, err := s.GetSnapByName(ctx, name)
	if existingSnap != nil || err != nil {
		return s.fallbackPrefix + uuid.New()
	}

	return name
}

// ListVolumes returns all volumes that have metadata that is understandable
// by this plugin, so volumes from multiple compatible plugins may be returned.
func (s *Linstor) ListVolumes(ctx context.Context) ([]*volume.Info, error) {
	allResDefs, err := s.client.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve resource definitions: %v", err)
	}

	var vols = make([]*volume.Info, 0)

	for _, rd := range allResDefs {
		// If we encounter a failure here, we can assume that the resource was
		// not created by a CSI driver, but we can't check here that it was
		// created by this instance of the CSI driver in particular.
		// Linstor names are CSI IDs.
		vol, err := s.resourceDefinitionToVolume(rd)
		if err != nil {
			s.log.WithFields(logrus.Fields{
				"resourceDefinition": fmt.Sprintf("%+v", rd),
			}).WithError(err).Error("failed to internally represent volume but continuing — likely non-CSI volume")
			continue
		}

		if vol != nil {
			vols = append(vols, vol)
		}
	}

	return vols, nil
}

// GetSnapByName retrieves a pointer to a volume.SnapInfo by its name.
func (s *Linstor) GetSnapByName(ctx context.Context, name string) (*volume.SnapInfo, error) {
	vols, err := s.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}

	return s.doGetSnapByName(vols, name), nil
}

func (s *Linstor) doGetSnapByName(vols []*volume.Info, name string) *volume.SnapInfo {
	for _, vol := range vols {
		for _, snap := range vol.Snapshots {
			if snap.Name == name {
				return snap
			}
		}
	}
	return nil
}

// GetSnapByID retrieves a pointer to a volume.SnapInfo by its id.
func (s *Linstor) GetSnapByID(ctx context.Context, id string) (*volume.SnapInfo, error) {
	vols, err := s.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}

	return s.doGetSnapByID(vols, id), nil
}

func (s *Linstor) doGetSnapByID(vols []*volume.Info, id string) *volume.SnapInfo {
	for _, vol := range vols {
		for _, snap := range vol.Snapshots {
			if snap.CsiSnap.SnapshotId == id {
				return snap
			}
		}
	}
	return nil
}

// ListSnaps returns list of pointers to volume.SnapInfo based off of the
// serialized snapshot info stored in resource definitions.
func (s *Linstor) ListSnaps(ctx context.Context) ([]*volume.SnapInfo, error) {

	vols, err := s.ListVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %v", err)
	}

	g, egctx := errgroup.WithContext(ctx)
	allSnaps := make(chan lapi.Snapshot, 1)
	for _, vol := range vols {
		res := vol.ID
		g.Go(func() error {
			resSnaps, err := s.client.Resources.GetSnapshots(egctx, res)
			if err == nil {
				for _, s := range resSnaps {
					allSnaps <- s
				}
			}
			return nil404(err)
		})
	}

	var snaps = make([]*volume.SnapInfo, 0)
	var done = make(chan bool, 1)
	go func() {
		for snap := range allSnaps {
			snapCreatedByMe := s.doGetSnapByName(vols, snap.Name)
			if snapCreatedByMe != nil {
				snaps = append(snaps, snapCreatedByMe)
			} else {
				s.log.WithFields(logrus.Fields{
					"missingSnapshot": fmt.Sprintf("%+v", snap),
				}).Debug("unable to look up snap by its Name, potentially not made by me")
			}
		}
		done <- true
	}()

	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(allSnaps)

	<-done

	return snaps, nil
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
// Filesystems are formatted and block devics are bind mounted.
// Operates locally on the machines where it is called.
func (s *Linstor) Mount(vol *volume.Info, source, target, fsType string, options []string) error {
	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("mounting volume failed: %v", err)
	}

	// Override default CSI fsType with the one passed in the StorageClass
	if params.FS != "" {
		fsType = params.FS
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

//Unmount unmounts the target. Operates locally on the machines where it is called.
func (s *Linstor) Unmount(target string) error {
	s.log.WithFields(logrus.Fields{
		"target": target,
	}).Info("unmounting volume")

	notMounted, err := s.mounter.IsNotMountPoint(target)
	if err != nil {
		return fmt.Errorf("unable to determine mount status of %s %v", target, err)
	}

	if notMounted {
		return nil
	}

	return s.mounter.Unmount(target)
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
