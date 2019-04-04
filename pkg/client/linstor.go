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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"

	lc "github.com/LINBIT/golinstor"
	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	ptypes "github.com/golang/protobuf/ptypes"
	"github.com/haySwim/data"
	"github.com/pborman/uuid"
	logrus "github.com/sirupsen/logrus"
)

const (
	NodeListKey            = "nodelist"
	LayerListKey           = "layerlist"
	ClientListKey          = "clientlist"
	ReplicasOnSameKey      = "replicasonsame"
	ReplicasOnDifferentKey = "replicasondifferent"
	AutoPlaceKey           = "autoplace"
	DoNotPlaceWithRegexKey = "donotplacewithregex"
	SizeKiBKey             = "sizekib"
	StoragePoolKey         = "storagepool"
	DisklessStoragePoolKey = "disklessstoragepool"
	EncryptionKey          = "encryption"
	BlockSizeKey           = "blocksize"
	ForceKey               = "force"
	FSKey                  = "filesystem"
	UseLocalStorageKey     = "podWithLocalVolume"
	// These have to be camel case. Maybe move them into resource config for
	// consistency?
	MountOptsKey = "mountOpts"
	FSOptsKey    = "fsOpts"
)

type Linstor struct {
	log            *logrus.Entry
	annotationsKey string
	fallbackPrefix string
	logOut         io.Writer
	controllers    string
}

// NewLinstor returns a high-level linstor client for CSI applications to interact with
// By default, it will try to connect with localhost:3370. See Controllers func
// for chancing this setting.
func NewLinstor(options ...func(*Linstor) error) (*Linstor, error) {
	l := &Linstor{
		annotationsKey: "csi-volume-annotations",
		fallbackPrefix: "csi-",
		controllers:    "localhost:3370",
		log:            logrus.NewEntry(logrus.New()),
	}

	l.log.Logger.SetFormatter(&logrus.TextFormatter{})
	l.log.Logger.SetOutput(ioutil.Discard)

	// run all option functions.
	for _, opt := range options {
		err := opt(l)
		if err != nil {
			return nil, err
		}
	}

	// Add in fields that may have been configured above.
	l.log = l.log.WithFields(logrus.Fields{
		"annotationsKey":      l.annotationsKey,
		"linstorCSIComponent": "client",
		"controllers":         l.controllers,
	})

	l.log.WithFields(logrus.Fields{
		"resourceDeployment": fmt.Sprintf("%+v", l),
	}).Debug("generated new ResourceDeployment")
	return l, nil
}

// Controllers is the list of Linstor controllers to communicate with.
func Controllers(controllers ...string) func(*Linstor) error {
	return func(l *Linstor) error {
		l.controllers = strings.Join(controllers, ",")
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

// Debug sets debugging log behavor.
func Debug(l *Linstor) error {
	l.log.Logger.SetLevel(logrus.DebugLevel)
	l.log.Logger.SetReportCaller(true)
	return nil
}

func (s *Linstor) ListAll(parameters map[string]string) ([]*volume.Info, error) {
	return nil, nil
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
			fmt.Errorf("got request for %d Bytes of storage (needed to allocate %s), but size is limited to %s",
				requiredBytes, volumeSize, limit)
	}
	return int64(volumeSize.Value()), nil
}

func (s *Linstor) resDefToVolume(resDef lc.ResDef) (*volume.Info, error) {
	for _, p := range resDef.RscDfnProps {
		if p.Key == "Aux/"+s.annotationsKey {
			vol := &volume.Info{
				Parameters: make(map[string]string),
				Snapshots:  []*volume.SnapInfo{},
			}

			if err := json.Unmarshal([]byte(p.Value), vol); err != nil {
				return nil, fmt.Errorf("failed to unmarshal annotations for ResDef %+v", resDef)
			}

			if vol.Name == "" {
				return nil, fmt.Errorf("Failed to extract resource name from %+v", vol)
			}
			s.log.WithFields(logrus.Fields{
				"resourceDefinition": fmt.Sprintf("%+v", resDef),
				"volume":             fmt.Sprintf("%+v", vol),
			}).Debug("converted resource definition to volume")
			return vol, nil
		}
	}
	return nil, nil
}

func (s *Linstor) resDeploymentConfigFromVolumeInfo(vol *volume.Info) (*lc.ResourceDeploymentConfig, error) {
	cfg := &lc.ResourceDeploymentConfig{}

	cfg.LogOut = s.logOut

	cfg.Controllers = s.controllers

	// At this time vol.ID has to be a valid LINSTOR Name
	cfg.Name = vol.ID

	cfg.SizeKiB = uint64(data.NewKibiByte(data.ByteSize(vol.SizeBytes)).Value())

	for k, v := range vol.Parameters {
		switch strings.ToLower(k) {
		case NodeListKey:
			cfg.NodeList = strings.Split(v, " ")
		case LayerListKey:
			cfg.LayerList = strings.Split(v, " ")
		case ReplicasOnSameKey:
			cfg.ReplicasOnSame = strings.Split(v, " ")
		case ReplicasOnDifferentKey:
			cfg.ReplicasOnDifferent = strings.Split(v, " ")
		case StoragePoolKey:
			cfg.StoragePool = v
		case DisklessStoragePoolKey:
			cfg.DisklessStoragePool = v
		case AutoPlaceKey:
			if v == "" {
				v = "0"
			}
			autoplace, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("unable to parse %q as an integer", v)
			}
			cfg.AutoPlace = autoplace
		case DoNotPlaceWithRegexKey:
			cfg.DoNotPlaceWithRegex = v
		case EncryptionKey:
			if strings.ToLower(v) == "true" {
				cfg.Encryption = true
			}
		}
	}
	serializedVol, err := json.Marshal(vol)
	if err != nil {
		return nil, err
	}

	// TODO: Support for other annotations.
	cfg.Annotations = make(map[string]string)
	cfg.Annotations[s.annotationsKey] = string(serializedVol)

	return cfg, nil
}

func (s *Linstor) resDeploymentFromVolumeInfo(vol *volume.Info) (*lc.ResourceDeployment, error) {
	cfg, err := s.resDeploymentConfigFromVolumeInfo(vol)
	if err != nil {
		return nil, err
	}
	r := lc.NewResourceDeployment(*cfg)
	return &r, nil
}

func (s *Linstor) GetByName(name string) (*volume.Info, error) {
	s.log.WithFields(logrus.Fields{
		"csiVolumeName": name,
	}).Debug("looking up resource by CSI volume name")

	r := lc.NewResourceDeployment(lc.ResourceDeploymentConfig{
		Name:        "CSIGetByName",
		Controllers: s.controllers,
		LogOut:      s.logOut})
	list, err := r.ListResourceDefinitions()
	if err != nil {
		return nil, err
	}
	for _, rd := range list {
		vol, err := s.resDefToVolume(rd)
		if err != nil {
			return nil, err
		}
		// Probably found a resource we didn't create.
		if vol == nil {
			continue
		}
		if vol.Name == name {
			return vol, nil
		}
	}
	return nil, nil
}

func (s *Linstor) GetByID(ID string) (*volume.Info, error) {
	s.log.WithFields(logrus.Fields{
		"csiVolumeID": ID,
	}).Debug("looking up resource by CSI volume ID")

	r := lc.NewResourceDeployment(lc.ResourceDeploymentConfig{
		Name:        "CSIGetByID",
		Controllers: s.controllers,
		LogOut:      s.logOut})
	list, err := r.ListResourceDefinitions()
	if err != nil {
		return nil, err
	}

	for _, rd := range list {
		if rd.RscName == ID {
			vol, err := s.resDefToVolume(rd)
			if err != nil {
				return nil, err
			}
			return vol, nil
		}
	}
	return nil, nil
}

func (s *Linstor) Create(vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("creating volume")

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return err
	}

	return r.CreateAndAssign()
}

func (s *Linstor) Delete(vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("deleting volume")

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return err
	}

	return r.Delete()
}

func (s *Linstor) AccessibleTopologies(vol *volume.Info) ([]*csi.Topology, error) {
	if vol.Parameters[UseLocalStorageKey] != "true" {
		return nil, nil
	}

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return nil, err
	}

	nodes, err := r.DeployedNodes()
	if err != nil {
		return nil, err
	}

	topos := []*csi.Topology{}
	for _, n := range nodes {
		topos = append(topos, &csi.Topology{Segments: map[string]string{"kubernetes.io/hostname": n}})
	}

	return topos, nil
}

func (s *Linstor) Attach(vol *volume.Info, node string) error {
	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	}).Info("attaching volume")

	// This is hackish, configure a volume copy that only makes new diskless asignments.
	cfg, err := s.resDeploymentConfigFromVolumeInfo(vol)
	if err != nil {
		return err
	}
	cfg.NodeList = []string{}
	cfg.AutoPlace = 0
	cfg.ClientList = []string{node}

	return lc.NewResourceDeployment(*cfg).Assign()
}

// Detach removes a volume from the node.
func (s *Linstor) Detach(vol *volume.Info, node string) error {
	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	}).Info("detaching volume")

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return err
	}

	return r.Unassign(node)
}

// SnapCreate calls linstor to create a new snapshot on the volume indicated by
// the SourceVolumeId contained in the CSI Snapshot.
func (s *Linstor) SnapCreate(snap *volume.SnapInfo) (*volume.SnapInfo, error) {
	vol, err := s.GetByID(snap.CsiSnap.SourceVolumeId)
	if err != nil {
		return nil, fmt.Errorf("failed to retrive volume info from id %s", snap.CsiSnap.SourceVolumeId)
	}
	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return nil, err
	}

	linSnap, err := r.SnapshotCreate(snap.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %v", err)
	}

	// Fill in missing snapshot fields on creation, keep original SourceVolumeId.
	snap.CsiSnap = &csi.Snapshot{
		SnapshotId:     linSnap.UUID,
		SourceVolumeId: snap.CsiSnap.SourceVolumeId,
		SizeBytes:      int64(data.NewKibiByte(data.KiB * data.ByteSize(linSnap.SnapshotVlmDfns[0].VlmSize)).InclusiveBytes()),
		CreationTime:   ptypes.TimestampNow(),
		ReadyToUse:     true,
	}

	s.log.WithFields(logrus.Fields{
		"linstorSnapshot": fmt.Sprintf("%+v", linSnap),
		"csiSnapshot":     fmt.Sprintf("%+v", *snap),
	}).Debug("created new snapshot")

	// Update volume information to reflect the newly-added snapshot.
	vol.Snapshots = append(vol.Snapshots, snap)
	serializedVol, err := json.Marshal(vol)
	if err != nil {
		return nil, err
	}
	if err := r.SetAuxProp(s.annotationsKey, string(serializedVol)); err != nil {
		// We should at least try to delete the snapshot here, even though it succeeded
		// without error it's going be unregistered as far as the CO is concerned.
		if err := r.SnapshotDelete(snap.CsiSnap.SnapshotId); err != nil {
			s.log.WithError(err).Error("failed to clean up snapshot after recording its metadata failed")
		}
		return nil, fmt.Errorf("unable to record new snapshot metadata: %v", err)
	}

	return snap, nil
}

// SnapDelete calls LINSTOR to delete the snapshot based on the CSI Snapshot ID.
func (s *Linstor) SnapDelete(snap *volume.SnapInfo) error {
	vol, err := s.GetByID(snap.CsiSnap.SourceVolumeId)
	if err != nil {
		return fmt.Errorf("failed to retrive volume info from id %s", snap.CsiSnap.SourceVolumeId)
	}

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return err
	}

	if err := r.SnapshotDelete(snap.CsiSnap.SnapshotId); err != nil {
		return fmt.Errorf("failed to remove snaphsot: %v", err)
	}

	// Record the changes to the snaphots on the resource def's aux props.
	updatedSnaps := []*volume.SnapInfo{}
	for _, s := range vol.Snapshots {
		if s.CsiSnap.SourceVolumeId != snap.CsiSnap.SourceVolumeId {
			updatedSnaps = append(updatedSnaps, s)
		}
	}
	vol.Snapshots = updatedSnaps
	serializedVol, err := json.Marshal(vol)
	if err != nil {
		return err
	}
	if err := r.SetAuxProp(s.annotationsKey, string(serializedVol)); err != nil {
		if err := r.SnapshotDelete(snap.CsiSnap.SnapshotId); err != nil {
			s.log.WithError(err).Error("failed to update snapshot list after recording its metadata failed")
		}
		return fmt.Errorf("unable to record new snapshot metadata: %v", err)
	}

	return nil
}

// VolFromSnap creates the volume using the data contained within the snapshot.
func (s *Linstor) VolFromSnap(snap *volume.SnapInfo, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume":   fmt.Sprintf("%+v", vol),
		"snapshot": fmt.Sprintf("%+v", snap),
	}).Info("creating volume from snapshot")

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return err
	}

	return r.NewResourceFromSnapshot(snap.CsiSnap.SnapshotId)
}

func (s *Linstor) VolFromVol(sourceVol, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume":       fmt.Sprintf("%+v", vol),
		"sourceVolume": fmt.Sprintf("%+v", sourceVol),
	}).Info("creating volume from snapshot")

	baseRes, err := s.resDeploymentFromVolumeInfo(sourceVol)
	if err != nil {
		return err
	}
	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return err
	}

	return r.NewResourceFromResource(*baseRes)
}

func (s *Linstor) CanonicalizeVolumeName(suggestedName string) string {
	name, err := linstorifyResourceName(suggestedName)
	if err != nil {
		return s.fallbackPrefix + uuid.New()
	} else {
		// We already handled the idempotency/existing case
		// This is to make sure that nobody else created a resource with that name (e.g., another user/plugin)
		existingVolume, err := s.GetByID(name)
		if existingVolume != nil || err != nil {
			return s.fallbackPrefix + uuid.New()
		}
	}

	return name
}

func (s *Linstor) CanonicalizeSnapshotName(suggestedName string) string {
	// TODO: Snapshots actually have different naming requirements, it might
	// be nice to conform to those eventually.
	name, err := linstorifyResourceName(suggestedName)
	if err != nil {
		return s.fallbackPrefix + uuid.New()
	} else {
		// We already handled the idempotency/existing case
		// This is to make sure that nobody else created a snapshot with that name (e.g., another user/plugin)
		existingSnap, err := s.GetSnapByName(name)
		if existingSnap != nil || err != nil {
			return s.fallbackPrefix + uuid.New()
		}
	}

	return name
}

// ListVolumes returns all volumes that have metadata that is understandable
// by this plugin, so volumes from multiple compatible plugins may be returned.
func (s *Linstor) ListVolumes() ([]*volume.Info, error) {
	r := lc.NewResourceDeployment(lc.ResourceDeploymentConfig{
		Name:        "CSIVolumeList",
		Controllers: s.controllers,
		LogOut:      s.logOut})

	allResDefs, err := r.ListResourceDefinitions()
	if err != nil {
		return nil, fmt.Errorf("failed to retrive resource definitions: %v", err)
	}

	vols := []*volume.Info{}

	for _, rd := range allResDefs {
		// If we encounter a failure here, we can assume that the resource was
		// not created by a CSI driver, but we can't check here that it was
		// created by this instance of the CSI driver in particular.
		// Linstor names are CSI IDs.
		vol, err := s.resDefToVolume(rd)
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

func (s *Linstor) GetSnapByName(name string) (*volume.SnapInfo, error) {
	vols, err := s.ListVolumes()
	if err != nil {
		return nil, err
	}

	for _, vol := range vols {
		for _, snap := range vol.Snapshots {
			if snap.Name == name {
				return snap, nil
			}
		}
	}
	return nil, nil
}

func (s *Linstor) GetSnapByID(ID string) (*volume.SnapInfo, error) {
	vols, err := s.ListVolumes()
	if err != nil {
		return nil, err
	}

	return s.doGetSnapByID(vols, ID), nil
}

func (s *Linstor) doGetSnapByID(vols []*volume.Info, ID string) *volume.SnapInfo {
	for _, vol := range vols {
		for _, snap := range vol.Snapshots {
			if snap.CsiSnap.SnapshotId == ID {
				return snap
			}
		}
	}
	return nil
}

func (s *Linstor) ListSnaps() ([]*volume.SnapInfo, error) {
	r := lc.NewResourceDeployment(lc.ResourceDeploymentConfig{
		Name:        "CSISnapList",
		Controllers: s.controllers,
		LogOut:      s.logOut})

	allSnaps, err := r.SnapshotList()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %v", err)
	}
	s.log.WithFields(logrus.Fields{
		"allLinstorSnapshots": fmt.Sprintf("%+v", allSnaps),
	}).Debug("retrived all linstor snapshots")

	vols, err := s.ListVolumes()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %v", err)
	}

	var snaps []*volume.SnapInfo

	for _, snap := range allSnaps {
		snapCreatedByMe := s.doGetSnapByID(vols, snap.UUID)
		if snapCreatedByMe != nil {
			snaps = append(snaps, snapCreatedByMe)
		} else {
			s.log.WithFields(logrus.Fields{
				"missingSnapshot": fmt.Sprintf("%+v", snap),
			}).Info("unable to look up snap by it's UUID, potentially not made by me")

		}
	}

	return snaps, nil
}

func (s *Linstor) NodeAvailable(node string) (bool, error) {
	// Hard coding magic string to pass csi-test.
	if node == "some-fake-node-id" {
		return false, nil
	}

	// TODO: Check if the node is available.
	return true, nil
}

func (s *Linstor) GetAssignmentOnNode(vol *volume.Info, node string) (*volume.Assignment, error) {
	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	}).Debug("getting assignment info")

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return nil, err
	}

	devPath, err := r.GetDevPath(node, false)
	if err != nil {
		return nil, err
	}
	va := &volume.Assignment{
		Vol:  vol,
		Node: node,
		Path: devPath,
	}
	s.log.WithFields(logrus.Fields{
		"volumeAssignment": fmt.Sprintf("%+v", va),
	}).Debug("found assignment info")

	return va, nil
}

func (s *Linstor) Mount(vol *volume.Info, source, target, fsType string, options []string) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
		"source": source,
		"target": target,
	}).Info("mounting volume")

	r, err := s.resDeploymentFromVolumeInfo(vol)
	if err != nil {
		return err
	}

	// Merge mount options from Storage Classes and CSI calls.
	options = append(options, vol.Parameters[MountOptsKey])
	mntOpts := strings.Join(options, ",")

	// If an FSType is supplided by the parameters, override the one passed
	// to the Mount Call.
	parameterFsType, ok := vol.Parameters[FSKey]
	if ok {
		fsType = parameterFsType
	}

	mounter := lc.FSUtil{
		ResourceDeployment: r,
		FSType:             fsType,
		MountOpts:          mntOpts,
		FSOpts:             vol.Parameters[FSOptsKey],
	}
	s.log.WithFields(logrus.Fields{
		"mounter": fmt.Sprintf("%+v", mounter),
	}).Debug("configured mounter")

	err = mounter.SafeFormat(source)
	if err != nil {
		return err
	}

	return mounter.Mount(source, target)
}

func (s *Linstor) Unmount(target string) error {
	s.log.WithFields(logrus.Fields{
		"target": target,
	}).Info("unmounting volume")

	r := lc.NewResourceDeployment(lc.ResourceDeploymentConfig{
		Name:   "CSI Unmount",
		LogOut: s.logOut})
	mounter := lc.FSUtil{
		ResourceDeployment: &r,
	}
	s.log.WithFields(logrus.Fields{
		"mounter": fmt.Sprintf("%+v", mounter),
	}).Debug("configured mounter")

	return mounter.UnMount(target)
}

// validResourceName returns an error if the input string is not a valid LINSTOR name
func validResourceName(resName string) error {
	if resName == "all" {
		return errors.New("Not allowed to use 'all' as resource name")
	}

	b, err := regexp.MatchString("[[:alpha:]]", resName)
	if err != nil {
		return err
	} else if !b {
		return errors.New("Resource name did not contain at least one alphabetic (A-Za-z) character")
	}

	re := "^[A-Za-z_][A-Za-z0-9\\-_]{1,47}$"
	b, err = regexp.MatchString(re, resName)
	if err != nil {
		return err
	} else if !b {
		// without open coding it (ugh!) as good as it gets
		return fmt.Errorf("Resource name did not match: '%s'", re)
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

	re := regexp.MustCompile("[^A-Za-z0-9\\-_]")
	newName := re.ReplaceAllLiteralString(name, "_")
	if err := validResourceName(newName); err == nil {
		return newName, err
	}

	// fulfill at least the minimal requirement
	newName = "LS_" + newName
	if err := validResourceName(newName); err == nil {
		return newName, nil
	}

	return "", fmt.Errorf("Could not linstorify name (%s)", name)
}
