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
	"io/ioutil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	ptypes "github.com/golang/protobuf/ptypes"
	"github.com/haySwim/data"
	"github.com/pborman/uuid"
	logrus "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"k8s.io/kubernetes/pkg/util/mount"
)

// Parameter key names.
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
	DisklessOnRemainingKey = "disklessonremaining"
	EncryptionKey          = "encryption"
	FSKey                  = "filesystem"
	UseLocalStorageKey     = "localstoragepolicy"
	MountOptsKey           = "mountopts"
	FSOptsKey              = "fsopts"
)

type parameters struct {
	nodeList            []string
	layerList           []lapi.LayerType
	clientList          []string
	replicasOnSame      []string
	replicasOnDifferent []string
	placementCount      int32
	doNotPlaceWithRegex string
	sizeKiB             uint64
	storagePool         string
	disklessStoragePool string
	encryption          bool
	disklessonremaining bool
	fs                  string
	lsp                 localStoragePolicy
	mountOpts           string
	fsOpts              string
}

const defaultDisklessStoragePoolName = "DfltDisklessStorPool"

func newParameters(params map[string]string) (parameters, error) {
	var p = parameters{
		layerList:           []lapi.LayerType{lapi.DRBD, lapi.STORAGE},
		placementCount:      1,
		disklessStoragePool: defaultDisklessStoragePoolName,
		encryption:          false,
		lsp:                 localStoragePolicyIgnore,
	}
	for k, v := range params {
		switch strings.ToLower(k) {
		case NodeListKey:
			p.nodeList = strings.Split(v, " ")
		case LayerListKey:
			l, err := parseLayerList(v)
			if err != nil {
				return p, err
			}
			p.layerList = l
		case ReplicasOnSameKey:
			p.replicasOnSame = strings.Split(v, " ")
		case ReplicasOnDifferentKey:
			p.replicasOnDifferent = strings.Split(v, " ")
		case StoragePoolKey:
			p.storagePool = v
		case DisklessStoragePoolKey:
			p.disklessStoragePool = v
		case AutoPlaceKey:
			if v == "" {
				v = "1"
			}
			autoplace, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return p, fmt.Errorf("unable to parse %q as a 32 bit integer", v)
			}
			p.placementCount = int32(autoplace)
		case DoNotPlaceWithRegexKey:
			p.doNotPlaceWithRegex = v
		case EncryptionKey:
			e, err := strconv.ParseBool(v)
			if err != nil {
				return p, err
			}
			p.encryption = e
		case DisklessOnRemainingKey:
			d, err := strconv.ParseBool(v)
			if err != nil {
				return p, err
			}
			p.disklessonremaining = d
		case ClientListKey:
			p.clientList = strings.Split(v, " ")
		case SizeKiBKey:
			if v == "" {
				v = "4"
			}
			size, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return p, fmt.Errorf("unable to parse %q as an unsigned 64 bit integer", v)
			}
			p.sizeKiB = uint64(size)
		case FSKey:
			p.fs = v
		case UseLocalStorageKey:
			lsp, err := parseLocalStoragePolicy(v)
			if err != nil {
				return p, err
			}
			p.lsp = lsp
		case MountOptsKey:
			p.mountOpts = v
		case FSOptsKey:
			p.fsOpts = v
		}
	}

	// User has manually configured deployments, ignore autoplacing options.
	if len(p.nodeList)+len(p.clientList) != 0 {
		p.placementCount = 0
		p.replicasOnSame = make([]string, 0)
		p.replicasOnDifferent = make([]string, 0)
		p.doNotPlaceWithRegex = ""
	}

	return p, nil
}

func parseLayerList(s string) ([]lapi.LayerType, error) {
	list := strings.Split(s, " ")
	var layers = make([]lapi.LayerType, 0)
	knownLayers := []lapi.LayerType{lapi.DRBD, lapi.STORAGE, lapi.LUKS, lapi.NVME}

userLayers:
	for _, l := range list {
		for _, k := range knownLayers {
			if strings.EqualFold(l, string(k)) {
				layers = append(layers, k)
				continue userLayers
			}
		}
		// Reached the bottom without finding a match.
		return layers, fmt.Errorf("unknown layer type %s, known layer types %v", l, knownLayers)
	}
	return layers, nil
}

type localStoragePolicy int

const (
	localStoragePolicyPreferred localStoragePolicy = iota
	localStoragePolicyRequired
	localStoragePolicyIgnore
)

const LinstorNodeTopologyKey = "linbit.com/hostname"

type Linstor struct {
	log            *logrus.Entry
	annotationsKey string
	fallbackPrefix string
	endpoint       *url.URL
	client         *lapi.Client
	logCfg         *lapi.LogCfg
	mounter        *mount.SafeFormatAndMount
}

// NewLinstor returns a high-level linstor client for CSI applications to interact with
// By default, it will try to connect with localhost:3370.
func NewLinstor(options ...func(*Linstor) error) (*Linstor, error) {
	l := &Linstor{
		annotationsKey: "Aux/csi-volume-annotations",
		fallbackPrefix: "csi-",
		log:            logrus.NewEntry(logrus.New()),
		client:         &lapi.Client{},
		logCfg: &lapi.LogCfg{
			Out:       ioutil.Discard,
			Formatter: &logrus.TextFormatter{},
			Level:     "info",
		}}

	var err error

	l.endpoint, err = url.Parse("http://localhost:3370")
	if err != nil {
		return nil, err
	}

	l.log.Logger.SetFormatter(l.logCfg.Formatter)
	l.log.Logger.SetOutput(l.logCfg.Out)

	// run all option functions.
	for _, opt := range options {
		err := opt(l)
		if err != nil {
			return nil, err
		}
	}

	c, err := lapi.NewClient(lapi.Log(l.logCfg), lapi.BaseURL(l.endpoint))
	if err != nil {
		return nil, err
	}

	cp := l.client

	*cp = *c

	// Add in fields that may have been configured above.
	l.log = l.log.WithFields(logrus.Fields{
		"annotationsKey":      l.annotationsKey,
		"linstorCSIComponent": "client",
	})

	l.mounter = &mount.SafeFormatAndMount{
		Interface: mount.New("/bin/mount"),
		Exec:      mount.NewOsExec(),
	}

	l.log.WithFields(logrus.Fields{
		"client": fmt.Sprintf("%+v", l.client),
	}).Debug("generated new linstor client")
	return l, nil
}

// Endpoint is the url where the LINSTOR controller api can be accessed.
func Endpoint(s string) func(*Linstor) error {
	return func(l *Linstor) error {
		u, err := url.Parse(s)
		if err != nil {
			return fmt.Errorf("unable to use %s as LINSTOR API endpoint: %v", s, err)
		}
		l.endpoint = u
		return nil
	}
}

// LogOut sets the Linstor client to write logs to the provided io.Writer
// instead of discarding logs.
func LogOut(out io.Writer) func(*Linstor) error {
	return func(l *Linstor) error {
		l.log.Logger.SetOutput(out)

		// For the client's logs.
		l.logCfg.Out = out
		return nil
	}
}

// LogFmt sets the format of the log outpout via the provided logrus.Formatter.
func LogFmt(fmt logrus.Formatter) func(*Linstor) error {
	return func(l *Linstor) error {
		l.log.Logger.SetFormatter(fmt)

		// For the client's logs.
		l.logCfg.Formatter = fmt
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

		// For the client's logs.
		l.logCfg.Level = s

		// Report function names on debug
		if level == logrus.DebugLevel {
			l.log.Logger.SetReportCaller(true)
		}
		return nil
	}
}

func (s *Linstor) ListAll(ctx context.Context, page, perPage int) ([]*volume.Info, error) {
	var vols = make([]*volume.Info, 0)

	resDefs, err := s.client.ResourceDefinitions.GetAll(ctx, &lapi.ListOpts{Page: page, PerPage: perPage})
	if err != nil {
		return vols, nil
	}

	for _, rd := range resDefs {
		vol, err := s.resourceDefinitionToVolume(rd)
		if err != nil {
			return vols, nil
		}

		vols = append(vols, vol)
	}

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

func (s *Linstor) resourceDefinitionToVolume(resDef lapi.ResourceDefinition) (*volume.Info, error) {
	csiVolumeAnnotation, ok := resDef.Props[s.annotationsKey]
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

func (s *Linstor) volToResourceDefinitionCreate(vol *volume.Info, params parameters) (lapi.ResourceDefinitionCreate, error) {
	resDef, err := s.volToResourceDefinition(vol, params)
	if err != nil {
		return lapi.ResourceDefinitionCreate{}, err
	}
	return lapi.ResourceDefinitionCreate{ResourceDefinition: resDef}, nil
}

func (s *Linstor) volToResourceDefinition(vol *volume.Info, params parameters) (lapi.ResourceDefinition, error) {
	resDef := lapi.ResourceDefinition{
		ExternalName: vol.Name,
		Props:        make(map[string]string),
		LayerData:    make([]lapi.ResourceDefinitionLayer, len(params.layerList)),
	}

	for i := range resDef.LayerData {
		resDef.LayerData[i].Type = params.layerList[i]
	}

	serializedVol, err := json.Marshal(vol)
	if err != nil {
		return resDef, err
	}

	// TODO: Support for other annotations.
	resDef.Props[s.annotationsKey] = string(serializedVol)

	return resDef, nil
}

func (s *Linstor) volToResourceCreateList(vol *volume.Info, params parameters) []lapi.ResourceCreate {
	var resCreates = make([]lapi.ResourceCreate, len(params.nodeList)+len(params.clientList))

	for _, node := range params.nodeList {
		resCreates = append(resCreates, s.volToResourceCreate(vol, params, node, false))
	}

	for _, node := range params.clientList {
		resCreates = append(resCreates, s.volToResourceCreate(vol, params, node, true))
	}

	return resCreates
}

func (s *Linstor) volToResourceCreate(vol *volume.Info, params parameters, node string, diskless bool) lapi.ResourceCreate {
	res := lapi.ResourceCreate{
		LayerList: params.layerList,
		Resource: lapi.Resource{
			Name:     vol.ID,
			NodeName: node,
			Props:    make(map[string]string, 1),
			Flags:    make([]string, 0),
		}}
	if diskless {
		res.Resource.Props[lc.KeyStorPoolName] = params.disklessStoragePool
		res.Resource.Flags = append(res.Resource.Flags, lc.FlagDiskless)
	} else {
		res.Resource.Props[lc.KeyStorPoolName] = params.storagePool
	}
	return res
}

func (s *Linstor) paramsToAutoPlace(params parameters) lapi.AutoPlaceRequest {
	return lapi.AutoPlaceRequest{
		DisklessOnRemaining: params.disklessonremaining,
		LayerList:           params.layerList,
		SelectFilter: lapi.AutoSelectFilter{
			PlaceCount:           params.placementCount,
			StoragePool:          params.storagePool,
			NotPlaceWithRscRegex: params.doNotPlaceWithRegex,
			ReplicasOnSame:       params.replicasOnSame,
			ReplicasOnDifferent:  params.replicasOnDifferent,
		}}
}

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

func (s *Linstor) GetByID(ctx context.Context, ID string) (*volume.Info, error) {
	s.log.WithFields(logrus.Fields{
		"csiVolumeID": ID,
	}).Debug("looking up resource by CSI volume ID")

	res, err := s.client.ResourceDefinitions.Get(ctx, ID)
	if err != nil {
		return nil, nil404(err)
	}

	return s.resourceDefinitionToVolume(res)
}

func (s *Linstor) Create(ctx context.Context, vol *volume.Info, req *csi.CreateVolumeRequest) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("creating volume")

	params, err := newParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("unable to create volume due to bad parameters %+v: %v", vol.Parameters, err)
	}

	if err := s.createResourceDefinition(ctx, vol, params); err != nil {
		return err
	}

	// Create the volume definition, now that vol has been updated with its ID.
	if err := s.client.ResourceDefinitions.CreateVolumeDefinition(ctx, vol.ID,
		lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{SizeKib: uint64(data.NewKibiByte(data.ByteSize(vol.SizeBytes)).Value())}}); err != nil {
		return err
	}

	// If we don't care about local storage or nodes are being manually assigned
	// or there are no topology preferences skip the topology based assignment logic.
	topos := req.GetAccessibilityRequirements()
	if params.lsp == localStoragePolicyIgnore ||
		params.placementCount == 0 ||
		topos == nil {
		return s.deploy(ctx, vol, params)
	}

	remainingAssignments := params.placementCount

	for i, pref := range topos.GetPreferred() {
		// While there are still preferred nodes and remainingAssignments
		// attach resources diskfully to those nodes in order of most to least preferred.
		if p, ok := pref.GetSegments()[LinstorNodeTopologyKey]; ok && remainingAssignments > 0 {
			// If attachment fails move onto next most preferred volume.
			if err := s.client.Resources.Create(ctx, s.volToResourceCreate(vol, params, p, false)); err != nil {
				s.log.WithFields(logrus.Fields{
					"volumeID":                   vol.ID,
					"topologyPreference":         i,
					"topologyNode":               p,
					"totalVolumeCount":           params.placementCount,
					"remainingVolumeAssignments": remainingAssignments,
					"reason":                     err,
				}).Info("unable to satisfy topology preference")
				continue
			}
			// If attachment succeeds, decrement the number of remainingAssignments.
			remainingAssignments--
			// If we're out of remaining attachments, we're done.
			if remainingAssignments == 0 {
				return nil
			}
		}
	}

	// We weren't able to assign any volume according to topology preferences
	// and local storage is required.
	if params.placementCount == remainingAssignments && params.lsp == localStoragePolicyRequired {
		return fmt.Errorf("unable to satisfy volume topology requirements for volume %s", vol.ID)
	}

	// If params.placementCount is higher than the number of assigned nodes s.deploy should
	// automatically provision the rest.
	return s.deploy(ctx, vol, params)
}

func (s *Linstor) deploy(ctx context.Context, vol *volume.Info, params parameters) error {
	if params.placementCount == 0 {
		manualPlacements := s.volToResourceCreateList(vol, params)
		for _, placement := range manualPlacements {
			err := s.client.Resources.Create(ctx, placement)
			if err != nil {
				return err
			}
		}
	}

	// We're autoplacing resources, this should be the usual case.
	return s.client.Resources.Autoplace(ctx, vol.ID, s.paramsToAutoPlace(params))
}

func (s *Linstor) Delete(ctx context.Context, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("deleting volume")

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

	return nil404(s.client.ResourceDefinitions.Delete(ctx, vol.ID))
}

func (s *Linstor) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	p, err := newParameters(vol.Parameters)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	if p.lsp != localStoragePolicyRequired {
		return s.lspIgnoreAccessibleTopologies(ctx, vol)
	}

	r, err := s.client.Resources.GetAll(ctx, vol.ID)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	var topos = make([]*csi.Topology, 0)
	for _, n := range deployedNodes(r) {
		topos = append(topos, &csi.Topology{Segments: map[string]string{LinstorNodeTopologyKey: n}})
	}

	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"parameters": fmt.Sprintf("%+v", p),
		"topologies": fmt.Sprintf("%+v", topos),
	}).Debug("determined volume topologies")

	return topos, nil
}

func (s *Linstor) lspIgnoreAccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	p, err := newParameters(vol.Parameters)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	pools, err := s.getAllStoragePools(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	var nodes = make([]string, 0)
	for _, sp := range pools {
		// The default diskless storage pool doesn't show up in the list and by
		// default all network attachable resources can use it.
		if p.disklessStoragePool == defaultDisklessStoragePoolName ||
			// Otherwise, the user is using a particular diskless storage pool
			(sp.StoragePoolName == p.disklessStoragePool && sp.ProviderKind == lapi.DISKLESS) {
			nodes = append(nodes, sp.NodeName)
		}
	}

	var topos = make([]*csi.Topology, 0)
	for _, n := range uniq(nodes) {
		topos = append(topos, &csi.Topology{Segments: map[string]string{LinstorNodeTopologyKey: n}})
	}

	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"parameters": fmt.Sprintf("%+v", p),
		"topologies": fmt.Sprintf("%+v", topos),
	}).Debug("determined volume topologies")

	return topos, nil
}

func uniq(strs []string) []string {
	var seen = make(map[string]bool, len(strs))
	var j int

	for _, s := range strs {
		if seen[s] {
			continue
		}
		seen[s] = true
		strs[j] = s
		j++
	}

	return strs[:j]
}

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

	params, err := newParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("unable to attach volume due to bad parameters %+v: %v", vol.Parameters, err)
	}

	return s.client.Resources.Create(ctx, s.volToResourceCreate(vol, params, node, true))
}

// Detach removes a volume from the node.
func (s *Linstor) Detach(ctx context.Context, vol *volume.Info, node string) error {
	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	}).Info("detaching volume")

	res, err := s.client.Resources.Get(ctx, vol.ID, node)
	if err != nil {
		return err
	}

	if deployed(res) {
		s.log.WithFields(logrus.Fields{
			"volume":     fmt.Sprintf("%+v", vol),
			"targetNode": node,
		}).Info("volume is diskfull on node, refusing to detach")
		return nil
	}

	return s.client.Resources.Delete(ctx, vol.ID, node)
}

// CapacityBytes returns the amount of free space in the storage pool specified
// the the params.
func (s *Linstor) CapacityBytes(ctx context.Context, params map[string]string) (int64, error) {
	p, err := newParameters(params)
	if err != nil {
		return 0, fmt.Errorf("unable to get capacity: %v", err)
	}
	pools, err := s.getAllStoragePools(ctx)
	if err != nil {
		return 0, fmt.Errorf("unable to get capacity for storage pool %s: %v", p.storagePool, err)
	}

	var total int64
	for _, sp := range pools {
		if p.storagePool == sp.StoragePoolName || p.storagePool == "" {
			total += sp.FreeCapacity
		}
	}

	return int64(data.NewKibiByte(data.KiB * data.ByteSize(total)).To(data.B)), nil
}

func (s *Linstor) getAllStoragePools(ctx context.Context) ([]lapi.StoragePool, error) {
	var allPools = make([]lapi.StoragePool, 0)

	nodes, err := s.client.Nodes.GetAll(ctx)
	if err != nil {
		return allPools, fmt.Errorf("unable to get all storage pools: %v", err)
	}

	poolChan := make(chan lapi.StoragePool, 1)

	g, egctx := errgroup.WithContext(ctx)
	for _, n := range nodes {
		node := n.Name
		g.Go(func() error {
			pools, err := s.client.Nodes.GetStoragePools(egctx, node)
			if err == nil {
				for _, sp := range pools {
					poolChan <- sp
				}
			}
			return err
		})
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for sp := range poolChan {
			allPools = append(allPools, sp)
		}
	}()

	if err := g.Wait(); err != nil {
		return allPools, err
	}
	close(poolChan)
	wg.Wait()

	return allPools, nil
}

// SnapCreate calls linstor to create a new snapshot on the volume indicated by
// the SourceVolumeId contained in the CSI Snapshot.
func (s *Linstor) SnapCreate(ctx context.Context, snap *volume.SnapInfo) (*volume.SnapInfo, error) {
	vol, err := s.GetByID(ctx, snap.CsiSnap.SourceVolumeId)
	if err != nil {
		return nil, fmt.Errorf("failed to retrive volume info from id %s", snap.CsiSnap.SourceVolumeId)
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
		return fmt.Errorf("failed to retrive volume info from id %s", snap.CsiSnap.SourceVolumeId)
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

	return s.createResourceFromSnaphot(ctx, vol, snap)
}

func (s *Linstor) VolFromVol(ctx context.Context, sourceVol, vol *volume.Info) error {
	s.log.WithFields(logrus.Fields{
		"volume":       fmt.Sprintf("%+v", vol),
		"sourceVolume": fmt.Sprintf("%+v", sourceVol),
	}).Info("creating volume from snapshot")

	return s.createResourceFromResource(ctx, sourceVol, vol)
}

// Creates a resourceDefinition, updating the vol.ID if successful.
func (s *Linstor) createResourceDefinition(ctx context.Context, vol *volume.Info, params parameters) error {
	resDefCreate, err := s.volToResourceDefinitionCreate(vol, params)
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

func (s *Linstor) createResourceFromSnaphot(ctx context.Context, vol *volume.Info, snap *volume.SnapInfo) error {
	params, err := newParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("unable to create volume due to bad parameters %+v: %v", vol.Parameters, err)
	}

	if err := s.createResourceDefinition(ctx, vol, params); err != nil {
		return err
	}

	r, err := s.client.Resources.GetAll(ctx, vol.ID)
	if err != nil {
		return err
	}

	snapRestore := lapi.SnapshotRestore{
		ToResource: vol.ID,
		Nodes:      deployedNodes(r),
	}

	if err := s.client.Resources.RestoreVolumeDefinitionSnapshot(ctx, snap.CsiSnap.SourceVolumeId, snap.Name, snapRestore); err != nil {
		return err
	}

	if err := s.client.Resources.RestoreSnapshot(ctx, snap.CsiSnap.SourceVolumeId, snap.Name, snapRestore); err != nil {
		return err
	}

	return nil
}

func (s *Linstor) createResourceFromResource(ctx context.Context, from, to *volume.Info) error {

	tmpName := s.fallbackPrefix + uuid.New()
	if err := s.client.Resources.CreateSnapshot(ctx,
		lapi.Snapshot{
			Name:         tmpName,
			ResourceName: from.ID,
		}); err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	return s.createResourceFromSnaphot(ctx, to,
		&volume.SnapInfo{
			Name: tmpName,
			CsiSnap: &csi.Snapshot{
				SourceVolumeId: from.ID,
			}})
}

func (s *Linstor) saveVolume(ctx context.Context, vol *volume.Info) error {
	serializedVol, err := json.Marshal(vol)
	if err != nil {
		return err
	}
	return s.setProps(ctx, vol, map[string]string{s.annotationsKey: string(serializedVol)})
}

func (s *Linstor) setProps(ctx context.Context, vol *volume.Info, props map[string]string) error {
	return s.client.ResourceDefinitions.Modify(ctx, vol.ID,
		lapi.PropsModify{
			OverrideProps: props,
		})
}

func (s *Linstor) CanonicalizeSnapshotName(ctx context.Context, suggestedName string) string {
	// TODO: Snapshots actually have different naming requirements, it might
	// be nice to conform to those eventually.
	name, err := linstorifyResourceName(suggestedName)
	if err != nil {
		return s.fallbackPrefix + uuid.New()
	} else {
		// We already handled the idempotency/existing case
		// This is to make sure that nobody else created a snapshot with that name (e.g., another user/plugin)
		existingSnap, err := s.GetSnapByName(ctx, name)
		if existingSnap != nil || err != nil {
			return s.fallbackPrefix + uuid.New()
		}
	}

	return name
}

// ListVolumes returns all volumes that have metadata that is understandable
// by this plugin, so volumes from multiple compatible plugins may be returned.
func (s *Linstor) ListVolumes(ctx context.Context) ([]*volume.Info, error) {
	allResDefs, err := s.client.ResourceDefinitions.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrive resource definitions: %v", err)
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

func (s *Linstor) GetSnapByID(ctx context.Context, ID string) (*volume.SnapInfo, error) {
	vols, err := s.ListVolumes(ctx)
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

func (s *Linstor) GetAssignmentOnNode(ctx context.Context, vol *volume.Info, node string) (*volume.Assignment, error) {
	s.log.WithFields(logrus.Fields{
		"volume":     fmt.Sprintf("%+v", vol),
		"targetNode": node,
	}).Debug("getting assignment info")

	vols, err := s.client.Resources.GetVolumes(ctx, vol.ID, node)
	if err != nil {
		return nil, err
	}

	var devPath string

	// We only work with volume 0.
	for _, v := range vols {
		if v.VolumeNumber == 0 {
			devPath = v.DevicePath
			break
		}

		return nil, fmt.Errorf("resource %s has no volume 0 on node %s", vol.ID, node)
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

	inUse, err := s.mounter.DeviceOpened(source)
	if err != nil {
		return fmt.Errorf("checking for exclusive open failed: %v", err)
	}
	if inUse {
		return fmt.Errorf("unable to get an exclusive open on %s, check device health", source)
	}

	var block bool
	if fsType == "" {
		block = true
	}

	if !block {
		// Merge mount options from Storage Classes and CSI calls.
		options = append(options, vol.Parameters[MountOptsKey])
		// and if an fsType is supplied by the parameters, override the one passed
		// to the Mount Call.
		p, err := newParameters(vol.Parameters)
		if err != nil {
			return fmt.Errorf("mounting volume failed: %v", err)
		}
		if p.fs != "" {
			fsType = p.fs
		}

		if err := s.mounter.MakeDir(target); err != nil {
			return fmt.Errorf("could not create target directory %s, %v", target, err)
		}

	} else {
		if err := s.mounter.MakeFile(target); err != nil {
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

func parseLocalStoragePolicy(s string) (localStoragePolicy, error) {
	switch strings.ToLower(s) {
	case "required":
		return localStoragePolicyRequired, nil
	case "preferred":
		return localStoragePolicyPreferred, nil
	case "ignore", "":
		return localStoragePolicyIgnore, nil
	default:
		return localStoragePolicyIgnore, fmt.Errorf("%s is not a valid localStoragePolicy", s)
	}
}

func nil404(e error) error {
	if e == lapi.NotFoundError {
		return nil
	}
	return e
}

func deployedNodes(res []lapi.Resource) []string {
	var nodes = make([]string, 0)
	for _, r := range res {
		if deployed(r) {
			nodes = append(nodes, r.NodeName)
		}
	}
	return nodes
}

func deployed(res lapi.Resource) bool {
	return doesNotcontainAll(res.Flags, lc.FlagDiskless)
}

func containsAll(list []string, candidates ...string) bool {
	if len(candidates) == 0 {
		return false
	}

nextCandidate:
	for _, c := range candidates {
		for _, e := range list {
			if e == c {
				continue nextCandidate
			}
		}
		// Made it through all data with no match.
		return false
	}
	return true
}

func doesNotcontainAll(list []string, candidates ...string) bool {
	return !containsAll(list, candidates...)
}
