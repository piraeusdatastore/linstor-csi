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

package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pborman/uuid"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
)

// Info provides the everything need to manipulate volumes.
type Info struct {
	Name         string            `json:"name"`
	ID           string            `json:"id"`
	CreatedBy    string            `json:"createdBy"`
	CreationTime time.Time         `json:"creationTime"`
	SizeBytes    int64             `json:"sizeBytes"`
	Readonly     bool              `json:"readonly"`
	Parameters   map[string]string `json:"parameters"`
}

//go:generate enumer -type=paramKey
type paramKey int

const (
	unknown paramKey = iota
	allowremotevolumeaccess
	autoplace
	clientlist
	disklessonremaining
	disklessstoragepool
	donotplacewithregex
	encryption
	fsopts
	layerlist
	mountopts
	nodelist
	placementcount
	placementpolicy
	replicasondifferent
	replicasonsame
	sizekib
	storagepool
	postmountxfsopts
	resourcegroup
)

// Parameters configuration for linstor volumes.
type Parameters struct {
	// ClientList is a list of nodes where the volume should be assigned to disklessly
	// at the time that the volume is first created.
	ClientList []string
	// NodeList is a list of nodes where the volume should be assigned to diskfully
	// at the time that the volume is first created. Specifying this overrides any
	// other automatic placement rules.
	NodeList []string
	// ReplicasOnDifferent is a list that corresponds to the `linstor resource create`
	// option of the same name.
	ReplicasOnDifferent []string
	// ReplicasOnSame is a list that corresponds to the `linstor resource create`
	// option of the same name.
	ReplicasOnSame []string
	// DisklessStoragePool is the diskless storage pool to use for diskless assignments.
	DisklessStoragePool string
	// DoNotPlaceWithRegex corresponds to the `linstor resource create`
	// option of the same name.
	DoNotPlaceWithRegex string
	// FSOpts is a string of filesystem options passed at mount time.
	FSOpts string
	// MountOpts is a string of mount options passed at mount time. Comma
	// separated like in /etc/fstab.
	MountOpts string
	// StoragePool is the storage pool to use for diskful assignments.
	StoragePool string
	SizeKiB     uint64
	// PlacementCount is the number of replicas of the volume in total.
	PlacementCount int32
	// Disklessonremaining corresponds to the `linstor resource create`
	// option of the same name.
	Disklessonremaining bool
	// Encrypt volumes if true.
	Encryption bool
	// AllowRemoteVolumeAccess if true, volumes may be accessed over the network.
	AllowRemoteVolumeAccess bool
	// LayerList is a list that corresponds to the `linstor resource create`
	// option of the same name.
	LayerList []lapi.LayerType
	// PlacementPolicy determines where volumes are created.
	PlacementPolicy topology.PlacementPolicy
	// PostMountXfsOpts is an optional string of post-mount call
	PostMountXfsOpts string
	// ResourceGroup is the resource-group name in LINSTOR.
	ResourceGroup string
	// Properties are the properties to be set on the resource group.
	Properties map[string]string
}

// DefaultDisklessStoragePoolName is the hidden diskless storage pool that linstor
// assigned diskless volumes to if they're not given a user created DisklessStoragePool.
const DefaultDisklessStoragePoolName = "DfltDisklessStorPool"

// NewParameters parses out the raw parameters we get and sets appropriate
// zero values
func NewParameters(params map[string]string) (Parameters, error) {
	// set zero values
	var p = Parameters{
		LayerList:               []lapi.LayerType{lapi.DRBD, lapi.STORAGE},
		PlacementCount:          1,
		DisklessStoragePool:     DefaultDisklessStoragePoolName,
		Encryption:              false,
		PlacementPolicy:         topology.AutoPlace,
		AllowRemoteVolumeAccess: true,
		Properties:              make(map[string]string),
	}

	for k, v := range params {
		if strings.HasPrefix(k, linstor.PropertyNamespace+"/") {
			k = k[len(linstor.PropertyNamespace)+1:]
			p.Properties[k] = v
			continue
		}

		if strings.HasPrefix(k, lc.NamespcDrbdOptions+"/") {
			p.Properties[k] = v
			continue
		}

		key, err := paramKeyString(strings.ToLower(k))
		if err != nil {
			return p, fmt.Errorf("invalid parameter: %v", err)
		}

		switch key {
		case nodelist:
			p.NodeList = strings.Split(v, " ")
		case layerlist:
			l, err := ParseLayerList(v)
			if err != nil {
				return p, err
			}
			p.LayerList = l
		case replicasonsame:
			p.ReplicasOnSame = maybeAddAux(strings.Split(v, " ")...)
		case replicasondifferent:
			p.ReplicasOnDifferent = maybeAddAux(strings.Split(v, " ")...)
		case storagepool:
			p.StoragePool = v
		case disklessstoragepool:
			p.DisklessStoragePool = v
		case autoplace, placementcount:
			if v == "" {
				v = "1"
			}
			count, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return p, fmt.Errorf("bad parameters: unable to parse %q as a 32 bit integer", v)
			}
			p.PlacementCount = int32(count)
		case donotplacewithregex:
			p.DoNotPlaceWithRegex = v
		case encryption:
			e, err := strconv.ParseBool(v)
			if err != nil {
				return p, err
			}
			p.Encryption = e
		case disklessonremaining:
			d, err := strconv.ParseBool(v)
			if err != nil {
				return p, err
			}
			p.Disklessonremaining = d
		case allowremotevolumeaccess:
			a, err := strconv.ParseBool(v)
			if err != nil {
				return p, err
			}
			p.AllowRemoteVolumeAccess = a
		case clientlist:
			p.ClientList = strings.Split(v, " ")
		case sizekib:
			if v == "" {
				v = "4"
			}
			size, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return p, fmt.Errorf("bad parameters: unable to parse %q as an unsigned 64 bit integer", v)
			}
			p.SizeKiB = size
		case placementpolicy:
			policy, err := topology.PlacementPolicyString(v)
			if err != nil {
				return p, fmt.Errorf("invalid placement policy: %v", err)
			}
			p.PlacementPolicy = policy
		case mountopts:
			p.MountOpts = v
		case fsopts:
			p.FSOpts = v
		case postmountxfsopts:
			p.PostMountXfsOpts = v
		case resourcegroup:
			p.ResourceGroup = v
		}
	}

	if p.ResourceGroup == "" {
		rg, _, err := p.ToResourceGroupModify(&lapi.ResourceGroup{})
		if err != nil {
			return p, fmt.Errorf("failed to generate resource group properties from storage class: %w", err)
		}

		encoded, err := json.Marshal(rg)
		if err != nil {
			return p, fmt.Errorf("failed to encode parameters to generate resource group name: %w", err)
		}

		namespace := uuid.UUID(linstor.ResourceGroupNamespace)
		p.ResourceGroup = "sc-" + uuid.NewSHA1(namespace, encoded).String()
	}

	// User has manually configured deployments, ignore autoplacing options.
	if len(p.NodeList)+len(p.ClientList) != 0 {
		p.PlacementCount = 0
		p.ReplicasOnSame = make([]string, 0)
		p.ReplicasOnDifferent = make([]string, 0)
		p.DoNotPlaceWithRegex = ""
		p.PlacementPolicy = topology.Manual
	}

	return p, nil
}

// Convert parameters into a modify-object that reconciles any differences between parameters and resource group
func (params *Parameters) ToResourceGroupModify(rg *lapi.ResourceGroup) (lapi.ResourceGroupModify, bool, error) {
	changed := false
	rgModify := lapi.ResourceGroupModify{
		OverrideProps: make(map[string]string),
	}

	if rg.SelectFilter.PlaceCount != params.PlacementCount {
		changed = true
		rgModify.SelectFilter.PlaceCount = params.PlacementCount
	}

	// LINSTOR does not accept "" as a placeholder for an absent storage pool, so we must take care to never try to
	// set it in such cases.
	if params.StoragePool != "" {
		if rg.SelectFilter.StoragePool != params.StoragePool {
			changed = true
			rgModify.SelectFilter.StoragePool = params.StoragePool
		}

		existingPool, ok := rg.Props[lc.KeyStorPoolName]
		if !ok || existingPool != params.StoragePool {
			changed = true
			rgModify.OverrideProps[lc.KeyStorPoolName] = params.StoragePool
		}
	}

	if !stringSlicesEqual(rg.SelectFilter.ReplicasOnSame, params.ReplicasOnSame) {
		changed = true
		rgModify.SelectFilter.ReplicasOnSame = params.ReplicasOnSame
	}

	if !stringSlicesEqual(rg.SelectFilter.ReplicasOnDifferent, params.ReplicasOnDifferent) {
		changed = true
		rgModify.SelectFilter.ReplicasOnDifferent = params.ReplicasOnDifferent
	}

	stringLayers := make([]string, len(params.LayerList))
	for i, layer := range params.LayerList {
		stringLayers[i] = string(layer)
	}
	if !stringSlicesEqual(rg.SelectFilter.LayerStack, stringLayers) {
		changed = true
		rgModify.SelectFilter.LayerStack = stringLayers
	}

	if rg.SelectFilter.NotPlaceWithRscRegex != params.DoNotPlaceWithRegex {
		changed = true
		rgModify.SelectFilter.NotPlaceWithRscRegex = params.DoNotPlaceWithRegex
	}

	if rg.SelectFilter.DisklessOnRemaining != params.Disklessonremaining {
		changed = true
		rgModify.SelectFilter.DisklessOnRemaining = params.Disklessonremaining
	}

	for k, v := range params.Properties {
		existingV, ok := rg.Props[k]
		if !ok {
			changed = true
			rgModify.OverrideProps[k] = v
			continue
		}

		if existingV != v {
			return lapi.ResourceGroupModify{}, false, fmt.Errorf("cannot change existing property value: existing = %s, expected = %s", existingV, v)
		}
	}

	return rgModify, changed, nil
}

// DisklessFlag returns the diskless flag passed to resource create calls.
// It will select the flag matching the top-most layer that supports disk-less resources.
func (params *Parameters) DisklessFlag() (string, error) {
	for _, l := range params.LayerList {
		if l == lapi.DRBD {
			return lc.FlagDrbdDiskless, nil
		}
		if l == lapi.NVME {
			return lc.FlagNvmeInitiator, nil
		}
		if l == lapi.OPENFLEX {
			// Openflex volumes are connected via NVMe
			return lc.FlagNvmeInitiator, nil
		}
	}

	return "", fmt.Errorf("could not determine diskless flag for layers: %v", params.LayerList)
}

//ParseLayerList returns a slice of LayerType from a string of space-separated layers.
func ParseLayerList(s string) ([]lapi.LayerType, error) {
	list := strings.Split(s, " ")
	var layers = make([]lapi.LayerType, 0)
	knownLayers := []lapi.LayerType{
		lapi.DRBD,
		lapi.STORAGE,
		lapi.LUKS,
		lapi.NVME,
		lapi.CACHE,
		lapi.OPENFLEX,
		lapi.WRITECACHE,
	}

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

// Sort sorts a list of snaphosts.
func Sort(vols []*Info) {
	sort.Slice(vols, func(j, k int) bool {
		return vols[j].CreationTime.Before(vols[k].CreationTime)
	})
}

// ToResourceCreateList prepares a list of lapi.ResourceCreate to be used to
// manually assign resources based on the node and client lists of the volume.
func (i *Info) ToResourceCreateList() ([]lapi.ResourceCreate, error) {
	params, err := NewParameters(i.Parameters)
	if err != nil {
		return nil, err
	}

	var resCreates = make([]lapi.ResourceCreate, len(params.NodeList)+len(params.ClientList))

	// The parameter parsed fine above, so they can't fail in these method calls.
	var k int
	for _, node := range params.NodeList {
		resCreates[k], _ = i.ToDiskfullResourceCreate(node)
		k++
	}

	for _, node := range params.ClientList {
		resCreates[k], _ = i.ToDisklessResourceCreate(node)
		k++
	}

	return resCreates, nil
}

// ToDiskfullResourceCreate prepares a Info to be deployed by linstor on a node
// with local storage.
func (i *Info) ToDiskfullResourceCreate(node string) (lapi.ResourceCreate, error) {
	params, err := NewParameters(i.Parameters)
	if err != nil {
		return lapi.ResourceCreate{}, err
	}

	res := i.toGenericResourceCreate(params, node)
	return res, nil
}

// ToDisklessResourceCreate prepares a Info to be deployed by linstor on a node
// without local storage.
func (i *Info) ToDisklessResourceCreate(node string) (lapi.ResourceCreate, error) {
	params, err := NewParameters(i.Parameters)
	if err != nil {
		return lapi.ResourceCreate{}, err
	}

	diskless, err := params.DisklessFlag()
	if err != nil {
		return lapi.ResourceCreate{}, err
	}

	res := i.toGenericResourceCreate(params, node)
	res.Resource.Props[lc.KeyStorPoolName] = params.DisklessStoragePool
	res.Resource.Flags = append(res.Resource.Flags, diskless)

	return res, nil
}

func (i *Info) toGenericResourceCreate(params Parameters, node string) lapi.ResourceCreate {
	return lapi.ResourceCreate{
		Resource: lapi.Resource{
			Name:     i.ID,
			NodeName: node,
			Props:    make(map[string]string, 1),
			Flags:    make([]string, 0),
		},
		LayerList: params.LayerList,
	}
}

// ToAutoPlace prepares a Info to be deployed by linstor via autoplace.
func (i *Info) ToAutoPlace() (lapi.AutoPlaceRequest, error) {
	// Workaround for LINSTOR bug prior to v1.8.0. The default layer list for auto-place requests was '[]',
	// which is then defaulted to drbd,storage. This would override any layer list specified in the resource group.
	// This workaround alleviates the issue by setting the layer list on the auto-place request explicitly.
	params, err := NewParameters(i.Parameters)
	if err != nil {
		return lapi.AutoPlaceRequest{}, err
	}

	return lapi.AutoPlaceRequest{LayerList: params.LayerList}, nil
}

// Assignment represents a volume situated on a particular node.
type Assignment struct {
	Vol *Info
	// Node is the node that the assignment is valid for.
	Node string
	// Path is a location on the Node's filesystem where the volume may be accessed.
	Path string
}

// CreateDeleter handles the creation and deletion of volumes.
type CreateDeleter interface {
	Querier
	Create(ctx context.Context, vol *Info, req *csi.CreateVolumeRequest) error
	Delete(ctx context.Context, vol *Info) error

	// AccessibleTopologies returns the list of key value pairs volume topologies
	// for the volume or nil if not applicable.
	AccessibleTopologies(ctx context.Context, vol *Info) ([]*csi.Topology, error)
}

// SnapshotCreateDeleter handles the creation and deletion of snapshots.
type SnapshotCreateDeleter interface {
	// CompatibleSnapshotId returns an ID unique to the suggested name
	CompatibleSnapshotId(name string) string
	SnapCreate(ctx context.Context, id string, sourceVol *Info) (*csi.Snapshot, error)
	SnapDelete(ctx context.Context, snap *csi.Snapshot) error
	// FindSnapByID searches the snapshot in the backend
	// It returns:
	// * the snapshot, nil if not found
	// * true, if the snapshot is either in progress or successful
	// * any error encountered
	FindSnapByID(ctx context.Context, id string) (*csi.Snapshot, bool, error)
	FindSnapsBySource(ctx context.Context, sourceVol *Info, start, limit int) ([]*csi.Snapshot, error)
	// List Snapshots should return a sorted list of snapshots.
	ListSnaps(ctx context.Context, start, limit int) ([]*csi.Snapshot, error)
	// VolFromSnap creates a new volume based on the provided snapshot.
	VolFromSnap(ctx context.Context, snap *csi.Snapshot, vol *Info) error
}

// AttacherDettacher handles operations relating to volume accessiblity on nodes.
type AttacherDettacher interface {
	Querier
	Attach(ctx context.Context, vol *Info, node string) error
	Detach(ctx context.Context, vol *Info, node string) error
	NodeAvailable(ctx context.Context, node string) error
	GetAssignmentOnNode(ctx context.Context, vol *Info, node string) (*Assignment, error)
}

// Querier retrives various states of volumes.
type Querier interface {
	// ListAll should return a sorted list of pointers to Info.
	ListAll(ctx context.Context) ([]*Info, error)
	// FindByName returns nil when volume is not found.
	FindByName(ctx context.Context, name string) (*Info, error)
	// FindByID returns nil when volume is not found.
	FindByID(ctx context.Context, ID string) (*Info, error)
	// AllocationSizeKiB returns the number of KiB required to provision required bytes.
	AllocationSizeKiB(requiredBytes, limitBytes int64) (int64, error)
	// CapacityBytes determines the capacity of the underlying storage in Bytes.
	CapacityBytes(ctx context.Context, params map[string]string) (int64, error)
}

// Mounter handles the filesystems located on volumes.
type Mounter interface {
	Mount(vol *Info, source, target, fsType string, readonly bool, options []string) error
	Unmount(target string) error
	IsNotMountPoint(target string) (bool, error)
}

// VolumeStats provides details about filesystem usage.
type VolumeStats struct {
	AvailableBytes  int64
	TotalBytes      int64
	UsedBytes       int64
	AvailableInodes int64
	TotalInodes     int64
	UsedInodes      int64
}

// VolumeStatter provides info about volume/filesystem usage.
type VolumeStatter interface {
	// GetVolumeStats determines filesystem usage.
	GetVolumeStats(path string) (VolumeStats, error)
}

type NodeInformer interface {
	GetNodeTopologies(ctx context.Context, nodename string) (*csi.Topology, error)
}

// Expander handles the resizing operations for volumes.
type Expander interface {
	NodeExpand(source, target string) error
	ControllerExpand(ctx context.Context, vol *Info) error
}

func maybeAddAux(props ...string) []string {
	const auxPrefix = lc.NamespcAuxiliary + "/"

	result := make([]string, len(props))
	for i, prop := range props {
		if strings.HasPrefix(prop, auxPrefix) {
			result[i] = prop
		} else {
			result[i] = auxPrefix + prop
		}
	}

	return result
}

func stringSlicesEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
