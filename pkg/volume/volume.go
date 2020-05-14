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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/haySwim/data"
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
	Snapshots    []*SnapInfo       `json:"snapshots"`
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
	// DRBDOpts are the options than can be set on DRBD resources
	DRBDOpts map[string]string
}

// DefaultDisklessStoragePoolName is the hidden diskless storage pool that linstor
// assigned diskless volumes to if they're not given a user created DisklessStoragePool.
const DefaultDisklessStoragePoolName = "DfltDisklessStorPool"

const drbdOptsPrefix = "DrbdOptions/"

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
		DRBDOpts:                make(map[string]string),
	}

	for k, v := range params {
		if strings.HasPrefix(k, drbdOptsPrefix) {
			p.DRBDOpts[k] = v
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
			p.ReplicasOnSame = strings.Split(v, " ")
		case replicasondifferent:
			p.ReplicasOnDifferent = strings.Split(v, " ")
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

//ParseLayerList returns a slice of LayerType from a string of space-separated layers.
func ParseLayerList(s string) ([]lapi.LayerType, error) {
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

// Sort sorts a list of snaphosts.
func Sort(vols []*Info) {
	sort.Slice(vols, func(j, k int) bool {
		return vols[j].CreationTime.Before(vols[k].CreationTime)
	})
}

// ToResourceGroupSpawn creates a lapi.ResourceDefinitionSpawn from
// a volume.Info.
func (i *Info) ToResourceGroupSpawn() (lapi.ResourceGroupSpawn, error) {
	return lapi.ResourceGroupSpawn{
		ResourceDefinitionName:         "",
		ResourceDefinitionExternalName: i.Name,
		VolumeSizes:                    []int64{int64(data.NewKibiByte(data.ByteSize(i.SizeBytes)).Value())},
		DefinitionsOnly:                true,
	}, nil
}

func (i *Info) ToResourceGroupModify(rg lapi.ResourceGroup) (lapi.ResourceGroupModify, error) {
	rgModify := lapi.ResourceGroupModify{
		OverrideProps: make(map[string]string),
	}

	if rg.Props == nil {
		rg.Props = make(map[string]string)
	}

	params, err := NewParameters(i.Parameters)
	if err != nil {
		return rgModify, err
	}

	rgModify.SelectFilter.PlaceCount = params.PlacementCount

	if params.StoragePool != "" { // otherwise we set the storagepool to "", which does not make LINSTOR happy
		rgModify.SelectFilter.StoragePool = params.StoragePool
		rgModify.OverrideProps[lc.KeyStorPoolName] = params.StoragePool
	}

	for _, p := range params.ReplicasOnDifferent {
		rgModify.SelectFilter.ReplicasOnDifferent = append(rgModify.SelectFilter.ReplicasOnDifferent, p)
	}

	for _, p := range params.ReplicasOnSame {
		rgModify.SelectFilter.ReplicasOnSame = append(rgModify.SelectFilter.ReplicasOnSame, p)
	}

	for _, p := range params.LayerList {
		rgModify.SelectFilter.LayerStack = append(rgModify.SelectFilter.LayerStack, string(p))
	}

	rgModify.SelectFilter.NotPlaceWithRscRegex = params.DoNotPlaceWithRegex
	rgModify.SelectFilter.DisklessOnRemaining = params.Disklessonremaining

	for k, v := range params.DRBDOpts {
		rgModify.OverrideProps[k] = v
	}
	// delete the ones that we had, but do no longer exist
	// this is mainly for the sake of completeness, one can not edit a SC in k8s
	for k := range rg.Props {
		if _, ok := rgModify.OverrideProps[k]; !ok {
			rgModify.DeleteProps = append(rgModify.DeleteProps, k)
		}
	}

	return rgModify, nil
}

// ToResourceGroup creates a LINSTOR ResourceGroup struct that can be used to create a RG
func (i *Info) ToResourceGroup() (lapi.ResourceGroup, error) {
	resourceGroup := lapi.ResourceGroup{
		Props:        make(map[string]string),
		SelectFilter: lapi.AutoSelectFilter{},
	}
	params, err := NewParameters(i.Parameters)
	if err != nil {
		return resourceGroup, err
	}
	resourceGroup.Name = params.ResourceGroup

	return resourceGroup, nil
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

	res := i.toGenericResourceCreate(params, node)
	res.Resource.Props[lc.KeyStorPoolName] = params.DisklessStoragePool
	res.Resource.Flags = append(res.Resource.Flags, lc.FlagDiskless)
	return res, nil
}

func (i *Info) toGenericResourceCreate(params Parameters, node string) lapi.ResourceCreate {
	return lapi.ResourceCreate{
		Resource: lapi.Resource{
			Name:     i.ID,
			NodeName: node,
			Props:    make(map[string]string, 1),
			Flags:    make([]string, 0),
		}}
}

// ToAutoPlace prepares a Info to be deployed by linstor via autoplace.
func (i *Info) ToAutoPlace() (lapi.AutoPlaceRequest, error) {
	// kept for abstraction
	return lapi.AutoPlaceRequest{}, nil
}

// SnapInfo provides everything needed to manipulate snapshots.
type SnapInfo struct {
	Name    string        `json:"name"`
	CsiSnap *csi.Snapshot `json:"csiSnapshot"`
}

// SnapSort sorts a list of snaphosts.
func SnapSort(snaps []*SnapInfo) {
	sort.Slice(snaps, func(j, k int) bool {
		if snaps[j].CsiSnap.CreationTime.Seconds == snaps[k].CsiSnap.CreationTime.Seconds {
			return snaps[j].CsiSnap.CreationTime.Nanos < snaps[k].CsiSnap.CreationTime.Nanos
		}
		return snaps[j].CsiSnap.CreationTime.Seconds < snaps[k].CsiSnap.CreationTime.Seconds
	})
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
	SnapCreate(ctx context.Context, snap *SnapInfo) (*SnapInfo, error)
	SnapDelete(ctx context.Context, snap *SnapInfo) error
	GetSnapByName(ctx context.Context, name string) (*SnapInfo, error)
	GetSnapByID(ctx context.Context, ID string) (*SnapInfo, error)
	// List Snapshots should return a sorted list of snapshots.
	ListSnaps(ctx context.Context) ([]*SnapInfo, error)
	// CanonicalizeSnapshotName tries to return a relatively similar version
	// of the suggestedName if the storage backend cannot use the suggestedName
	// in its original form.
	CanonicalizeSnapshotName(ctx context.Context, suggestedName string) string
	// VolFromSnap creats a new volume based on the provided snapshot.
	VolFromSnap(ctx context.Context, snap *SnapInfo, vol *Info) error
	// VolFromVol creats a new volume based on the provided volume.
	VolFromVol(ctx context.Context, sourceVol, vol *Info) error
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
	GetByName(ctx context.Context, name string) (*Info, error)
	//GetByID should return nil when volume is not found.
	GetByID(ctx context.Context, ID string) (*Info, error)
	// AllocationSizeKiB returns the number of KiB required to provision required bytes.
	AllocationSizeKiB(requiredBytes, limitBytes int64) (int64, error)
	// CapacityBytes determines the capacity of the underlying storage in Bytes.
	CapacityBytes(ctx context.Context, params map[string]string) (int64, error)
}

// Mounter handles the filesystems located on volumes.
type Mounter interface {
	Mount(vol *Info, source, target, fsType string, options []string) error
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
