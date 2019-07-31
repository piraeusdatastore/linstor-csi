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
	"github.com/LINBIT/linstor-csi/pkg/linstor"
	"github.com/LINBIT/linstor-csi/pkg/topology"
	"github.com/container-storage-interface/spec/lib/go/csi"
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
	fs
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
	// ReplicasOnDifferent is a list that corresonds to the `linstor resource create`
	// option of the same name.
	ReplicasOnDifferent []string
	// ReplicasOnSame is a list that corresonds to the `linstor resource create`
	// option of the same name.
	ReplicasOnSame []string
	// DisklessStoragePool is the diskless storage pool to use for diskless assignments.
	DisklessStoragePool string
	// DoNotPlaceWithRegex corresonds to the `linstor resource create`
	// option of the same name.
	DoNotPlaceWithRegex string
	// FS is the filesystem type: ext4, xfs, and so on.
	FS string
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
	// Disklessonremaining corresonds to the `linstor resource create`
	// option of the same name.
	Disklessonremaining bool
	// Encrypt volumes if true.
	Encryption bool
	// AllowRemoteVolumeAccess if true, volumes may be accessed over the network.
	AllowRemoteVolumeAccess bool
	// LayerList is a list that corresonds to the `linstor resource create`
	// option of the same name.
	LayerList []lapi.LayerType
	// PlacementPolicy determines where volumes are created.
	PlacementPolicy topology.PlacementPolicy
}

// DefaultDisklessStoragePoolName is the hidden diskless storage pool that linstor
// assigned diskless volumes to if they're not given a user created DisklessStoragePool.
const DefaultDisklessStoragePoolName = "DfltDisklessStorPool"

// NewParameters parses out the raw parameters we get and sets appropreate
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
	}

	for k, v := range params {
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
		case fs:
			p.FS = v
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

// ToResourceDefinitionCreate prepares a lapi.ResourceDefinitionCreate from
// a volume.Info.
func (i *Info) ToResourceDefinitionCreate() (lapi.ResourceDefinitionCreate, error) {
	resDef, err := i.ToResourceDefinition()
	if err != nil {
		return lapi.ResourceDefinitionCreate{}, err
	}
	return lapi.ResourceDefinitionCreate{ResourceDefinition: resDef}, nil
}

// ToResourceDefinition prepares a lapi.ResourceDefinition from a volume.Info.
func (i *Info) ToResourceDefinition() (lapi.ResourceDefinition, error) {
	params, err := NewParameters(i.Parameters)
	if err != nil {
		return lapi.ResourceDefinition{}, err
	}

	resDef := lapi.ResourceDefinition{
		ExternalName: i.Name,
		Props:        make(map[string]string),
		LayerData:    make([]lapi.ResourceDefinitionLayer, len(params.LayerList)),
	}

	for k := range resDef.LayerData {
		resDef.LayerData[k].Type = params.LayerList[k]
	}

	serializedVol, err := json.Marshal(i)
	if err != nil {
		return resDef, err
	}

	// TODO: Support for other annotations.
	resDef.Props[linstor.AnnotationsKey] = string(serializedVol)

	return resDef, nil
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
	res.Resource.Props[lc.KeyStorPoolName] = params.StoragePool
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
		LayerList: params.LayerList,
		Resource: lapi.Resource{
			Name:     i.ID,
			NodeName: node,
			Props:    make(map[string]string, 1),
			Flags:    make([]string, 0),
		}}
}

// ToAutoPlace prepares a Info to be deployed by linstor via autoplace.
func (i *Info) ToAutoPlace() (lapi.AutoPlaceRequest, error) {
	params, err := NewParameters(i.Parameters)
	if err != nil {
		return lapi.AutoPlaceRequest{}, err
	}

	return lapi.AutoPlaceRequest{
		DisklessOnRemaining: params.Disklessonremaining,
		LayerList:           params.LayerList,
		SelectFilter: lapi.AutoSelectFilter{
			PlaceCount:           params.PlacementCount,
			StoragePool:          params.StoragePool,
			NotPlaceWithRscRegex: params.DoNotPlaceWithRegex,
			ReplicasOnSame:       params.ReplicasOnSame,
			ReplicasOnDifferent:  params.ReplicasOnDifferent,
		}}, nil
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
}
