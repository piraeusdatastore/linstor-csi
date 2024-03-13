package volume

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
)

//go:generate go run github.com/alvaroloes/enumer@v1.1.2 -type=paramKey
type paramKey int

const (
	allowremotevolumeaccess paramKey = iota
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
	usepvcname
	overprovision
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
	// FSOpts is a string of filesystem options passed at creation time.
	FSOpts string
	// MountOpts is a string of mount options passed at mount time. Comma
	// separated like in /etc/fstab.
	MountOpts string
	// StoragePool is the storage pool to use for diskful assignments.
	StoragePools []string
	// PlacementCount is the number of replicas of the volume in total.
	PlacementCount int32
	// Disklessonremaining corresponds to the `linstor resource create`
	// option of the same name.
	Disklessonremaining bool
	// Encrypt volumes if true.
	Encryption bool
	// AllowRemoteVolumeAccess if true, volumes may be accessed over the network.
	AllowRemoteVolumeAccess RemoteAccessPolicy
	// LayerList is a list that corresponds to the `linstor resource create`
	// option of the same name.
	LayerList []devicelayerkind.DeviceLayerKind
	// PlacementPolicy determines where volumes are created.
	PlacementPolicy topology.PlacementPolicy
	// PostMountXfsOpts is an optional string of post-mount call
	PostMountXfsOpts string
	// ResourceGroup is the resource-group name in LINSTOR.
	ResourceGroup string
	// Properties are the properties to be set on the resource group.
	Properties map[string]string
	// UsePvcName derives the volume name from the PVC name+namespace, if that information is available.
	UsePvcName bool
	// OverProvision determines how much free capacity is reported.
	// If set, free capacity is calculated by (TotalCapacity * OverProvision) - ReservedCapacity.
	// If not set, the free capacity is taken directly from LINSTOR.
	OverProvision *float64
}

const DefaultDisklessStoragePoolName = "DfltDisklessStorPool"

// DefaultRemoteAccessPolicy is the access policy used by default when none is specified.
var DefaultRemoteAccessPolicy = RemoteAccessPolicyAnywhere

// NewParameters parses out the raw parameters we get and sets appropriate
// zero values
func NewParameters(params map[string]string, topologyPrefix string) (Parameters, error) {
	// set zero values
	p := Parameters{
		LayerList:           []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Storage},
		PlacementCount:      1,
		DisklessStoragePool: DefaultDisklessStoragePoolName,
		Encryption:          false,
		PlacementPolicy:     topology.AutoPlaceTopology,
		Properties:          make(map[string]string),
	}

	for k, v := range params {
		parts := strings.SplitN(k, "/", 2)

		var namespace, rawkey string
		if len(parts) < 2 {
			namespace = ""
			rawkey = k
		} else {
			namespace = parts[0]
			rawkey = parts[1]
		}

		var param paramKey

		switch namespace {
		case lc.NamespcDrbdOptions:
			// DrbdOptions is not really a namespace in the kubernetes sense, but it fits in nicely in this logic
			p.Properties[lc.NamespcDrbdOptions+"/"+rawkey] = v
			continue
		case linstor.PropertyNamespace:
			k = k[len(linstor.PropertyNamespace)+1:]
			p.Properties[k] = v

			continue
		case linstor.ParameterNamespace, "":
			parsed, err := paramKeyString(strings.ToLower(rawkey))
			if err != nil {
				return p, fmt.Errorf("invalid parameter: %w", err)
			}

			param = parsed
		default:
			// Probably some external parameter passed in the storage class, ignore
			continue
		}

		switch param {
		case nodelist:
			p.NodeList = strings.Split(v, " ")
		case layerlist:
			l, err := ParseLayerList(v)
			if err != nil {
				return p, err
			}

			p.LayerList = l
		case replicasonsame:
			p.ReplicasOnSame = maybeAddTopologyPrefix(topologyPrefix, strings.Split(v, " ")...)
		case replicasondifferent:
			p.ReplicasOnDifferent = maybeAddTopologyPrefix(topologyPrefix, strings.Split(v, " ")...)
		case storagepool:
			p.StoragePools = strings.Split(v, " ")
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
			var policy RemoteAccessPolicy
			err := policy.UnmarshalText([]byte(v))
			if err != nil {
				return p, err
			}

			p.AllowRemoteVolumeAccess = policy
		case clientlist:
			p.ClientList = strings.Split(v, " ")
		case placementpolicy:
			policy, err := topology.PlacementPolicyString(v)
			if err != nil {
				return p, fmt.Errorf("invalid placement policy: %w", err)
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
		case usepvcname:
			u, err := strconv.ParseBool(v)
			if err != nil {
				return p, err
			}

			p.UsePvcName = u
		case sizekib:
			// This parameter was unused. It is just parsed to not break any old storage classes that might be using
			// it. Storage sizes are handled via CSI requests directly.
			log.Warnf("using useless parameter '%s'", rawkey)

		case overprovision:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return p, err
			}

			p.OverProvision = &f
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

	if p.AllowRemoteVolumeAccess == nil {
		if slices.Contains(p.LayerList, devicelayerkind.Drbd) || slices.Contains(p.LayerList, devicelayerkind.Nvme) {
			p.AllowRemoteVolumeAccess = DefaultRemoteAccessPolicy
		} else {
			p.AllowRemoteVolumeAccess = RemoteAccessPolicyLocalOnly
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

	if !slices.Equal(rg.SelectFilter.StoragePoolList, params.StoragePools) {
		changed = true
		rgModify.SelectFilter.StoragePoolList = params.StoragePools
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
		switch l {
		case devicelayerkind.Drbd:
			return lc.FlagDrbdDiskless, nil
		case devicelayerkind.Nvme:
			return lc.FlagNvmeInitiator, nil
		default:
			continue
		}
	}

	return "", fmt.Errorf("could not determine diskless flag for layers: %v", params.LayerList)
}

// ParseLayerList returns a slice of LayerType from a string of space-separated layers.
func ParseLayerList(s string) ([]devicelayerkind.DeviceLayerKind, error) {
	list := strings.Split(s, " ")
	layers := make([]devicelayerkind.DeviceLayerKind, 0)
	knownLayers := []devicelayerkind.DeviceLayerKind{
		devicelayerkind.Drbd,
		devicelayerkind.Storage,
		devicelayerkind.Luks,
		devicelayerkind.Nvme,
		devicelayerkind.Cache,
		devicelayerkind.Writecache,
		devicelayerkind.Exos,
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

// ToResourceCreateList prepares a list of lapi.ResourceCreate to be used to
// manually assign resources based on the node and client lists of the volume.
func (params *Parameters) ToResourceCreateList(id string) ([]lapi.ResourceCreate, error) {
	resCreates := make([]lapi.ResourceCreate, len(params.NodeList)+len(params.ClientList))

	// The parameter parsed fine above, so they can't fail in these method calls.
	var k int

	for _, node := range params.NodeList {
		resCreates[k], _ = params.ToDiskfullResourceCreate(id, node)
		k++
	}

	for _, node := range params.ClientList {
		resCreates[k], _ = params.ToDisklessResourceCreate(id, node)
		k++
	}

	return resCreates, nil
}

// ToDiskfullResourceCreate prepares a Info to be deployed by linstor on a node
// with local storage.
func (params *Parameters) ToDiskfullResourceCreate(id, node string) (lapi.ResourceCreate, error) {
	res := params.toGenericResourceCreate(id, node)
	return res, nil
}

// ToDisklessResourceCreate prepares a Info to be deployed by linstor on a node
// without local storage.
func (params *Parameters) ToDisklessResourceCreate(id, node string) (lapi.ResourceCreate, error) {
	diskless, err := params.DisklessFlag()
	if err != nil {
		return lapi.ResourceCreate{}, err
	}

	res := params.toGenericResourceCreate(id, node)
	res.Resource.Props[lc.KeyStorPoolName] = params.DisklessStoragePool
	res.Resource.Flags = append(res.Resource.Flags, diskless)

	return res, nil
}

func (params *Parameters) toGenericResourceCreate(id, node string) lapi.ResourceCreate {
	return lapi.ResourceCreate{
		Resource: lapi.Resource{
			Name:     id,
			NodeName: node,
			Props:    make(map[string]string, 1),
			Flags:    make([]string, 0),
		},
		LayerList: params.LayerList,
	}
}
