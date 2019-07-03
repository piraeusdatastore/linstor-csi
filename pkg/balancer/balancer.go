package balancer

import (
	"context"
	"fmt"
	"strings"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type GetK8sClient func() (kubernetes.Interface, error)

var k8sClient GetK8sClient = GetInternalk8sClient

const (
	RackLabel      = "failure-domain.beta.kubernetes.io/zone"
	StorageLabel   = "node-role.kubernetes.io/storage"
	PrefNicPropKey = "PrefNic"
	// LinstorNodeTopologyKey refers to a node running the LINSTOR csi node service
	// and the linstor Satellite and is therefore capabile of hosting LINSTOR volumes.
	// TODO: it should be move to utils
	LinstorNodeTopologyKey = "linbit.com/hostname"
	layerListKey           = "layerlist"
)

type StoragePool struct {
	Name          string
	FreeCapacity  int64
	TotalCapacity int64
	PrefNic       string
}

type PrefNic struct {
	Name          string
	FreeCapacity  int64
	TotalCapacity int64
	StoragePools  []*StoragePool
}

type Node struct {
	Name          string
	FreeCapacity  int64
	TotalCapacity int64
	PrefNics      map[string]*PrefNic
}

type BalanceDecision struct {
	StoragePoolName string
	NodeName        string
}

type NodeLinstorClient interface {
	GetStoragePools(ctx context.Context, nodeName string, opts ...*lapi.ListOpts) ([]lapi.StoragePool, error)
}

// TODO: move to utils
type parameters struct {
	layerList               []lapi.LayerType
	allowRemoteVolumeAccess bool
}

// TODO: move to utils
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

// TODO: move to utils
// newParameters parses out the raw parameters we get and sets appropreate
// zero values
func newParameters(params map[string]string) (parameters, error) {
	// set zero values
	var p = parameters{
		layerList: []lapi.LayerType{lapi.DRBD, lapi.STORAGE},
	}

	for k, v := range params {
		switch strings.ToLower(k) {
		case layerListKey:
			l, err := parseLayerList(v)
			if err != nil {
				return p, err
			}
			p.layerList = l
		}
	}

	return p, nil
}

func GetInternalk8sClient() (clientset kubernetes.Interface, err error) {

	// setup k8s client
	config, err := rest.InClusterConfig()

	if err != nil {
		return clientset, err
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		return clientset, err

	}
	return clientset, err
}

// internal function to reuse K7s client
func retrieveRackId(clientset kubernetes.Interface, nodeName string) (rack string, err error) {
	node, err := clientset.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
	if err != nil {
		return "", err

	}
	rack = node.Labels[RackLabel]
	if rack == "" {
		return "", fmt.Errorf("Node %s has no %s label", nodeName, RackLabel)
	}

	return rack, nil
}

func getStorageNodes(clientset kubernetes.Interface) (*v1.NodeList, error) {
	labelSelector := &metav1.LabelSelector{MatchLabels: map[string]string{StorageLabel: "true"}}
	label := metav1.FormatLabelSelector(labelSelector)
	return clientset.CoreV1().Nodes().List(metav1.ListOptions{
		LabelSelector: label,
	})
}

func getNodesInRack(rack string, nodeList *v1.NodeList) (nodes []string, err error) {
	for _, node := range nodeList.Items {
		if node.Labels[RackLabel] == rack {
			nodes = append(nodes, node.Name)
		}
	}
	if len(nodes) == 0 {
		return nodes, fmt.Errorf("No Storage nodes found in Rack %s", rack)
	}
	return nodes, nil
}

func getStorageNodesInRack(rack string, clientset kubernetes.Interface) (nodes []string, err error) {
	storNodes, err := getStorageNodes(clientset)
	if err != nil {
		return nodes, err
	}
	return getNodesInRack(rack, storNodes)
}

func getNodesUtil(ctx context.Context, nClient NodeLinstorClient, selectedNodes []string) (nodes map[string]*Node, err error) {
	nodes = map[string]*Node{}
	for _, node := range selectedNodes {
		spls, err := nClient.GetStoragePools(ctx, node)
		if err != nil {
			return nodes, err
		}

		for _, sp := range spls {
			nicName := sp.Props[PrefNicPropKey]
			free := sp.FreeCapacity
			total := sp.TotalCapacity
			nodeName := node
			spName := sp.StoragePoolName

			if node, ok := nodes[nodeName]; ok {
				node.FreeCapacity += free
				node.TotalCapacity += total
				if prefNic, ok := node.PrefNics[nicName]; ok {
					prefNic.FreeCapacity += free
					prefNic.TotalCapacity += total
					prefNic.StoragePools = append(prefNic.StoragePools, &StoragePool{
						Name:          spName,
						TotalCapacity: total,
						FreeCapacity:  free,
					})
				} else {
					node.PrefNics[nicName] = &PrefNic{
						Name:          nicName,
						TotalCapacity: total,
						FreeCapacity:  free,
						StoragePools: []*StoragePool{
							&StoragePool{
								Name:          spName,
								TotalCapacity: total,
								FreeCapacity:  free,
							},
						},
					}
				}

			} else {
				nodes[nodeName] = &Node{
					Name:          nodeName,
					TotalCapacity: total,
					FreeCapacity:  free,
					PrefNics: map[string]*PrefNic{
						nicName: &PrefNic{
							Name:          nicName,
							TotalCapacity: total,
							FreeCapacity:  free,
							StoragePools: []*StoragePool{
								&StoragePool{
									Name:          spName,
									TotalCapacity: total,
									FreeCapacity:  free,
								},
							},
						},
					},
				}
			}
		}
	}

	return nodes, nil
}

func getLessUsedNode(nUtil map[string]*Node) (leastUsedNode *Node, err error) {
	// maximal value for utilization is 0 so let's make it above it :D
	var minUtilization float64 = 1.1
	for _, node := range nUtil {
		utilization := (float64(node.TotalCapacity) - float64(node.FreeCapacity)) / float64(node.TotalCapacity)
		if minUtilization > utilization {
			minUtilization = utilization
			leastUsedNode = node
		}
	}

	if leastUsedNode == nil {
		return nil, fmt.Errorf("Something went wrong couldn't calculate utilization for nodes")
	}
	return leastUsedNode, nil
}

func getLessUsedNic(node *Node) (nic *PrefNic, err error) {
	// maximal value for utilization is 1 so let's make it above it :D
	var minUtilization float64 = 1.1
	for _, prefNic := range node.PrefNics {
		utilization := (float64(prefNic.TotalCapacity) - float64(prefNic.FreeCapacity)) / float64(prefNic.TotalCapacity)
		if minUtilization > utilization {
			minUtilization = utilization
			nic = prefNic
		}
	}

	if nic == nil {
		return nil, fmt.Errorf("Something went wrong couldn't calculate utilization for nic")
	}
	return nic, nil
}

func getLessUsedStoragePool(prefNic *PrefNic) (storagePool string, err error) {
	// maximal value for utilization is 1 so let's make it above it :D
	var minUtilization float64 = 1.1
	for _, sp := range prefNic.StoragePools {
		utilization := (float64(sp.TotalCapacity) - float64(sp.FreeCapacity)) / float64(sp.TotalCapacity)
		if minUtilization > utilization {
			minUtilization = utilization
			storagePool = sp.Name
		}
	}

	if storagePool == "" {
		return "", fmt.Errorf("Something went wrong couldn't calculate utilization for StoragePool")
	}
	return storagePool, nil
}

func pickStoragePoolFromNodes(ctx context.Context, nClient NodeLinstorClient, nodes []string) (*BalanceDecision, error) {

	util, err := getNodesUtil(ctx, nClient, nodes)
	if err != nil {
		return nil, err
	}

	node, err := getLessUsedNode(util)
	if err != nil {
		return nil, err
	}

	prefNic, err := getLessUsedNic(node)
	if err != nil {
		return nil, err
	}

	storagePool, err := getLessUsedStoragePool(prefNic)
	if err != nil {
		return nil, err
	}

	return &BalanceDecision{
		NodeName:        node.Name,
		StoragePoolName: storagePool,
	}, nil
}

// pick from Storage Nodes in the same Rack
func pickStoragePoolTopo(ctx context.Context, selectedNode string, nClient NodeLinstorClient, clientset kubernetes.Interface) (sp *BalanceDecision, err error) {
	rack, err := retrieveRackId(clientset, selectedNode)
	if err != nil {
		return nil, err
	}

	nodes, err := getStorageNodesInRack(rack, clientset)
	if err != nil {
		return nil, err
	}

	return pickStoragePoolFromNodes(ctx, nClient, nodes)

}

// pick from All Storage Nodes
func pickStoragePool(ctx context.Context, nClient NodeLinstorClient, clientset kubernetes.Interface) (sp *BalanceDecision, err error) {
	nodes, err := getStorageNodes(clientset)
	if err != nil {
		return nil, err
	}

	nodeList := []string{}
	for _, node := range nodes.Items {
		nodeList = append(nodeList, node.Name)
	}

	return pickStoragePoolFromNodes(ctx, nClient, nodeList)

}

type BalanceScheduler struct {
	log       *logrus.Entry
	client    *lapi.Client
	clientset kubernetes.Interface
}

func NewBalanceScheduler(log *logrus.Entry, lClient *lapi.Client) (b BalanceScheduler, err error) {
	clientset, err := k8sClient()
	if err != nil {
		return b, err
	}

	return BalanceScheduler{
		log:       log,
		client:    lClient,
		clientset: clientset,
	}, nil
}

func (b BalanceScheduler) deploy(ctx context.Context, vol *volume.Info, params parameters, node string, storagePool string) error {
	return b.client.Resources.Create(ctx, volToDiskfullResourceCreate(vol, params, node, storagePool))
}

func (b BalanceScheduler) Create(ctx context.Context, vol *volume.Info, req *csi.CreateVolumeRequest) error {
	b.log.WithFields(logrus.Fields{
		"volume": fmt.Sprintf("%+v", vol),
	}).Info("creating volume")

	params, err := newParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("unable to create volume due to bad parameters %+v: %v", vol.Parameters, err)
	}

	if !params.allowRemoteVolumeAccess {
		return fmt.Errorf("placementPolicyBalance cannot work on on local storage")
	}

	// If we don't care about local storage or nodes are being manually assigned
	// or there are no topology preferences skip the topology based assignment logic.
	topos := req.GetAccessibilityRequirements()
	if topos == nil {
		decision, err := pickStoragePool(ctx, b.client.Nodes, b.clientset)
		if err != nil {
			return err
		}

		// deploy to Node and StoragePool picked by Balancer
		if err := b.deploy(ctx, vol, params, decision.NodeName, decision.StoragePoolName); err != nil {
			return fmt.Errorf("cannot deploy volume: %s on Node: %s, and StoragePool: %s: %s", vol.ID, decision.NodeName, decision.StoragePoolName, err)
		}
	}

	// For now we do not support more than one Diskfull Resources so set remainingAssignments to 1
	remainingAssignments := 1

	for i, pref := range topos.GetPreferred() {
		// While there are still preferred nodes and remainingAssignments
		// attach resources diskfully to those nodes in order of most to least preferred.
		if p, ok := pref.GetSegments()[LinstorNodeTopologyKey]; ok && remainingAssignments > 0 {
			// If attachment fails move onto next most preferred volume.
			decision, err := pickStoragePoolTopo(ctx, p, b.client.Nodes, b.clientset)
			if err != nil {
				b.log.WithFields(logrus.Fields{
					"volumeID":                   vol.ID,
					"topologyPreference":         i,
					"topologyNode":               p,
					"remainingVolumeAssignments": remainingAssignments,
					"reason":                     err,
				}).Info("unable to pick StoragePool")
				continue
			}
			if err := b.deploy(ctx, vol, params, decision.NodeName, decision.StoragePoolName); err != nil {
				b.log.WithFields(logrus.Fields{
					"volumeID":                   vol.ID,
					"topologyPreference":         i,
					"topologyNode":               p,
					"remainingVolumeAssignments": remainingAssignments,
					"reason":                     err,
					"NodeName":                   decision.NodeName,
					"StoragePoolName":            decision.StoragePoolName,
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
	if remainingAssignments > 0 {
		return fmt.Errorf("unable to satisfy volume topology requirements for volume %s", vol.ID)
	}

	return nil
}

func (b BalanceScheduler) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	p, err := newParameters(vol.Parameters)
	if err != nil {
		return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	if p.allowRemoteVolumeAccess {
		r, err := b.client.Resources.GetAll(ctx, vol.ID)
		if err != nil {
			return nil, fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
		}
		nodes := deployedNodes(r)
		// all nodes will be in the same Rack so take only 1 of them
		rack, err := retrieveRackId(b.clientset, nodes[0])
		if err != nil {
			return nil, err
		}
		var topos = make([]*csi.Topology, 0)
		topos = append(topos, &csi.Topology{Segments: map[string]string{RackLabel: rack}})
		return topos, nil
	}

	return nil, nil
}

// TODO: it should be moved to util package
func volToDiskfullResourceCreate(vol *volume.Info, params parameters, node string, storagePool string) lapi.ResourceCreate {
	res := volToGenericResourceCreate(vol, params, node)
	res.Resource.Props[lc.KeyStorPoolName] = storagePool
	return res
}

// TODO: it should be moved to util package
func volToGenericResourceCreate(vol *volume.Info, params parameters, node string) lapi.ResourceCreate {
	return lapi.ResourceCreate{
		LayerList: params.layerList,
		Resource: lapi.Resource{
			Name:     vol.ID,
			NodeName: node,
			Props:    make(map[string]string, 1),
			Flags:    make([]string, 0),
		}}
}

// TODO: move to utils
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

// TODO: move to utils
func doesNotcontainAll(list []string, candidates ...string) bool {
	return !containsAll(list, candidates...)
}

// TODO: move to utils
func deployed(res lapi.Resource) bool {
	return doesNotcontainAll(res.Flags, lc.FlagDiskless)
}

// TODO: move to utils
func deployedNodes(res []lapi.Resource) []string {
	var nodes = make([]string, 0)
	for _, r := range res {
		if deployed(r) {
			nodes = append(nodes, r.NodeName)
		}
	}
	return nodes
}
