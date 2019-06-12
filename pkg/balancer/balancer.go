package balancer

import (
	"context"
	"fmt"

	lapi "github.com/LINBIT/golinstor/client"
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

func GetInternalk8sClient() (clientset kubernetes.Interface, err error) {

	// setup k7s client
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
func RetrieveRackId(clientset kubernetes.Interface, nodeName string) (rack string, err error) {
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

func getNodesUtil(ctx context.Context, nClient NodeLinstorClient, nodesInRack []string) (nodes map[string]*Node, err error) {
	nodes = map[string]*Node{}
	for _, node := range nodesInRack {
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

func PickStoragePool(ctx context.Context, selectedNode string, nClient NodeLinstorClient) (sp *BalanceDecision, err error) {
	clientset, err := k8sClient()
	if err != nil {
		return nil, err
	}

	rack, err := RetrieveRackId(clientset, selectedNode)
	if err != nil {
		return nil, err
	}

	nodes, err := getStorageNodesInRack(rack, clientset)
	if err != nil {
		return nil, err
	}

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
