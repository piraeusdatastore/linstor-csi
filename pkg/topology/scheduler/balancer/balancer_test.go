package balancer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

type NodesService struct {
}

func equal(sliceA, sliceB []string) bool {
	if len(sliceA) != len(sliceB) {
		return false
	}

	for i, element := range sliceA {
		if element != sliceB[i] {
			return false
		}
	}

	return true
}

func createNode(clientSet kubernetes.Interface, labels map[string]string, nodeName string) {
	clientSet.CoreV1().Nodes().Create(
		context.Background(),
		&v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
				Name:   nodeName,
			},
		},
		metav1.CreateOptions{},
	)
}

func TestRetrieveRackId(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	nodeName := "storage1"
	rackName := "Rack7"
	labels := map[string]string{RackLabel: rackName}
	createNode(clientSet, labels, nodeName)
	rack, err := retrieveRackId(context.Background(), clientSet, nodeName)
	if err != nil {
		t.Errorf("Something went wrong: %v\n", err)
	} else if rack != rackName {
		t.Errorf("Should be Rack7 got %s\n", rack)
	}
}

func TestRetrieveRackIdNoLabel(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	nodeName := "storage1"
	labels := map[string]string{}
	createNode(clientSet, labels, nodeName)
	_, err := retrieveRackId(context.Background(), clientSet, nodeName)
	if err == nil {
		t.Error("retrieveRackId should fail")
	}
}

func TestRetrieveRackIdNoNode(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	nodeName := "storage1"
	_, err := retrieveRackId(context.Background(), clientSet, nodeName)
	if err == nil {
		t.Error("retrieveRackId should fail")
	}
}

func TestGetStorageNodes(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	storLabels := map[string]string{StorageLabel: "true"}
	nonStorLabels := map[string]string{}
	createNode(clientSet, storLabels, "storage1")
	createNode(clientSet, storLabels, "storage2")
	createNode(clientSet, storLabels, "storage3")
	createNode(clientSet, nonStorLabels, "compute1")
	nodes, err := getStorageNodes(context.Background(), clientSet)
	if err != nil {
		t.Errorf("someting went wrong: %v", err)
	}
	for _, node := range nodes.Items {
		if !strings.HasPrefix(node.Name, "storage") {
			t.Errorf("Node %s is not a storage node", node.Name)
		}
	}
}

func createNodesInRack(rack string, nodes *v1.NodeList, nodesInRack []string) {
	rackLabels := map[string]string{RackLabel: rack}
	for _, node := range nodesInRack {
		nodes.Items = append(nodes.Items, v1.Node{
			TypeMeta: metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{
				Name:   node,
				Labels: rackLabels,
			},
			Spec: v1.NodeSpec{},
		})
	}
}

func TestGetNodesInRack(t *testing.T) {
	nodesRack1 := []string{"storage1", "storage2", "storage3"}
	nodesRack2 := []string{"storage5", "storage3", "storage4"}
	nodes := &v1.NodeList{
		Items: []v1.Node{},
	}
	createNodesInRack("rack1", nodes, nodesRack1)
	createNodesInRack("rack2", nodes, nodesRack2)
	nodesInRack1, err := getNodesInRack("rack1", nodes)
	if err != nil {
		t.Errorf("Something went wrong: %v", err)
	}
	if !equal(nodesInRack1, nodesRack1) {
		t.Errorf("Expected: %v\n, got: %v\n", nodesRack1, nodesInRack1)
	}

	nodesInRack2, err := getNodesInRack("rack2", nodes)
	if err != nil {
		t.Errorf("Something went wrong: %v", err)
	}
	if !equal(nodesInRack2, nodesRack2) {
		t.Errorf("Expected: %v\n, got: %v\n", nodesRack1, nodesInRack1)
	}
}

func TestGetNodesInRackNoNodes(t *testing.T) {
	nodesRack1 := []string{"storage1", "storage2", "storage3"}
	nodesRack2 := []string{"storage5", "storage3", "storage4"}
	nodes := &v1.NodeList{
		Items: []v1.Node{},
	}
	createNodesInRack("rack1", nodes, nodesRack1)
	createNodesInRack("rack2", nodes, nodesRack2)
	nodesInRack3, err := getNodesInRack("rack3", nodes)
	if err == nil {
		t.Errorf("Something went wrong: should got empy slice got %v", nodesInRack3)
	}
}

func TestGetStorageNodesInRack(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	storLabelsRack1 := map[string]string{StorageLabel: "true", RackLabel: "rack1"}
	storLabelsRack2 := map[string]string{StorageLabel: "true", RackLabel: "rack2"}
	nonStorLabels := map[string]string{}
	createNode(clientSet, storLabelsRack1, "storage1")
	createNode(clientSet, storLabelsRack1, "storage2")
	createNode(clientSet, storLabelsRack2, "storage3")
	createNode(clientSet, nonStorLabels, "compute1")

	nodes, err := getStorageNodesInRack(context.Background(), "rack1", clientSet)
	if err != nil {
		t.Errorf("Something went wrong: %v", err)
	}
	expectedNodes := []string{"storage1", "storage2"}
	if !equal(nodes, expectedNodes) {
		t.Errorf("Expected %v got %v", expectedNodes, nodes)
	}
}

func TestGetStorageNodesInRackFail(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	storLabelsRack1 := map[string]string{StorageLabel: "true", RackLabel: "rack1"}
	storLabelsRack2 := map[string]string{StorageLabel: "true", RackLabel: "rack2"}
	nonStorLabels := map[string]string{}
	createNode(clientSet, storLabelsRack1, "storage1")
	createNode(clientSet, storLabelsRack1, "storage2")
	createNode(clientSet, storLabelsRack2, "storage3")
	createNode(clientSet, nonStorLabels, "compute1")

	nodes, err := getStorageNodesInRack(context.Background(), "rack3", clientSet)
	if err == nil {
		t.Error("Something went wrong: expected Error")
	}
	expectedNodes := []string{}
	if !equal(nodes, expectedNodes) {
		t.Errorf("Expected %v got %v", expectedNodes, nodes)
	}
}

// Nodename has to end with number
func (ns *NodesService) GetStoragePools(ctx context.Context, nodeName string, opts ...*lapi.ListOpts) ([]lapi.StoragePool, error) {
	if nodeName == "fail" {
		return []lapi.StoragePool{}, fmt.Errorf("Disaster")
	}
	offset, err := strconv.Atoi(string(nodeName[len(nodeName)-1]))
	if err != nil {
		return []lapi.StoragePool{}, fmt.Errorf("The node has to end with a number from 0-9")
	}
	return []lapi.StoragePool{
		lapi.StoragePool{
			StoragePoolName: "sp1",
			NodeName:        nodeName,
			FreeCapacity:    10 + int64(offset),
			TotalCapacity:   100,
			Props:           map[string]string{PrefNicPropKey: "eno1"},
			ProviderKind:    lapi.LVM,
		},
		lapi.StoragePool{
			StoragePoolName: "sp2",
			NodeName:        nodeName,
			FreeCapacity:    80 + int64(offset),
			TotalCapacity:   100,
			Props:           map[string]string{PrefNicPropKey: "eno2"},
			ProviderKind:    lapi.LVM,
		},
	}, nil
}

func TestGetNodesUtil(t *testing.T) {
	nodesInRack := []string{"storage1", "storage2"}
	tCtx := context.Background()
	nodes, err := getNodesUtil(tCtx, &NodesService{}, nodesInRack)
	assert.Nil(t, err)

	expectedOutput := map[string]*Node{
		"storage1": &Node{
			Name:          "storage1",
			TotalCapacity: 200,
			FreeCapacity:  92,
			PrefNics: map[string]*PrefNic{
				"eno1": &PrefNic{
					Name:          "eno1",
					TotalCapacity: 100,
					FreeCapacity:  11,
					StoragePools: []*StoragePool{
						&StoragePool{
							Name:          "sp1",
							TotalCapacity: 100,
							FreeCapacity:  11,
						},
					},
				},
				"eno2": &PrefNic{
					Name:          "eno2",
					TotalCapacity: 100,
					FreeCapacity:  81,
					StoragePools: []*StoragePool{
						&StoragePool{
							Name:          "sp2",
							TotalCapacity: 100,
							FreeCapacity:  81,
						},
					},
				},
			},
		},
		"storage2": &Node{
			Name:          "storage2",
			TotalCapacity: 200,
			FreeCapacity:  94,
			PrefNics: map[string]*PrefNic{
				"eno1": &PrefNic{
					Name:          "eno1",
					TotalCapacity: 100,
					FreeCapacity:  12,
					StoragePools: []*StoragePool{
						&StoragePool{
							Name:          "sp1",
							TotalCapacity: 100,
							FreeCapacity:  12,
						},
					},
				},
				"eno2": &PrefNic{
					Name:          "eno2",
					TotalCapacity: 100,
					FreeCapacity:  82,
					StoragePools: []*StoragePool{
						&StoragePool{
							Name:          "sp2",
							TotalCapacity: 100,
							FreeCapacity:  82,
						},
					},
				},
			},
		},
	}
	assert.Equal(t, expectedOutput, nodes, "shoudl be equal")
}

func TestGetNodesUtilFail(t *testing.T) {
	nodesInRack := []string{"fail"}
	tCtx := context.Background()
	_, err := getNodesUtil(tCtx, &NodesService{}, nodesInRack)
	assert.NotNil(t, err)
}

func TestGetLessUsedNode(t *testing.T) {
	testCases := []struct {
		Nodes      map[string]*Node
		ChosenNode string
	}{
		{
			Nodes: map[string]*Node{
				"storage1": &Node{
					FreeCapacity:  10,
					TotalCapacity: 100,
					Name:          "storage1",
				},
				"storage2": &Node{
					FreeCapacity:  20,
					TotalCapacity: 100,
					Name:          "storage2",
				},
			},
			ChosenNode: "storage2",
		},
		{
			Nodes: map[string]*Node{
				"storage1": &Node{
					FreeCapacity:  30000,
					TotalCapacity: 100000,
					Name:          "storage1",
				},
				"storage2": &Node{
					FreeCapacity:  20,
					TotalCapacity: 100,
					Name:          "storage2",
				},
				"storage3": &Node{
					FreeCapacity:  1,
					TotalCapacity: 2,
					Name:          "storage3",
				},
			},
			ChosenNode: "storage3",
		},
	}

	for _, testCase := range testCases {
		node, err := getLessUsedNode(testCase.Nodes)
		assert.Nil(t, err)
		assert.Equal(t, testCase.ChosenNode, node.Name, "nodeName shoudl be the same")
	}
}

func TestGetLessUsedNodeFail(t *testing.T) {
	_, err := getLessUsedNode(map[string]*Node{})
	assert.NotNil(t, err)
}

func TestGetLessUsedNic(t *testing.T) {
	testCases := []struct {
		Node      *Node
		ChosenNic string
	}{
		{
			Node: &Node{
				Name: "storage1",
				PrefNics: map[string]*PrefNic{
					"eno1": &PrefNic{
						Name:          "eno1",
						FreeCapacity:  20,
						TotalCapacity: 100,
					},
				},
			},
			ChosenNic: "eno1",
		},
		{
			Node: &Node{
				Name: "storage1",
				PrefNics: map[string]*PrefNic{
					"eno1": &PrefNic{
						Name:          "eno1",
						FreeCapacity:  20,
						TotalCapacity: 70083299,
					},
					"eno2": &PrefNic{
						Name:          "eno2",
						FreeCapacity:  80,
						TotalCapacity: 100,
					},
					"eno3": &PrefNic{
						Name:          "eno3",
						FreeCapacity:  2,
						TotalCapacity: 4,
					},
				},
			},
			ChosenNic: "eno2",
		},
	}

	for _, testCase := range testCases {
		nic, err := getLessUsedNic(testCase.Node)
		assert.Nil(t, err)
		assert.Equal(t, testCase.ChosenNic, nic.Name, "nodeName shoudl be the same")
	}
}

func TestGetLessUsedNicFail(t *testing.T) {
	_, err := getLessUsedNic(&Node{})
	assert.NotNil(t, err)
}

func TestGetStoragePool(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	nodeName := "compute1"
	rackName := "rack1"
	labels := map[string]string{RackLabel: rackName}
	createNode(clientSet, labels, nodeName)
	storLabels := map[string]string{StorageLabel: "true", RackLabel: rackName}
	createNode(clientSet, storLabels, "storage1")
	createNode(clientSet, storLabels, "storage2")
	createNode(clientSet, storLabels, "storage3")
	tCtx := context.Background()
	sp, err := pickStoragePoolTopo(tCtx, "compute1", &NodesService{}, clientSet)
	assert.Nil(t, err)
	assert.Equal(t, &BalanceDecision{
		StoragePoolName: "sp2",
		NodeName:        "storage3",
	}, sp, "The chosen storage pool")
}
