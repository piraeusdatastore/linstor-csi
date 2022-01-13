package autoplacetopology_test

import (
	"context"
	"testing"

	linstor "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/piraeusdatastore/linstor-csi/pkg/client/mocks"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/autoplacetopology"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

var (
	volumeId = "test-volume"
	params   = &volume.Parameters{PlacementCount: 3, AllowRemoteVolumeAccess: volume.RemoteAccessPolicyLocalOnly}
	// NB: we need this weird casting to make go happy.
	n              = uint64(linstor.FailNotEnoughNodes)
	autoplaceError = lapi.ApiCallError{lapi.ApiCallRc{RetCode: int64(n)}}
	nodes          = []lapi.Node{
		{Name: "node1"},
		{Name: "node2"},
		{Name: "node3"},
		{Name: "node4"},
	}
)

func TestScheduler_Create(t *testing.T) {
	ctx := context.Background()

	t.Run("no requirements", func(t *testing.T) {
		// Asserts that if no requirement is given, normal autoplace is performed
		rm := mocks.ResourceProvider{}
		nm := mocks.NodeProvider{}
		sched := autoplacetopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &rm, Nodes: &nm}}, logrus.WithField("test", t.Name()))

		rm.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, volumeId}, ReturnArguments: mock.Arguments{nil, nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: []string{"node1", "node2", "node3", "node4"}}}}, ReturnArguments: mock.Arguments{nil}},
		}
		nm.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, &lapi.ListOpts{}}, ReturnArguments: mock.Arguments{nodes, nil}},
		}

		err := sched.Create(ctx, volumeId, params, nil)
		assert.NoError(t, err)
		rm.AssertExpectations(t)
		nm.AssertExpectations(t)
	})

	t.Run("preferred", func(t *testing.T) {
		// Asserts that, if only preferred topologies are given, one is created first
		rm := mocks.ResourceProvider{}
		nm := mocks.NodeProvider{}
		sched := autoplacetopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &rm, Nodes: &nm}}, logrus.WithField("test", t.Name()))

		rm.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, volumeId}, ReturnArguments: mock.Arguments{nil, nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{AdditionalPlaceCount: 1, PlaceCount: 1, NodeNameList: []string{"node3"}}}}, ReturnArguments: mock.Arguments{nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: []string{"node1", "node2", "node3", "node4"}}}}, ReturnArguments: mock.Arguments{nil}},
		}
		nm.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, &lapi.ListOpts{}}, ReturnArguments: mock.Arguments{nodes, nil}},
		}

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node3"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node1"}},
			},
		})
		assert.NoError(t, err)
		rm.AssertExpectations(t)
		nm.AssertExpectations(t)
	})

	t.Run("preferred with failure", func(t *testing.T) {
		// Asserts that, if only preferred topologies are given, one is created first
		rm := mocks.ResourceProvider{}
		nm := mocks.NodeProvider{}
		sched := autoplacetopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &rm, Nodes: &nm}}, logrus.WithField("test", t.Name()))

		rm.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, volumeId}, ReturnArguments: mock.Arguments{nil, nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{AdditionalPlaceCount: 1, PlaceCount: 1, NodeNameList: []string{"node3"}}}}, ReturnArguments: mock.Arguments{autoplaceError}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{AdditionalPlaceCount: 1, PlaceCount: 1, NodeNameList: []string{"node1"}}}}, ReturnArguments: mock.Arguments{nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: []string{"node1", "node2", "node3", "node4"}}}}, ReturnArguments: mock.Arguments{nil}},
		}
		nm.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, &lapi.ListOpts{}}, ReturnArguments: mock.Arguments{nodes, nil}},
		}

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node3"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node1"}},
			},
		})
		assert.NoError(t, err)
		rm.AssertExpectations(t)
		nm.AssertExpectations(t)
	})

	t.Run("requisite + preferred", func(t *testing.T) {
		// Asserts that, if both requisite and preferred are given, first we try to place the preferred, then pick from
		// the remaining requisites
		m := mocks.ResourceProvider{}
		sched := autoplacetopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &m}}, logrus.WithField("test", t.Name()))

		m.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, volumeId}, ReturnArguments: mock.Arguments{nil, nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{AdditionalPlaceCount: 1, PlaceCount: 1, NodeNameList: []string{"node2"}}}}, ReturnArguments: mock.Arguments{nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: []string{"node1", "node2", "node3", "node4"}}}}, ReturnArguments: mock.Arguments{nil}},
		}

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node1"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node3"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node4"}},
			},
			Preferred: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node4"}},
			},
		})
		assert.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("requisite", func(t *testing.T) {
		// Asserts that using only requisites will require placement on (one of) these nodes
		m := mocks.ResourceProvider{}
		sched := autoplacetopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &m}}, logrus.WithField("test", t.Name()))

		m.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, volumeId}, ReturnArguments: mock.Arguments{nil, nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: []string{"node2", "node3", "node4"}}}}, ReturnArguments: mock.Arguments{nil}},
		}

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node3"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node4"}},
			},
		})
		assert.NoError(t, err)
		m.AssertExpectations(t)
	})

	t.Run("requisite impossible", func(t *testing.T) {
		// Asserts that scheduling reports an error in case non of the requisites could be fulfilled
		m := mocks.ResourceProvider{}
		sched := autoplacetopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &m}}, logrus.WithField("test", t.Name()))

		m.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, volumeId}, ReturnArguments: mock.Arguments{nil, nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: []string{"node2", "node3", "node4"}}}}, ReturnArguments: mock.Arguments{autoplaceError}},
		}

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node3"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node4"}},
			},
		})
		assert.Error(t, err)
		assert.IsType(t, status.Error(codes.ResourceExhausted, ""), err)
		m.AssertExpectations(t)
	})

	t.Run("requisite + autoplace", func(t *testing.T) {
		// Asserts that after filling requisites, the remaining replicas are placed using autoplace.
		m := mocks.ResourceProvider{}
		sched := autoplacetopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &m}}, logrus.WithField("test", t.Name()))

		m.ExpectedCalls = []*mock.Call{
			{Method: "GetAll", Arguments: mock.Arguments{mock.Anything, volumeId}, ReturnArguments: mock.Arguments{nil, nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{PlaceCount: 2, NodeNameList: []string{"node1", "node2"}}}}, ReturnArguments: mock.Arguments{nil}},
			{Method: "Autoplace", Arguments: mock.Arguments{mock.Anything, volumeId, lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{}}}, ReturnArguments: mock.Arguments{nil}},
		}

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node1"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
			},
		})
		assert.NoError(t, err)
		m.AssertExpectations(t)
	})
}
