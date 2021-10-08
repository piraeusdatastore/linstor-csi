package followtopology_test

import (
	"context"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/piraeusdatastore/linstor-csi/pkg/client/mocks"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler/followtopology"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

var (
	volumeId   = "test-volume"
	params     = &volume.Parameters{PlacementCount: 2}
	brokenNode = "node2"
)

type fakeError struct{}

func (f *fakeError) Error() string {
	return "fake"
}

func TestScheduler_Create(t *testing.T) {
	ctx := context.Background()

	m := mocks.ResourceProvider{}
	m.On("MakeAvailable", mock.Anything, volumeId, brokenNode, mock.Anything).Return(&fakeError{})
	m.On("MakeAvailable", mock.Anything, volumeId, mock.Anything, mock.Anything).Return(nil)
	m.On("Autoplace", mock.Anything, volumeId, mock.Anything).Return(nil)

	sched := followtopology.NewScheduler(&lc.HighLevelClient{Client: &lapi.Client{Resources: &m}}, logrus.WithField("test", t.Name()))

	t.Run("no requirements", func(t *testing.T) {
		// Asserts that if no requirement is given, normal autoplace is performed
		m.Calls = nil

		err := sched.Create(ctx, volumeId, params, nil)
		assert.NoError(t, err)
		m.AssertCalled(t, "Autoplace", mock.Anything, volumeId, mock.Anything)
		m.AssertNotCalled(t, "MakeAvailable", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("preferred", func(t *testing.T) {
		// Asserts that, if only preferred topologies are given, they are created first
		m.Calls = nil

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Preferred: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node1"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node3"}},
			},
		})
		assert.NoError(t, err)
		m.AssertNotCalled(t, "Autoplace", mock.Anything, volumeId, mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node1", mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node2", mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node3", mock.Anything)
	})

	t.Run("requisite + preferred", func(t *testing.T) {
		// Asserts that, if both requisite and preferred are given, first we try to place the preferred, then pick from
		// the remaining requisites
		m.Calls = nil

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
		m.AssertNotCalled(t, "Autoplace", mock.Anything, volumeId, mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node1", mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node2", mock.Anything)
		m.AssertNotCalled(t, "MakeAvailable", mock.Anything, volumeId, "node3", mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node4", mock.Anything)
	})

	t.Run("requisite", func(t *testing.T) {
		// Asserts that using only requisites will require placement on (one of) these nodes
		m.Calls = nil

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node3"}},
				{Segments: map[string]string{topology.LinstorNodeKey: "node4"}},
			},
		})
		assert.NoError(t, err)
		m.AssertNotCalled(t, "Autoplace", mock.Anything, volumeId, mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node2", mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node3", mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node4", mock.Anything)
	})

	t.Run("requisite impossible", func(t *testing.T) {
		// Asserts that scheduling reports an error in case non of the requisites could be fulfilled
		m.Calls = nil

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node2"}},
			},
		})
		assert.Error(t, err)
		m.AssertNotCalled(t, "Autoplace", mock.Anything, volumeId, mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node2", mock.Anything)
	})

	t.Run("requisite + autoplace", func(t *testing.T) {
		// Asserts that after filling requisites, the remaining replicas are placed using autoplace
		m.Calls = nil

		err := sched.Create(ctx, volumeId, params, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{topology.LinstorNodeKey: "node1"}},
			},
		})
		assert.NoError(t, err)
		m.AssertCalled(t, "Autoplace", mock.Anything, volumeId, mock.Anything)
		m.AssertCalled(t, "MakeAvailable", mock.Anything, volumeId, "node1", mock.Anything)
	})
}
