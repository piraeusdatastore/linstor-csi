package autoplacetopology

import (
	"context"
	"fmt"

	linstor "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Scheduler places volumes according to both CSI Topology and user-provided autoplace parameters.
//
// This scheduler works like autoplace.Scheduler with a few key differences:
// * If a `Requisite` topology is requested, all placement will be restricted onto those nodes.
// * If a `Preferred` topology is requested, this scheduler will try to place at least one volume on those nodes.
// This scheduler complies with the CSI spec for topology:
//   https://github.com/container-storage-interface/spec/blob/v1.4.0/csi.proto#L523
type Scheduler struct {
	*lc.HighLevelClient
	log *logrus.Entry
}

// Ensure Scheduler conforms to scheduler.Interface.
var _ scheduler.Interface = &Scheduler{}

func (s *Scheduler) Create(ctx context.Context, volId string, params *volume.Parameters, topologies *csi.TopologyRequirement) error {
	log := s.log.WithField("volume", volId)

	// The CSI spec mandates:
	// * If `Requisite` exists, we _have_ to use those up first.
	// * If `Requisite` and `Preferred` exists, we have `Preferred` ⊆ `Requisite`, and `Preferred` SHOULD be used first.
	// * If `Requisite` does not exist and `Preferred` exists, we SHOULD use `Preferred`.
	// * If both `Requisite` and `Preferred` do not exist, we can do what ever.
	//
	// Making this compatible with LINSTOR autoplace parameters can be quite tricky. For example, a "DoNotPlaceWith"
	// constraint could mean we are not able to place the volume on the first preferred node.
	//
	// This scheduler works by first trying to place a volume on one of the preferred nodes. This should optimize
	// placement in case of late volume binding (where the first preferred node is the node starting the pod).
	// Then, normal autoplace happens, restricted to the requisite nodes. If there are still replicas to schedule, use
	// autoplace again, this time without extra placement constraints from topology.

	log.WithField("requirements", topologies).Trace("got topology requirement")

	for _, preferred := range topologies.GetPreferred() {
		log := log.WithField("segments", preferred.GetSegments())

		nodes, err := s.NodesForTopology(ctx, preferred.GetSegments())
		if err != nil {
			return fmt.Errorf("failed to get preferred node list from segments: %w", err)
		}

		log.WithField("nodes", nodes).Trace("try initial placement on preferred nodes")

		apRequest := lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: nodes}}

		if len(nodes) < int(params.PlacementCount) {
			apRequest.SelectFilter.PlaceCount = int32(len(nodes))
		}

		err = s.Resources.Autoplace(ctx, volId, apRequest)
		if err != nil {
			log.WithError(err).Trace("failed to autoplace")
		} else {
			log.Trace("successfully placed on preferred node")
			break
		}
	}

	// By now we should have placed a volume on one of the preferred nodes (or there were no preferred nodes). Now
	// we can try autoplacing the rest. Initially, we want to restrict ourselves to just the requisite nodes.
	var requisiteNodes []string

	for _, requisite := range topologies.GetRequisite() {
		nodes, err := s.NodesForTopology(ctx, requisite.GetSegments())
		if err != nil {
			return fmt.Errorf("failed to get preferred node list from segments: %w", err)
		}

		requisiteNodes = append(requisiteNodes, nodes...)
	}

	log.WithField("requisite", requisiteNodes).Trace("got requisite nodes")

	if len(requisiteNodes) > 0 {
		// We do have requisite nodes, so we need to autoplace just on those nodes.
		req := lapi.AutoPlaceRequest{
			SelectFilter: lapi.AutoSelectFilter{NodeNameList: requisiteNodes},
		}

		// We might need to restrict autoplace here. We could have just one requisite node, but a placement count of 3.
		// In this scenario, we want to autoplace on the requisite node, then run another autoplace with no restriction
		// to place the remaining replicas.
		if len(requisiteNodes) < int(params.PlacementCount) {
			req.SelectFilter.PlaceCount = int32(len(requisiteNodes))
		}

		log.WithField("requisite", requisiteNodes).Trace("try placement on requisite nodes")

		err := s.Resources.Autoplace(ctx, volId, req)
		if err != nil {
			if lapi.IsApiCallError(err, linstor.FailNotEnoughNodes) {
				// We need a special return code when the requisite could not be fulfilled
				return status.Errorf(codes.ResourceExhausted, "failed to enough replicas on requisite nodes: %v", err)
			}

			return fmt.Errorf("failed to autoplace constraint replicas: %w", err)
		}
	}

	if len(requisiteNodes) < int(params.PlacementCount) {
		// By now we should have replicas on all requisite nodes (if any). Any remaining replicas we can place
		// independent of topology constraints, so we just run an unconstraint autoplace.
		log.Trace("try placement without topology constraints")

		err := s.Resources.Autoplace(ctx, volId, lapi.AutoPlaceRequest{})
		if err != nil {
			return fmt.Errorf("failed to autoplace unconstraint replicas: %w", err)
		}
	}

	log.Trace("placement successful")

	return nil
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, volId string, allowDisklessAccess bool) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, volId, allowDisklessAccess)
}

func NewScheduler(c *lc.HighLevelClient, l *logrus.Entry) *Scheduler {
	return &Scheduler{HighLevelClient: c, log: l.WithField("scheduler", "autoplacetopology")}
}
