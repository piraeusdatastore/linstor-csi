package autoplacetopology

import (
	"context"
	"fmt"
	"sort"

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
		err := s.PlaceAccessibleToSegment(ctx, volId, preferred.GetSegments(), params.AllowRemoteVolumeAccess)
		if err != nil {
			log.WithError(err).Debug("failed to place on preferred segment")
		} else {
			log.Debug("placed accessible to preferred segment")
			break
		}
	}

	// By now we should have placed a volume on one of the preferred nodes (or there were no preferred nodes). Now
	// we can try autoplacing the rest. Initially, we want to restrict ourselves to just the requisite nodes.
	// However, we _can_ always expand to the accessible volumes as per remote volume policy.
	requisiteNodes := make(map[string]struct{})

	for _, requisite := range topologies.GetRequisite() {
		for _, seg := range params.AllowRemoteVolumeAccess.AccessibleSegments(requisite.GetSegments()) {
			log := log.WithField("segments", seg)

			nodes, err := s.NodesForTopology(ctx, seg)
			if err != nil {
				return fmt.Errorf("failed to get preferred node list from segments: %w", err)
			}

			log.WithField("nodes", nodes).Trace("got nodes for segment")

			for i := range nodes {
				requisiteNodes[nodes[i]] = struct{}{}
			}
		}
	}

	log.WithField("requisite", requisiteNodes).Trace("got requisite nodes")

	if len(requisiteNodes) > 0 {
		requisiteNodesList := make([]string, 0, len(requisiteNodes))
		for k := range requisiteNodes {
			requisiteNodesList = append(requisiteNodesList, k)
		}

		// Sort, for testing
		sort.Strings(requisiteNodesList)

		// We do have requisite nodes, so we need to autoplace just on those nodes.
		req := lapi.AutoPlaceRequest{
			SelectFilter: lapi.AutoSelectFilter{NodeNameList: requisiteNodesList},
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

// PlaceAccessibleToSegment tries to place a replica accessible to the given segment.
//
// Initially, placement on an exactly matching node is tried. If that is not possible, the remoteAccessPolicy is
// used to determine other nodes that would grant the given segment access.
func (s *Scheduler) PlaceAccessibleToSegment(ctx context.Context, volId string, segments map[string]string, remoteAccessPolicy volume.RemoteAccessPolicy) error {
	log := s.log.WithField("volume", volId).WithField("segments", segments)

	nodes, err := s.NodesForTopology(ctx, segments)
	if err != nil {
		return fmt.Errorf("failed to get preferred node list from segments: %w", err)
	}

	log.WithField("nodes", nodes).Trace("try initial placement on preferred nodes")

	apRequest := lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: nodes, PlaceCount: 1}}

	err = s.Resources.Autoplace(ctx, volId, apRequest)
	if err != nil {
		log.WithError(err).Trace("failed to autoplace")
	} else {
		log.Trace("successfully placed on preferred node")
		return nil
	}

	log.Trace("exact segments match failed, try with remote access policy")

	accessibleSegments := remoteAccessPolicy.AccessibleSegments(segments)

	log.WithField("accessibleSegments", accessibleSegments).Trace("got accessible segments")

	for _, seg := range accessibleSegments {
		log := log.WithField("segments", seg)

		nodes, err := s.NodesForTopology(ctx, seg)
		if err != nil {
			log.WithError(err).Warn("failed to get preferred node list from segments")
			continue
		}

		if len(nodes) == 0 {
			log.Trace("no matching node")
			continue
		}

		apRequest := lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{NodeNameList: nodes, PlaceCount: 1}}

		err = s.Resources.Autoplace(ctx, volId, apRequest)
		if err != nil {
			log.WithError(err).Trace("failed to autoplace")
		} else {
			log.Trace("successfully placed reachable for preferred node")
			return nil
		}
	}

	return fmt.Errorf("failed to place resource")
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, volId string, remoteAccessPolicy volume.RemoteAccessPolicy) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, volId, remoteAccessPolicy)
}

func NewScheduler(c *lc.HighLevelClient, l *logrus.Entry) *Scheduler {
	return &Scheduler{HighLevelClient: c, log: l.WithField("scheduler", "autoplacetopology")}
}
