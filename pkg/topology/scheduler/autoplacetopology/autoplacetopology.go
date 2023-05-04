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
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor/util"
	"github.com/piraeusdatastore/linstor-csi/pkg/slice"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology/scheduler"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Scheduler places volumes according to both CSI Topology and user-provided autoplace parameters.
//
// This scheduler works like autoplace.Scheduler with a few key differences:
// * If a `Requisite` topology is requested, all placement will be restricted onto those nodes.
// * If a `Preferred` topology is requested, this scheduler will try to place at least one volume on those nodes.
// This scheduler complies with the CSI spec for topology:
//
//	https://github.com/container-storage-interface/spec/blob/v1.4.0/csi.proto#L523
type Scheduler struct {
	*lc.HighLevelClient
	log *logrus.Entry
}

// Ensure Scheduler conforms to scheduler.Interface.
var _ scheduler.Interface = &Scheduler{}

// Create places volumes according to the constraints given by the LINSTOR SelectFilter and topology requirements by CSI
//
// The CSI spec mandates:
// * If `Requisite` exists, we _have_ to use those up first.
// * If `Requisite` and `Preferred` exists, we have `Preferred` ⊆ `Requisite`, and `Preferred` SHOULD be used first.
// * If `Requisite` does not exist and `Preferred` exists, we SHOULD use `Preferred`.
// * If both `Requisite` and `Preferred` do not exist, we can do what ever.
//
// Making this compatible with LINSTOR autoplace parameters can be quite tricky. For example, a "DoNotPlaceWith"
// constraint could mean we are not able to place the volume on the first preferred node.
//
// To fulfil all these requirements, this scheduler executes the following steps:
//  1. It collects the list of requisite nodes. Note that only one replica on any requisite nodes is enough to fulfil
//     the CSI spec.
//     1a. Bail out early if we already have the required replica count _and_ any requisite node has a replica.
//  2. Iterate over the preferred segments until we successfully placed a replica
//     2a. Bail out early if we now have the required replica count _and_ any requisite node has a replica.
//  3. Try to place remaining replicas on requisite nodes.
//  4. Try to place remaining replicas on any nodes.
func (s *Scheduler) Create(ctx context.Context, volId string, params *volume.Parameters, topologies *csi.TopologyRequirement) error {
	log := s.log.WithField("volume", volId)

	// Step 1: collect requisites nodes according to CSI spec
	log.Debug("collect requisite nodes")

	requisiteNodes, err := s.HighLevelClient.GetAllTopologyNodes(ctx, params.AllowRemoteVolumeAccess, topologies.GetRequisite())
	if err != nil {
		return fmt.Errorf("failed to get requisite node list: %w", err)
	}

	// Step 1a: bail early if resource already exists and matches replica count and requisite nodes
	log.Debug("check if requisites already done")

	diskfulNodes, err := s.GetCurrentDiskfulNodes(ctx, volId)
	if err != nil {
		return err
	}

	for _, node := range diskfulNodes {
		if slice.ContainsString(requisiteNodes, node) && len(diskfulNodes) >= int(params.PlacementCount) {
			log.Debug("resource already deployed with minimum replica count and on at least one requisite node")
			return nil
		}
	}

	// Step 2: Try to place a replica on any preferred segment
	log.WithField("requirements", topologies).Trace("got topology requirement")

	for _, preferred := range topologies.GetPreferred() {
		err := s.PlaceOneAccessibleToSegment(ctx, volId, preferred.GetSegments(), params.AllowRemoteVolumeAccess, diskfulNodes)
		if err != nil {
			log.WithError(err).Debug("failed to place on preferred segment")
		} else {
			log.Debug("placed accessible to preferred segment")
			break
		}
	}

	// Step 2a: bail early if resource already matches replica count and requisite nodes
	log.Debug("check if requisites already done after preferred")

	diskfulNodes, err = s.GetCurrentDiskfulNodes(ctx, volId)
	if err != nil {
		return err
	}

	for _, node := range diskfulNodes {
		if slice.ContainsString(requisiteNodes, node) && len(diskfulNodes) >= int(params.PlacementCount) {
			log.Debug("resource already deployed with minimum replica count and on at least one requisite node")
			return nil
		}
	}

	// Step 3:
	// By now we should have placed a volume on one of the preferred nodes (or there were no preferred nodes). Now
	// we can try autoplacing the rest (and the rest should be > 0). Initially, we want to restrict ourselves
	// to just the requisite nodes. However, we _can_ always expand to the accessible volumes as per remote volume
	// policy.
	log.WithField("requisite", requisiteNodes).Trace("got requisite nodes")

	if len(requisiteNodes) > 0 {
		// Sort, for testing
		sort.Strings(requisiteNodes)

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

	// Step 4:
	// By now we should have replicas on all requisite nodes (if any). Any remaining replicas we can place
	// independent of topology constraints, so we just run an unconstrained autoplace.
	if len(requisiteNodes) < int(params.PlacementCount) {
		log.Trace("try placement without topology constraints")

		err := s.Resources.Autoplace(ctx, volId, lapi.AutoPlaceRequest{})
		if err != nil {
			return fmt.Errorf("failed to autoplace unconstraint replicas: %w", err)
		}
	}

	log.Trace("placement successful")

	return nil
}

// PlaceOneAccessibleToSegment tries to place a replica accessible to the given segment.
//
// Initially, placement on an exactly matching node is tried. If that is not possible, the remoteAccessPolicy is
// used to determine other nodes that would grant the given segment access.
func (s *Scheduler) PlaceOneAccessibleToSegment(ctx context.Context, volId string, segments map[string]string, remoteAccessPolicy volume.RemoteAccessPolicy, existingNodes []string) error {
	log := s.log.WithField("volume", volId).WithField("segments", segments)

	nodes, err := s.NodesForTopology(ctx, segments)
	if err != nil {
		return fmt.Errorf("failed to get preferred node list from segments: %w", err)
	}

	for _, node := range nodes {
		if slice.ContainsString(existingNodes, node) {
			log.WithField("node", node).Debug("already deployed on preferred node")
			return nil
		}
	}

	log.WithField("nodes", nodes).Trace("try placement on preferred nodes")

	// NB: we use additional place count here, because the resource might already be partially placed. In that case
	// LINSTOR would reject a normal "place count = 1" request. place count still needs to be set to 1, otherwise
	// golinstor omits the parameter, causing LINSTOR to insert the RG default value. This then fails because
	// it tries to place >1 replicas on a node list of potential size 1.
	apRequest := lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{
		NodeNameList:         nodes,
		AdditionalPlaceCount: 1,
		PlaceCount:           1,
	}}

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

		// NB: additional place count here, same reason as above
		apRequest := lapi.AutoPlaceRequest{SelectFilter: lapi.AutoSelectFilter{
			NodeNameList:         nodes,
			AdditionalPlaceCount: 1,
			PlaceCount:           1,
		}}

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

func (s *Scheduler) GetCurrentDiskfulNodes(ctx context.Context, volId string) ([]string, error) {
	s.log.Debug("find existing diskful resources")

	existingRes, err := s.Resources.GetAll(ctx, volId)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing resources: %w", err)
	}

	return util.DeployedDiskfullyNodes(existingRes), nil
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, volId string, remoteAccessPolicy volume.RemoteAccessPolicy) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, volId, remoteAccessPolicy)
}

func NewScheduler(c *lc.HighLevelClient, l *logrus.Entry) *Scheduler {
	return &Scheduler{HighLevelClient: c, log: l.WithField("scheduler", "autoplacetopology")}
}
