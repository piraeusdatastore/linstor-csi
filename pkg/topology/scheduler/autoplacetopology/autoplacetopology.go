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
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
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

func (s *Scheduler) Create(ctx context.Context, vol *volume.Info, req *csi.CreateVolumeRequest) error {
	log := s.log.WithField("volume", vol.ID)

	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("unable to determine volume parameters: %w", err)
	}

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

	topos := req.GetAccessibilityRequirements()
	log.WithField("requirements", topos).Trace("got topology requirement")

	for _, preferred := range topos.GetPreferred() {
		node, ok := preferred.GetSegments()[topology.LinstorNodeKey]
		if !ok {
			log.WithField("segment", preferred.GetSegments()).Trace("segment without node name, skipping")
			continue
		}

		log.WithField("preferred", node).Trace("try initial placement on a preferred node")

		// Just try a single autoplace request on one of the preferred nodes if possible. We use AutoPlace instead
		// of MakeAvailable to ensure we respect the user-defined constraint from the storage class.
		err := s.Resources.Autoplace(ctx, vol.ID, lapi.AutoPlaceRequest{
			SelectFilter: lapi.AutoSelectFilter{PlaceCount: 1, NodeNameList: []string{node}},
		})
		if err != nil {
			log.WithError(err).WithField("preferred", node).Trace("failed to autoplace")
		} else {
			log.WithField("preferred", node).Trace("successfully placed on preferred node")
			break
		}
	}

	// By now we should have placed a volume on one of the preferred nodes (or there were no preferred nodes). Now
	// we can try autoplacing the rest. Initially, we want to restrict ourselves to just the requisite nodes.
	var requisiteNodes []string

	for _, requisite := range topos.GetRequisite() {
		node, ok := requisite.GetSegments()[topology.LinstorNodeKey]
		if !ok {
			log.WithField("segment", requisite.GetSegments()).Trace("segment without node name, skipping")
			continue
		}

		requisiteNodes = append(requisiteNodes, node)
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

		err := s.Resources.Autoplace(ctx, vol.ID, req)
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

		err := s.Resources.Autoplace(ctx, vol.ID, lapi.AutoPlaceRequest{})
		if err != nil {
			return fmt.Errorf("failed to autoplace unconstraint replicas: %w", err)
		}
	}

	log.Trace("placement successful")

	return nil
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, vol)
}

func NewScheduler(c *lc.HighLevelClient, l *logrus.Entry) *Scheduler {
	return &Scheduler{HighLevelClient: c, log: l.WithField("scheduler", "autoplacetopology")}
}
