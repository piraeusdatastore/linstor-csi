/*
CSI Driver for Linstor
Copyright © 2019 LINBIT USA, LLC

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

package followtopology

import (
	"context"
	"fmt"
	"reflect"

	"github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Scheduler places volumes according to linstor's autoplace feature.
type Scheduler struct {
	*lc.HighLevelClient
	log *logrus.Entry
}

func NewScheduler(c *lc.HighLevelClient, l *logrus.Entry) *Scheduler {
	return &Scheduler{HighLevelClient: c, log: l}
}

func (s *Scheduler) Create(ctx context.Context, vol *volume.Info, req *csi.CreateVolumeRequest) error {
	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	remainingAssignments := int(params.PlacementCount)

	// See https://github.com/container-storage-interface/spec/blob/v1.4.0/csi.proto#L523
	// TLDR:
	// * If `Requisite` exists, we _have_ to use those up first.
	// * If `Requisite` and `Preferred` exists, we have `Preferred` ⊆ `Requisite`, and `Preferred` SHOULD be used first.
	// * If `Requisite` does not exist and `Preferred` exists, we SHOULD use `Preferred`.
	// * If both `Requisite` and `Preferred` do not exist, we can do what ever.

	topos := req.GetAccessibilityRequirements()
	remainingRequisites := topos.GetRequisite()
	remainingPreferred := topos.GetPreferred()

	placed := 0
	for placed < remainingAssignments {
		var segment map[string]string
		if len(remainingPreferred) > 0 {
			segment = remainingPreferred[0].GetSegments()
		} else if len(remainingRequisites) > 0 {
			segment = remainingRequisites[0].GetSegments()
		} else {
			break
		}

		remainingPreferred = deleteSegment(remainingPreferred, segment)
		remainingRequisites = deleteSegment(remainingRequisites, segment)

		p, ok := segment[topology.LinstorNodeKey]
		if !ok {
			continue
		}

		err = s.Resources.MakeAvailable(ctx, vol.ID, p, client.ResourceMakeAvailable{})
		if err != nil {
			s.log.WithFields(logrus.Fields{
				"volumeID":     vol.ID,
				"topologyNode": p,
				"reason":       err,
			}).Info("unable to satisfy topology preference, skipping...")

			continue
		}

		placed++
	}

	if placed == 0 && len(topos.GetRequisite()) > 0 {
		return status.Error(codes.ResourceExhausted, "None of the requisite topologies could be fulfilled")
	}

	if placed < remainingAssignments {
		err = s.Resources.Autoplace(ctx, vol.ID, client.AutoPlaceRequest{})
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteSegment(topos []*csi.Topology, segment map[string]string) []*csi.Topology {
	for i := range topos {
		if reflect.DeepEqual(topos[i].GetSegments(), segment) {
			topos = append(topos[:i], topos[i+1:]...)
			break
		}
	}

	return topos
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, vol)
}
