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

	lc "github.com/LINBIT/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/LINBIT/linstor-csi/pkg/topology"
	"github.com/LINBIT/linstor-csi/pkg/volume"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
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
	topos := req.GetAccessibilityRequirements()
	if topos == nil {
		return fmt.Errorf("no volume topologies, unable to schedule volume %s", vol.ID)
	}

	params, err := volume.NewParameters(vol.Parameters)
	if err != nil {
		return fmt.Errorf("unable to determine AccessibleTopologies: %v", err)
	}

	remainingAssignments := params.PlacementCount

	for i, pref := range topos.GetPreferred() {
		// While there are still preferred nodes and remainingAssignments
		// attach resources diskfully to those nodes in order of most to least preferred.
		if p, ok := pref.GetSegments()[topology.LinstorNodeKey]; ok && remainingAssignments > 0 {
			drc, err := vol.ToDiskfullResourceCreate(p)
			if err != nil {
				return err
			}
			// If attachment fails move onto next most preferred volume.
			if err := s.Resources.Create(ctx, drc); err != nil {
				s.log.WithFields(logrus.Fields{
					"volumeID":                   vol.ID,
					"topologyPreference":         i,
					"topologyNode":               p,
					"totalVolumeCount":           params.PlacementCount,
					"remainingVolumeAssignments": remainingAssignments,
					"reason":                     err,
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
	// and local storage is required.
	if params.PlacementCount == remainingAssignments && !params.AllowRemoteVolumeAccess {
		return fmt.Errorf("unable to satisfy volume topology requirements for volume %s", vol.ID)
	}

	// If params.placementCount is higher than the number of assigned nodes,
	// let autoplace the rest.
	apRequest, err := vol.ToAutoPlace()
	if err != nil {
		return err
	}
	return s.Resources.Autoplace(ctx, vol.ID, apRequest)
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, vol)
}
