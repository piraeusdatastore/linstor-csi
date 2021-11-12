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

package manual

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"

	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Scheduler places volumes according to linstor's autoplace feature.
type Scheduler struct {
	*lc.HighLevelClient
}

func NewScheduler(c *lc.HighLevelClient) *Scheduler {
	return &Scheduler{HighLevelClient: c}
}

func (s *Scheduler) Create(ctx context.Context, volId string, params *volume.Parameters, _ *csi.TopologyRequirement) error {
	manualPlacements, err := params.ToResourceCreateList(volId)
	if err != nil {
		return err
	}

	for _, placement := range manualPlacements {
		err := s.Resources.Create(ctx, placement)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, volId string, remoteAccessPolicy volume.RemoteAccessPolicy) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, volId, remoteAccessPolicy)
}
