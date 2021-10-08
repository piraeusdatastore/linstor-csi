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

package autoplace

import (
	"context"

	"github.com/LINBIT/golinstor/client"
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

func (s *Scheduler) Create(ctx context.Context, volId string, _ *volume.Parameters, _ *csi.TopologyRequirement) error {
	return s.Resources.Autoplace(ctx, volId, client.AutoPlaceRequest{})
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, volId string, allowDisklessAccess bool) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, volId, allowDisklessAccess)
}
