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

func (s *Scheduler) Create(ctx context.Context, vol *volume.Info, req *csi.CreateVolumeRequest) error {
	apRequest, err := vol.ToAutoPlace()
	if err != nil {
		return err
	}
	return s.Resources.Autoplace(ctx, vol.ID, apRequest)
}

func (s *Scheduler) AccessibleTopologies(ctx context.Context, vol *volume.Info) ([]*csi.Topology, error) {
	return s.GenericAccessibleTopologies(ctx, vol)
}
