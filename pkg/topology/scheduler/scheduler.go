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

package scheduler

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

// Interface determines where to place volumes and where they are accessible from.
type Interface interface {
	Create(ctx context.Context, volId string, params *volume.Parameters, topologies *csi.TopologyRequirement) error
	AccessibleTopologies(ctx context.Context, volId string, remoteAccessPolicy volume.RemoteAccessPolicy) ([]*csi.Topology, error)
}
