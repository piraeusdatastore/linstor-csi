/*
CSI Driver for Linstor
Copyright © 2018 LINBIT USA, LLC

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

package linstor

import (
	lc "github.com/LINBIT/golinstor"
)

const (
	// DriverName is the name used in CSI calls for this driver.
	DriverName = "linstor.csi.linbit.com"

	// LinstorBackupKVName is the name of the KV store used to map L2L backups to local snapshot names
	LinstorBackupKVName = "csi-backup-mapping"

	// LegacyParameterPassKey is the Aux props key in linstor where serialized CSI parameters
	// are stored.
	LegacyParameterPassKey = lc.NamespcAuxiliary + "/csi-volume-annotations"

	// PropertyProvisioningCompletedBy is the Aux props key in LINSTOR intentifying this resource as
	// fully provisioned by this plugin.
	PropertyProvisioningCompletedBy = lc.NamespcAuxiliary + "/csi-provisioning-completed-by"

	// PropertyCreatedFor is the Aux props key in linstor used to identify why a specific object (for example, a
	// resource) exists.
	PropertyCreatedFor = lc.NamespcAuxiliary + "/csi-created-for"

	// PropertyAllowTwoPrimaries is DRBD option to allow second primary. Mainly used for live-migration.
	PropertyAllowTwoPrimaries = lc.NamespcDrbdNetOptions + "/allow-two-primaries"

	// PropertyDrbdNetProtocol is the DRBD network replication protocol option
	// (A = async, B = semi-sync, C = sync). DRBD requires Protocol C whenever
	// allow-two-primaries is enabled, otherwise drbdadm adjust fails with
	// "Protocol C required" (errno 139).
	PropertyDrbdNetProtocol = lc.NamespcDrbdNetOptions + "/protocol"

	// PropertyCsiProtocolOverride marks a resource-connection where this driver
	// has installed a temporary Protocol=C override to make a Protocol-A/B
	// resource compatible with allow-two-primaries during live migration. The
	// marker lets us safely remove only the overrides we set, without touching
	// connection-level overrides installed by the operator.
	PropertyCsiProtocolOverride = lc.NamespcAuxiliary + "/csi-protocol-override"

	// CreatedForTemporaryDisklessAttach marks a resource as temporary, i.e. it should be removed after it is no longer
	// needed.
	CreatedForTemporaryDisklessAttach = "temporary-diskless-attach"

	// ParameterNamespace is the preferred namespace when setting parameters in
	ParameterNamespace = DriverName

	// SnapshotParameterNamespace is the namespace when setting snapshot parameters in storage and snapshot classes.
	SnapshotParameterNamespace = "snap." + DriverName

	// PropertyNamespace is the namespace for LINSTOR properties in kubernetes storage class parameters.
	PropertyNamespace = "property." + DriverName

	// ResourceGroupNamespace is the UUID namespace for generated resource groups
	ResourceGroupNamespace = "resourcegroup." + DriverName
)
