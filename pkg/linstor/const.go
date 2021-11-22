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
	// LegacyParameterPassKey is the Aux props key in linstor where serialized CSI parameters
	// are stored.
	LegacyParameterPassKey = lc.NamespcAuxiliary + "/csi-volume-annotations"

	// PropertyProvisioningCompletedBy is the Aux props key in LINSTOR intentifying this resource as
	// fully provisioned by this plugin.
	PropertyProvisioningCompletedBy = lc.NamespcAuxiliary + "/csi-provisioning-completed-by"

	// PropertyCreatedFor is the Aux props key in linstor used to identify why a specific object (for example, a
	// resource) exists.
	PropertyCreatedFor = lc.NamespcAuxiliary + "/csi-created-for"

	// CreatedForTemporaryDisklessAttach marks a resource as temporary, i.e. it should be removed after it is no longer
	// needed.
	CreatedForTemporaryDisklessAttach = "temporary-diskless-attach"

	PublishedReadOnlyKey = lc.NamespcAuxiliary + "/csi-publish-readonly"

	// ParameterNamespace is the preferred namespace when setting parameters in
	ParameterNamespace = "linstor.csi.linbit.com"

	// SnapshotParameterNamespace is the namespace when setting snapshot parameters in storage and snapshot classes.
	SnapshotParameterNamespace = "snap.linstor.csi.linbit.com"

	// PropertyNamespace is the namespace for LINSTOR properties in kubernetes storage class parameters.
	PropertyNamespace = "property.linstor.csi.linbit.com"

	// ResourceGroupNamespace is the UUID namespace for generated resource groups
	ResourceGroupNamespace = "resourcegroup.linstor.csi.linbit.com"
)
