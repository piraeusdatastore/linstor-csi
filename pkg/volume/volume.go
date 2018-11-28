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

package volume

// Info provides the everything need to manipulate volumes.
type Info struct {
	Name       string
	ID         string
	CreatedBy  string
	SizeBytes  int64
	Readonly   bool
	Parameters map[string]string
}

// CreateDeleter handles the creation and deletion of volumes.
type CreateDeleter interface {
	Querier
	Create(vol *Info) error
	Delete(vol *Info) error
}

type AttacherDettacher interface {
	Querier
	Attach(vol *Info, node string) error
	Detech(vol *Info, node string) error
	NodeAvailable(node string) (bool, error)
}

// Querier retrives various states of volumes.
type Querier interface {
	ListAll(parameters map[string]string) ([]*Info, error)
	GetByName(name string) (*Info, error)

	//GetByID should return nil when volume is not found.
	GetByID(ID string) (*Info, error)
}
