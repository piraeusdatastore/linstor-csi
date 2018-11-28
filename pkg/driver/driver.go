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

package driver

import (
	"fmt"
)

// Driver fullfils CSI controller, node, and indentity server interfaces.
type Driver struct {
	endpoint string
}

// NewDriver build up a driver.
func NewDriver(endpoint string) (*Driver, error) {
	return &Driver{endpoint: endpoint}, nil
}

// Run the server, listening for API calls.
func (d Driver) Run() error {
	return fmt.Errorf("Not implemented")
}
