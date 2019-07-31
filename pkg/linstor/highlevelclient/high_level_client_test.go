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

package highlevelclient

import (
	"reflect"
	"testing"
)

func TestUniq(t *testing.T) {
	var tableTests = []struct {
		data     []string
		expected []string
	}{
		{
			data:     []string{"rck23"},
			expected: []string{"rck23"},
		},
		{
			data:     []string{"rck23", "rck23"},
			expected: []string{"rck23"},
		},
		{
			data:     []string{"foo", "bar", ""},
			expected: []string{"foo", "bar", ""},
		},
		{
			data:     []string{},
			expected: []string{},
		},
	}

	for _, tt := range tableTests {
		actual := uniq(tt.data)

		if !reflect.DeepEqual(tt.expected, actual) {
			t.Fatalf("Expected that uniq('%+v') results in\n\t%v\nbut got\n\t%v", tt.data, tt.expected, actual)
		}
	}
}
