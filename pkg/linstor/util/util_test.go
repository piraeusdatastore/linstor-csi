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

package util

import (
	"testing"

	apiconst "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
)

func TestContainsAll(t *testing.T) {
	tableTests := []struct {
		data     []string
		members  []string
		expected bool
	}{
		{
			data:     []string{"rck23"},
			members:  []string{"rck23"},
			expected: true,
		},
		{
			data:     []string{"rck23"},
			members:  []string{"rck23", "test", "bleh"},
			expected: false,
		},
		{
			data:     []string{"rck23", "test", "bleh"},
			members:  []string{"rck24", "quizz", "meh"},
			expected: false,
		},
		{
			data:     []string{"rck23"},
			members:  []string{},
			expected: false,
		},
		{
			data:     []string{"rck23", "test", "bleh"},
			members:  []string{"bleh"},
			expected: true,
		},
		{
			data:     []string{"rck23", "test", "bleh"},
			members:  []string{"rck23", "test", "bleh"},
			expected: true,
		},
	}

	for _, tt := range tableTests {
		actual := containsAll(tt.data, tt.members...)

		if actual != tt.expected {
			t.Fatalf("Expected that containsAll('%+v', '%v') results in\n\t%v\nbut got\n\t%v\n",
				tt.data, tt.members, tt.expected, actual)
		}
	}
}

func TestContainsAny(t *testing.T) {
	tableTests := []struct {
		data     []string
		members  []string
		expected bool
	}{
		{
			data:     []string{"rck23"},
			members:  []string{"rck23"},
			expected: true,
		},
		{
			data:     []string{"rck23"},
			members:  []string{"rck23", "test", "bleh"},
			expected: true,
		},
		{
			data:     []string{"rck23", "test", "bleh"},
			members:  []string{"rck24", "quizz", "meh"},
			expected: false,
		},
		{
			data:     []string{},
			members:  []string{},
			expected: false,
		},
		{
			data:     []string{},
			members:  []string{"test"},
			expected: false,
		},
		{
			data:     []string{"rck23"},
			members:  []string{},
			expected: false,
		},
		{
			data:     []string{"rck23", "test", "bleh"},
			members:  []string{"bleh"},
			expected: true,
		},
		{
			data:     []string{"rck23", "test", "bleh"},
			members:  []string{"rck23", "test", "bleh"},
			expected: true,
		},
	}

	for _, tt := range tableTests {
		actual := containsAny(tt.data, tt.members...)

		if actual != tt.expected {
			t.Fatalf("Expected that containsAny('%+v', '%v') results in\n\t%v\nbut got\n\t%v\n",
				tt.data, tt.members, tt.expected, actual)
		}
	}
}

func TestDeployedDiskfully(t *testing.T) {
	tableTests := []struct {
		res      lapi.Resource
		expected bool
	}{
		{
			res:      lapi.Resource{},
			expected: false,
		},
		{
			res: lapi.Resource{
				Name:     "foo0",
				NodeName: "bar",
			},
			expected: true,
		},
		{
			res: lapi.Resource{
				Name:     "foo2",
				NodeName: "bar",
				Flags:    []string{apiconst.FlagDiskless},
			},
			expected: false,
		},
		{
			res: lapi.Resource{
				Name:     "foo2",
				NodeName: "bar",
				Flags:    []string{apiconst.FlagDiskless},
			},
			expected: false,
		},
	}

	for _, tt := range tableTests {
		actual := DeployedDiskfully(tt.res)

		if tt.expected != actual {
			t.Fatalf("Expected that DeployedDiskfully('%+v') results in\n\t%v\nbut got\n\t%v", tt.res, tt.expected, actual)
		}
	}
}

func TestDeployedDisklessly(t *testing.T) {
	tableTests := []struct {
		res      lapi.Resource
		expected bool
	}{
		{
			res:      lapi.Resource{},
			expected: false,
		},
		{
			res: lapi.Resource{
				Name:     "foo0",
				NodeName: "bar",
			},
			expected: false, // No volumes.
		},
		{
			res: lapi.Resource{
				Name:     "foo1",
				NodeName: "bar",
			},
			expected: false,
		},
		{
			res: lapi.Resource{
				Name:     "foo2",
				NodeName: "bar",
				Flags:    []string{apiconst.FlagDiskless},
			},
			expected: true,
		},
		{
			res: lapi.Resource{
				Name:     "foo3",
				NodeName: "bar",
				Flags:    []string{apiconst.FlagDiskless, apiconst.FlagDelete},
			},
			expected: false,
		},
	}

	for _, tt := range tableTests {
		actual := DeployedDisklessly(tt.res)

		if tt.expected != actual {
			t.Fatalf("Expected that DeployedDisklessly('%+v') results in\n\t%v\nbut got\n\t%v", tt.res, tt.expected, actual)
		}
	}
}
