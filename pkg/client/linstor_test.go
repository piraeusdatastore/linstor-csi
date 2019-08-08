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

package client

import (
	"reflect"
	"testing"
)

func TestAllocationSizeKiB(t *testing.T) {

	l := &Linstor{}
	var tableTests = []struct {
		req int64
		lim int64
		out int64
	}{
		{1024, 0, 4},
		{4096, 4096, 4},
		{4097, 0, 5},
	}

	for _, tt := range tableTests {
		actual, _ := l.AllocationSizeKiB(tt.req, tt.lim)
		if tt.out != actual {
			t.Errorf("Expected: %d, Got: %d, from %+v", tt.out, actual, tt)
		}
	}

	// We'd have to allocate more bytes than the limit since we allocate at KiB
	// Increments.
	_, err := l.AllocationSizeKiB(4097, 40)
	if err == nil {
		t.Errorf("Expected limitBytes to be respected!")
	}
	_, err = l.AllocationSizeKiB(4097, 4096)
	if err == nil {
		t.Errorf("Expected limitBytes to be respected!")
	}
}

func TestValidResourceName(t *testing.T) {
	all := "all"
	if err := validResourceName(all); err == nil {
		t.Fatalf("Expected '%s' to be be an invalid keyword", all)
	}

	tooLong := "abcdefghijklmnopqrstuvwyzABCDEFGHIJKLMNOPQRSTUVWXYZ_______" // 49
	if err := validResourceName(tooLong); err == nil {
		t.Fatalf("Expected '%s' to be too long", tooLong)
	}

	utf8rune := "hello🐱kitty"
	if err := validResourceName(utf8rune); err == nil {
		t.Fatalf("Expected '%s' to fail, because of an utf rune", utf8rune)
	}

	invalid := "_-"
	if err := validResourceName(invalid); err == nil {
		t.Fatalf("Expected '%s' to fail, because it is an invalid name", invalid)
	}

	valid := "rck23"
	if err := validResourceName(valid); err != nil {
		t.Fatalf("Expected '%s' to be valid", valid)
	}
}

func TestLinstorifyResourceName(t *testing.T) {
	var unitTests = []struct {
		in, out string
		errExp  bool
	}{
		{
			in:     "rck23",
			out:    "rck23",
			errExp: false,
		}, {
			in:     "hello🐱kitty",
			out:    "hello_kitty",
			errExp: false,
		}, {
			in:     "1be00fd3-d435-436f-be20-561418c62762",
			out:    "LS_1be00fd3-d435-436f-be20-561418c62762",
			errExp: false,
		}, {
			in:     "b1e00fd3-d435-436f-be20-561418c62762",
			out:    "b1e00fd3-d435-436f-be20-561418c62762",
			errExp: false,
		}, {
			in:     "abcdefghijklmnopqrstuvwyzABCDEFGHIJKLMNOPQRSTUVWXYZ_______", // 49
			out:    "should fail",
			errExp: true,
		},
	}

	for _, test := range unitTests {
		resName, err := linstorifyResourceName(test.in)
		switch {
		case test.errExp && err == nil:
			t.Fatalf("Expected that rest '%s' returns an error\n", test.in)
		case !test.errExp && err != nil:
			t.Fatalf("Expected that rest '%s' does not return an error\n", test.in)
		case test.errExp && err != nil:
			continue
		}

		if resName != test.out {
			t.Fatalf("Expected that input '%s' transforms to '%s', but got '%s'\n", test.in, test.out, resName)
		}
	}

}

func TestMkfsArgs(t *testing.T) {
	var tableTests = []struct {
		opts, source string
		expected     []string
	}{
		{"-K", "/dev/path", []string{"-K", "/dev/path"}},
		{"", "/dev/path", []string{"/dev/path"}},
	}

	for _, tt := range tableTests {
		actual := mkfsArgs(tt.opts, tt.source)
		if !reflect.DeepEqual(tt.expected, actual) {
			t.Errorf("Expected that mkfsArgs(%q, %q) results in\n\t%v\nbut got\n\t%v\n",
				tt.opts, tt.source, tt.expected, actual)
		}
	}
}
