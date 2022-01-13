package slice_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/piraeusdatastore/linstor-csi/pkg/slice"
)

func TestContainsString(t *testing.T) {
	src := []string{"aa", "bb", "cc"}
	if !slice.ContainsString(src, "bb") {
		t.Errorf("ContainsString didn't find the string as expected")
	}

	if slice.ContainsString(src, "a") {
		t.Errorf("ContainsString found the string, which wasn't expected")
	}
}

func TestAppendUnique(t *testing.T) {
	var actual []string
	actual = slice.AppendUnique(actual, "1", "2")
	assert.Equal(t, []string{"1", "2"}, actual)

	actual = slice.AppendUnique(actual, "3", "1", "3")
	assert.Equal(t, []string{"1", "2", "3"}, actual)
}
