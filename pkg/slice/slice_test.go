package slice_test

import (
	"testing"

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
