package volume_test

import (
	"testing"

	lc "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"
	"github.com/stretchr/testify/assert"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func TestNewParameters(t *testing.T) {
	empty, err := volume.NewParameters(nil, lc.NamespcAuxiliary)
	assert.NoError(t, err)
	assert.NotEmpty(t, empty.ResourceGroup)

	fixed, err := volume.NewParameters(map[string]string{
		"resourcegroup": "rg1",
	}, lc.NamespcAuxiliary)
	assert.NoError(t, err)
	assert.Equal(t, "rg1", fixed.ResourceGroup)

	fixedWithNamespace, err := volume.NewParameters(map[string]string{
		linstor.ParameterNamespace + "/resourcegroup": "rg1",
	}, lc.NamespcAuxiliary)
	assert.NoError(t, err)
	assert.Equal(t, "rg1", fixedWithNamespace.ResourceGroup)

	expected := map[string]string{
		"DrbdOptions/auto-quorum":  "suspend-io",
		"DrbdOptions/Net/protocol": "C",
	}
	legacy, err := volume.NewParameters(expected, lc.NamespcAuxiliary)
	assert.NoError(t, err)
	assert.Equal(t, expected, legacy.Properties)

	generalProps, err := volume.NewParameters(map[string]string{
		linstor.PropertyNamespace + "/DrbdOptions/auto-quorum":  "suspend-io",
		linstor.PropertyNamespace + "/DrbdOptions/Net/protocol": "C",
	}, lc.NamespcAuxiliary)
	assert.NoError(t, err)
	assert.Equal(t, expected, generalProps.Properties)
}

func TestDisklessFlag(t *testing.T) {
	testcases := []struct {
		name     string
		params   volume.Parameters
		expected string
		isError  bool
	}{
		{
			name:     "default-layers",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Storage}},
			expected: lc.FlagDrbdDiskless,
			isError:  false,
		},
		{
			name:     "nvme-layers",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Nvme}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "both-diskless",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Nvme}},
			expected: lc.FlagDrbdDiskless,
			isError:  false,
		},
		{
			name:     "both-reversed",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Nvme, devicelayerkind.Drbd}},
			expected: lc.FlagNvmeInitiator,
			isError:  false,
		},
		{
			name:     "no-diskless-layer",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Cache, devicelayerkind.Storage}},
			expected: "",
			isError:  true,
		},
	}

	t.Parallel()
	for _, tcase := range testcases {
		tcase := tcase
		t.Run(tcase.name, func(t *testing.T) {
			actual, err := tcase.params.DisklessFlag()
			assert.Equal(t, tcase.expected, actual)
			if tcase.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseVolumeId(t *testing.T) {
	testcases := []struct {
		name     string
		id       string
		expected volume.ID
		isError  bool
	}{
		{
			name:     "resource-only",
			id:       "pvc-1234",
			expected: volume.ID{ResourceName: "pvc-1234", VolumeNumber: 0},
		},
		{
			name:     "explicit-volume-zero",
			id:       "pvc-1234/0",
			expected: volume.ID{ResourceName: "pvc-1234", VolumeNumber: 0},
		},
		{
			name:     "explicit-volume-number",
			id:       "pvc-1234/1",
			expected: volume.ID{ResourceName: "pvc-1234", VolumeNumber: 1},
		},
		{
			name:     "multi-digit-volume-number",
			id:       "pvc-1234/42",
			expected: volume.ID{ResourceName: "pvc-1234", VolumeNumber: 42},
		},
		{
			name:     "max-volume-number",
			id:       "pvc-1234/65535",
			expected: volume.ID{ResourceName: "pvc-1234", VolumeNumber: 65535},
		},
		{
			name:    "empty-string",
			id:      "",
			isError: true,
		},
		{
			name:    "missing-resource-name",
			id:      "/5",
			isError: true,
		},
		{
			name:    "only-separator",
			id:      "/",
			isError: true,
		},
		{
			name:    "negative-volume-number",
			id:      "pvc-1234/-1",
			isError: true,
		},
		{
			name:    "non-numeric-volume-number",
			id:      "pvc-1234/abc",
			isError: true,
		},
		{
			name:    "missing-volume-number",
			id:      "pvc-1234/",
			isError: true,
		},
		{
			name:    "extra-path-component",
			id:      "pvc-1234/1/2",
			isError: true,
		},
		{
			name:    "volume-number-too-large",
			id:      "pvc-1234/65536",
			isError: true,
		},
		{
			name:    "volume-number-overflows-int",
			id:      "pvc-1234/99999999999999999999",
			isError: true,
		},
	}

	t.Parallel()

	for i := range testcases {
		tcase := testcases[i]
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			actual, err := volume.ParseVolumeId(tcase.id)
			if tcase.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.expected, actual)
			}
		})
	}
}

func TestID_String(t *testing.T) {
	testcases := []struct {
		name     string
		id       volume.ID
		expected string
	}{
		{
			name:     "volume-zero-omits-number",
			id:       volume.ID{ResourceName: "pvc-1234", VolumeNumber: 0},
			expected: "pvc-1234",
		},
		{
			name:     "non-zero-volume-number",
			id:       volume.ID{ResourceName: "pvc-1234", VolumeNumber: 1},
			expected: "pvc-1234/1",
		},
		{
			name:     "multi-digit-volume-number",
			id:       volume.ID{ResourceName: "pvc-1234", VolumeNumber: 42},
			expected: "pvc-1234/42",
		},
	}

	t.Parallel()

	for i := range testcases {
		tcase := testcases[i]
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tcase.expected, tcase.id.String())
		})
	}
}

func TestVolumeIdRoundTrip(t *testing.T) {
	ids := []volume.ID{
		{ResourceName: "pvc-1234", VolumeNumber: 0},
		{ResourceName: "pvc-1234", VolumeNumber: 1},
		{ResourceName: "pvc-1234", VolumeNumber: 42},
	}

	t.Parallel()

	for i := range ids {
		id := ids[i]
		t.Run(id.String(), func(t *testing.T) {
			t.Parallel()

			parsed, err := volume.ParseVolumeId(id.String())
			assert.NoError(t, err)
			assert.Equal(t, id, parsed)
		})
	}
}

func TestParseSnapshotId(t *testing.T) {
	testcases := []struct {
		name     string
		id       string
		expected *volume.SnapshotId
		isError  bool
	}{
		{
			name: "s3-backup",
			id:   "s3://remote-1/source-vol/snap-1",
			expected: &volume.SnapshotId{
				Type:         volume.SnapshotTypeS3,
				Remote:       "remote-1",
				SourceName:   "source-vol",
				SnapshotName: "snap-1",
			},
		},
		{
			name: "linstor-l2l-backup",
			id:   "linstor://remote-2/source-vol/snap-2",
			expected: &volume.SnapshotId{
				Type:         volume.SnapshotTypeLinstor,
				Remote:       "remote-2",
				SourceName:   "source-vol",
				SnapshotName: "snap-2",
			},
		},
		{
			name: "in-cluster-without-remote",
			id:   "incluster:///source-vol/snap-3",
			expected: &volume.SnapshotId{
				Type:         volume.SnapshotTypeInCluster,
				Remote:       "",
				SourceName:   "source-vol",
				SnapshotName: "snap-3",
			},
		},
		{
			name: "scheme-is-case-insensitive",
			id:   "S3://remote-1/source-vol/snap-1",
			expected: &volume.SnapshotId{
				Type:         volume.SnapshotTypeS3,
				Remote:       "remote-1",
				SourceName:   "source-vol",
				SnapshotName: "snap-1",
			},
		},
		{
			name: "explicit-unknown-scheme",
			id:   "unknown://remote-1/source-vol/snap-1",
			expected: &volume.SnapshotId{
				Type:         volume.SnapshotTypeUnknown,
				Remote:       "remote-1",
				SourceName:   "source-vol",
				SnapshotName: "snap-1",
			},
		},
		{
			name: "legacy-snapshot-without-scheme",
			id:   "snap-legacy",
			expected: &volume.SnapshotId{
				Type:         volume.SnapshotTypeUnknown,
				SnapshotName: "snap-legacy",
			},
		},
		{
			name:    "unrecognized-scheme",
			id:      "ftp://remote-1/source-vol/snap-1",
			isError: true,
		},
		{
			name:    "too-few-path-components",
			id:      "s3://remote-1/only-one",
			isError: true,
		},
		{
			name:    "too-many-path-components",
			id:      "s3://remote-1/source-vol/snap-1/extra",
			isError: true,
		},
		{
			name:    "missing-path",
			id:      "s3://remote-1",
			isError: true,
		},
		{
			name:    "malformed-url",
			id:      "s3://remote-1:notaport/source-vol/snap-1",
			isError: true,
		},
	}

	t.Parallel()

	for i := range testcases {
		tcase := testcases[i]
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			actual, err := volume.ParseSnapshotId(tcase.id)
			if tcase.isError {
				assert.Error(t, err)
				assert.Nil(t, actual)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.expected, actual)
			}
		})
	}
}

func TestSnapshotId_String(t *testing.T) {
	testcases := []struct {
		name     string
		snap     volume.SnapshotId
		expected string
	}{
		{
			name:     "s3-backup",
			snap:     volume.SnapshotId{Type: volume.SnapshotTypeS3, Remote: "remote-1", SourceName: "source-vol", SnapshotName: "snap-1"},
			expected: "S3://remote-1/source-vol/snap-1",
		},
		{
			name:     "linstor-l2l-backup",
			snap:     volume.SnapshotId{Type: volume.SnapshotTypeLinstor, Remote: "remote-2", SourceName: "source-vol", SnapshotName: "snap-2"},
			expected: "Linstor://remote-2/source-vol/snap-2",
		},
		{
			name:     "in-cluster",
			snap:     volume.SnapshotId{Type: volume.SnapshotTypeInCluster, SourceName: "source-vol", SnapshotName: "snap-3"},
			expected: "InCluster:///source-vol/snap-3",
		},
	}

	t.Parallel()

	for i := range testcases {
		tcase := testcases[i]
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tcase.expected, tcase.snap.String())
		})
	}
}

func TestSnapshotIdRoundTrip(t *testing.T) {
	// Legacy snapshots (type Unknown, only a snapshot name) are intentionally
	// excluded: they are a read-only compatibility path and do not round-trip
	// through String().
	ids := []volume.SnapshotId{
		{Type: volume.SnapshotTypeS3, Remote: "remote-1", SourceName: "source-vol", SnapshotName: "snap-1"},
		{Type: volume.SnapshotTypeLinstor, Remote: "remote-2", SourceName: "source-vol", SnapshotName: "snap-2"},
		{Type: volume.SnapshotTypeInCluster, SourceName: "source-vol", SnapshotName: "snap-3"},
	}

	t.Parallel()

	for i := range ids {
		id := ids[i]
		t.Run(id.String(), func(t *testing.T) {
			t.Parallel()

			parsed, err := volume.ParseSnapshotId(id.String())
			assert.NoError(t, err)
			assert.Equal(t, &id, parsed)
		})
	}
}

func TestParameters_ToResourceGroupModify(t *testing.T) {
	testcases := []struct {
		name            string
		params          volume.Parameters
		existing        lapi.ResourceGroup
		expectedModify  lapi.ResourceGroupModify
		expectedChanged bool
		expectedError   bool
	}{
		{
			name:           "matching-rg-is-empty-modify",
			params:         volume.Parameters{Properties: map[string]string{"DrbdOptions/Net/Protocol": "C"}, LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Storage}, PlacementCount: 2, ResourceGroup: "matching-rg-is-empty-modify"},
			existing:       lapi.ResourceGroup{Name: "matching-rg-is-empty-modify", Props: map[string]string{lc.KeyStorPoolName: "", "DrbdOptions/Net/Protocol": "C"}, SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(devicelayerkind.Drbd), string(devicelayerkind.Storage)}, PlaceCount: 2}},
			expectedModify: lapi.ResourceGroupModify{OverrideProps: map[string]string{}},
		},
		{
			name:     "wrong-select-filters",
			params:   volume.Parameters{LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Writecache, devicelayerkind.Drbd, devicelayerkind.Storage}, PlacementCount: 3, ResourceGroup: "wrong-select-filters", StoragePools: []string{"pool1", "pool2"}},
			existing: lapi.ResourceGroup{Name: "wrong-select-filters", SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(devicelayerkind.Drbd)}, PlaceCount: 2}},
			expectedModify: lapi.ResourceGroupModify{
				OverrideProps: make(map[string]string),
				SelectFilter: lapi.AutoSelectFilter{
					LayerStack:      []string{string(devicelayerkind.Writecache), string(devicelayerkind.Drbd), string(devicelayerkind.Storage)},
					PlaceCount:      3,
					StoragePoolList: []string{"pool1", "pool2"},
				},
			},
			expectedChanged: true,
		},
		{
			name:          "differing-props-are-errors",
			params:        volume.Parameters{Properties: map[string]string{"DrbdOptions/Net/Protocol": "C"}, LayerList: []devicelayerkind.DeviceLayerKind{devicelayerkind.Drbd, devicelayerkind.Storage}, PlacementCount: 2, ResourceGroup: "differing-props-are-errors"},
			existing:      lapi.ResourceGroup{Name: "differing-props-are-errors", Props: map[string]string{lc.KeyStorPoolName: "", "DrbdOptions/Net/Protocol": "A", "DrbdOptions/Foo/Bar": "baz"}, SelectFilter: lapi.AutoSelectFilter{LayerStack: []string{string(devicelayerkind.Drbd), string(devicelayerkind.Storage)}, PlaceCount: 2}},
			expectedError: true,
		},
	}

	t.Parallel()
	for i := range testcases {
		tcase := testcases[i]
		t.Run(tcase.name, func(t *testing.T) {
			actualModified, actualChanged, err := tcase.params.ToResourceGroupModify(&tcase.existing)
			assert.Equal(t, tcase.expectedChanged, actualChanged)
			assert.Equal(t, tcase.expectedModify, actualModified)
			assert.Equal(t, tcase.expectedError, err != nil)
		})
	}
}
