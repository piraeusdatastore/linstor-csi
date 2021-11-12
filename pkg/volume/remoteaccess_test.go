package volume_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func TestRemoteAccessPolicy(t *testing.T) {
	t.Parallel()

	exampleSegments := map[string]string{
		"linbit.com/hostname":           "host-1",
		"linbit.com/sp-storage-pool":    "true",
		"topology.kubernetes.io/region": "region-1",
		"topology.kubernetes.io/zone":   "region-1-zone-1",
	}

	cases := []struct {
		name             string
		policy           volume.RemoteAccessPolicy
		segments         map[string]string
		expectedSegments []map[string]string
	}{
		{
			name:     "access-everywhere",
			policy:   volume.RemoteAccessPolicyAnywhere,
			segments: exampleSegments,
			expectedSegments: []map[string]string{
				{},
			},
		},
		{
			name:     "access-local",
			policy:   volume.RemoteAccessPolicyLocalOnly,
			segments: exampleSegments,
			expectedSegments: []map[string]string{
				{"linbit.com/hostname": "host-1"},
			},
		},
		{
			name:             "access-unknown-label",
			policy:           volume.RemoteAccessPolicy{{FromSame: []string{"some-new-label"}}},
			segments:         exampleSegments,
			expectedSegments: []map[string]string{{}},
		},
		{
			name:     "access-multi-rule",
			policy:   volume.RemoteAccessPolicy{{FromSame: []string{"topology.kubernetes.io/region"}}, {FromSame: []string{"topology.kubernetes.io/zone"}}},
			segments: exampleSegments,
			expectedSegments: []map[string]string{
				{"topology.kubernetes.io/region": "region-1"},
				{"topology.kubernetes.io/zone": "region-1-zone-1"},
			},
		},
	}

	for i := range cases {
		tcase := &cases[i]

		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			actual := tcase.policy.AccessibleSegments(tcase.segments)
			assert.Equal(t, tcase.expectedSegments, actual)
		})
	}
}

func TestRemoteAccessPolicyUnmarshal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		raw      string
		expected volume.RemoteAccessPolicy
	}{
		{
			name:     "legacy-true",
			raw:      "true",
			expected: volume.RemoteAccessPolicyAnywhere,
		},
		{
			name:     "legacy-false",
			raw:      "false",
			expected: volume.RemoteAccessPolicyLocalOnly,
		},
		{
			name: "parse-simple",
			raw: `
- fromSame:
    - topology.kubernetes.io/zone
    - topology.kubernetes.io/region
`,
			expected: volume.RemoteAccessPolicy{{FromSame: []string{"topology.kubernetes.io/zone", "topology.kubernetes.io/region"}}},
		},
		{
			name: "parse-multiple-rules",
			raw: `
- fromSame:
    - topology.kubernetes.io/zone
    - topology.kubernetes.io/region
- fromSame:
    - some-other-zone-label
`,
			expected: volume.RemoteAccessPolicy{{FromSame: []string{"topology.kubernetes.io/zone", "topology.kubernetes.io/region"}}, {FromSame: []string{"some-other-zone-label"}}},
		},
	}

	for i := range cases {
		tcase := &cases[i]

		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			var actual volume.RemoteAccessPolicy
			err := actual.UnmarshalText([]byte(tcase.raw))
			assert.NoError(t, err)
			assert.Equal(t, tcase.expected, actual)
		})
	}
}

func TestRemoteAccessPolicyMarshal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		original volume.RemoteAccessPolicy
		expected string
	}{
		{
			name:     "legacy-true",
			original: volume.RemoteAccessPolicyAnywhere,
			expected: "true",
		},
		{
			name:     "legacy-false",
			original: volume.RemoteAccessPolicyLocalOnly,
			expected: "false",
		},
		{
			name:     "marshal-simple",
			original: volume.RemoteAccessPolicy{{FromSame: []string{"topology.kubernetes.io/zone", "topology.kubernetes.io/region"}}},
			expected: `- fromSame:
    - topology.kubernetes.io/zone
    - topology.kubernetes.io/region
`,
		},
		{
			name:     "marshal-multiple-rules",
			original: volume.RemoteAccessPolicy{{FromSame: []string{"topology.kubernetes.io/zone", "topology.kubernetes.io/region"}}, {FromSame: []string{"some-other-zone-label"}}},
			expected: `- fromSame:
    - topology.kubernetes.io/zone
    - topology.kubernetes.io/region
- fromSame:
    - some-other-zone-label
`,
		},
	}

	for i := range cases {
		tcase := &cases[i]

		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			actual, err := tcase.original.MarshalText()
			assert.NoError(t, err)
			assert.Equal(t, tcase.expected, string(actual))
		})
	}
}

func TestPrunePattern(t *testing.T) {
	t.Parallel()

	cases := []struct {
		inp      []map[string]string
		expected []map[string]string
	}{
		{
			// nil -> nil
		},
		{
			inp:      []map[string]string{{"a": "1"}, {"a": "1"}},
			expected: []map[string]string{{"a": "1"}},
		},
		{
			inp:      []map[string]string{{"a": "1"}, {"a": "2"}},
			expected: []map[string]string{{"a": "1"}, {"a": "2"}},
		},
		{
			inp:      []map[string]string{{"a": "1", "b": "1"}, {"a": "1"}},
			expected: []map[string]string{{"a": "1"}},
		},
		{
			inp:      []map[string]string{{"a": "1", "b": "1"}, {"a": "1", "b": "2"}},
			expected: []map[string]string{{"a": "1", "b": "1"}, {"a": "1", "b": "2"}},
		},
		{
			inp:      []map[string]string{{"a": "1"}, {"a": "1", "b": "1"}, {"a": "1", "b": "2"}},
			expected: []map[string]string{{"a": "1"}},
		},
	}

	for i := range cases {
		tcase := &cases[i]

		t.Run(fmt.Sprintf("%+v", tcase.inp), func(t *testing.T) {
			actual := volume.PrunePattern(tcase.inp...)
			assert.Equal(t, tcase.expected, actual)
		})
	}
}
