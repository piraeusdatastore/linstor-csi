package volume_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func TestNewSnapshotParameters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		rawParameters map[string]string
		rawSecrets    map[string]string
		expected      *volume.SnapshotParameters
		expectedErr   string
	}{
		{
			name:     "empty",
			expected: &volume.SnapshotParameters{Type: volume.SnapshotTypeInCluster},
		},
		{
			name: "s3-with-name",
			rawParameters: map[string]string{
				linstor.SnapshotParameterNamespace + "/type":              "S3",
				linstor.SnapshotParameterNamespace + "/allow-incremental": "true",
				linstor.SnapshotParameterNamespace + "/remote-name":       "my-remote",
				linstor.SnapshotParameterNamespace + "/s3-bucket":         "my-bucket",
				linstor.SnapshotParameterNamespace + "/s3-endpoint":       "s3.us-west-1.amazonaws.com",
				linstor.SnapshotParameterNamespace + "/s3-signing-region": "us-west-1",
				linstor.SnapshotParameterNamespace + "/s3-use-path-style": "true",
			},
			rawSecrets: map[string]string{
				"access-key": "admin",
				"secret-key": "password",
			},
			expected: &volume.SnapshotParameters{
				Type:             volume.SnapshotTypeS3,
				RemoteName:       "my-remote",
				AllowIncremental: true,
				S3Endpoint:       "s3.us-west-1.amazonaws.com",
				S3SigningRegion:  "us-west-1",
				S3Bucket:         "my-bucket",
				S3AccessKey:      "admin",
				S3SecretKey:      "password",
				S3UsePathStyle:   true,
			},
		},
		{
			name: "s3-without-name",
			rawParameters: map[string]string{
				linstor.SnapshotParameterNamespace + "/type":              "S3",
				linstor.SnapshotParameterNamespace + "/s3-bucket":         "my-bucket",
				linstor.SnapshotParameterNamespace + "/s3-endpoint":       "s3.us-west-1.amazonaws.com",
				linstor.SnapshotParameterNamespace + "/s3-signing-region": "us-west-1",
			},
			rawSecrets: map[string]string{
				"access-key": "admin",
				"secret-key": "password",
			},
			expectedErr: fmt.Sprintf("snapshots of type `S3` require specifying a %s/remote-name", linstor.SnapshotParameterNamespace),
		},
	}

	for i := range cases {
		tcase := &cases[i]
		t.Run(tcase.name, func(t *testing.T) {
			actual, err := volume.NewSnapshotParameters(tcase.rawParameters, tcase.rawSecrets)

			if tcase.expected != nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tcase.expectedErr)
			}

			assert.Equal(t, tcase.expected, actual)
		})
	}
}
