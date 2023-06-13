package volume

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
)

//go:generate go run github.com/alvaroloes/enumer@v1.1.2 -type=SnapshotType -trimprefix=SnapshotType

type SnapshotType int

const (
	SnapshotTypeInCluster SnapshotType = iota
	SnapshotTypeS3
	SnapshotTypeLinstor
)

type SnapshotParameters struct {
	Type             SnapshotType `json:"type,omitempty"`
	AllowIncremental bool         `json:"allow-incremental"`
	RemoteName       string       `json:"remote-name,omitempty"`
	DeleteLocal      bool         `json:"delete-local,omitempty"`
	S3Endpoint       string       `json:"s3-endpoint,omitempty"`
	S3Bucket         string       `json:"s3-bucket,omitempty"`
	S3SigningRegion  string       `json:"s3-signing-region,omitempty"`
	S3UsePathStyle   bool         `json:"s3-use-path-style"`
	S3AccessKey      string       `json:"-"`
	S3SecretKey      string       `json:"-"`
}

func NewSnapshotParameters(params, secrets map[string]string) (*SnapshotParameters, error) {
	p := &SnapshotParameters{}

	for k, v := range params {
		if !strings.HasPrefix(k, linstor.SnapshotParameterNamespace) {
			log.WithField("key", k).Debug("skipping parameter without snapshot parameter prefix")
			continue
		}

		k = k[len(linstor.SnapshotParameterNamespace):]

		switch k {
		case "/type":
			t, err := SnapshotTypeString(v)
			if err != nil {
				return nil, err
			}

			p.Type = t
		case "/delete-local":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}

			p.DeleteLocal = b
		case "/allow-incremental":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}

			p.AllowIncremental = b
		case "/remote-name":
			p.RemoteName = v
		case "/s3-endpoint":
			p.S3Endpoint = v
		case "/s3-bucket":
			p.S3Bucket = v
		case "/s3-signing-region":
			p.S3SigningRegion = v
		case "/s3-use-path-style":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}

			p.S3UsePathStyle = b
		default:
			log.WithField("key", k).Warn("ignoring unknown snapshot parameter key")
		}
	}

	if p.Type != SnapshotTypeInCluster && p.RemoteName == "" {
		return nil, fmt.Errorf("snapshots of type `%s` require specifying a %s/remote-name", p.Type, linstor.SnapshotParameterNamespace)
	}

	if p.Type == SnapshotTypeInCluster && p.DeleteLocal {
		return nil, fmt.Errorf("cannot use %s/delete-local with in-cluster snapshots", linstor.SnapshotParameterNamespace)
	}

	p.S3AccessKey = secrets["access-key"]
	p.S3SecretKey = secrets["secret-key"]

	return p, nil
}

func (s *SnapshotParameters) String() string {
	// NB: we use a value here instead of a pointer, so we don't recurse endlessly.
	return fmt.Sprint(SnapshotParameters{
		Type:             s.Type,
		AllowIncremental: s.AllowIncremental,
		RemoteName:       s.RemoteName,
		S3Endpoint:       s.S3Endpoint,
		S3Bucket:         s.S3Bucket,
		S3SigningRegion:  s.S3SigningRegion,
		S3UsePathStyle:   s.S3UsePathStyle,
		S3AccessKey:      "***",
		S3SecretKey:      "***",
	})
}
