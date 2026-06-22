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

package volume

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	lc "github.com/LINBIT/golinstor"
	"github.com/container-storage-interface/spec/lib/go/csi"
)

// Info provides everything needed to manipulate volumes.
type Info struct {
	ID
	// The device sizes in bytes associated with this volume.
	// A valid Info always has at least a 0th volume.
	DeviceBytes   map[int]int64
	ResourceGroup string
	FsType        string
	Properties    map[string]string
	UseQuorum     bool
}

func (i *Info) Size() int64 {
	return i.DeviceBytes[i.VolumeNumber]
}

// Assignment represents a volume situated on a particular node.
type Assignment struct {
	// Node is the node that the assignment is valid for.
	Node string
	// Path is a location on the Node's filesystem where the volume may be accessed.
	Path string
}

// MaxVolumesPerResource is the maximum number of volumes a single resource can
// hold. DRBD (and therefore LINSTOR) uses a 16-bit volume number, so valid
// volume numbers fall in the range [0, MaxVolumesPerResource).
const MaxVolumesPerResource = 1 << 16

type ID struct {
	ResourceName string
	VolumeNumber int
}

func (i ID) String() string {
	if i.VolumeNumber == 0 {
		return i.ResourceName
	}

	return fmt.Sprintf("%s/%d", i.ResourceName, i.VolumeNumber)
}

func ParseVolumeId(id string) (ID, error) {
	if id == "" {
		return ID{}, fmt.Errorf("empty string is not a valid volume id")
	}

	volNr := 0

	parts := strings.SplitN(id, "/", 2)
	if parts[0] == "" {
		return ID{}, fmt.Errorf("volume id is missing a resource name: '%s'", id)
	}

	if len(parts) == 2 {
		v, err := strconv.Atoi(parts[1])
		if err != nil {
			return ID{}, fmt.Errorf("failed to parse volume id: %w", err)
		}

		if v < 0 || v >= MaxVolumesPerResource {
			return ID{}, fmt.Errorf("volume number out of range [0, %d): '%s'", MaxVolumesPerResource, id)
		}

		volNr = v
	}

	return ID{ResourceName: parts[0], VolumeNumber: volNr}, nil
}

type SnapshotId struct {
	Type         SnapshotType
	Remote       string
	SourceName   string
	VolumeNumber int
	SnapshotName string
}

// Source returns the volume the snapshot was taken from.
func (s SnapshotId) Source() ID {
	return ID{ResourceName: s.SourceName, VolumeNumber: s.VolumeNumber}
}

func (s SnapshotId) String() string {
	return (&url.URL{
		Scheme: s.Type.String(),
		Host:   s.Remote,
		Path:   fmt.Sprintf("/%s/%s", s.Source().String(), s.SnapshotName),
	}).String()
}

func ParseSnapshotId(id string) (*SnapshotId, error) {
	u, err := url.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshot id: %w", err)
	}

	if u.Scheme == "" {
		// Incomplete or "legacy" snapshots: they only contain the snapshot ID, nothing else.
		return &SnapshotId{
			Type:         SnapshotTypeUnknown,
			SnapshotName: id,
		}, nil
	}

	ty, err := SnapshotTypeString(u.Scheme)
	if err != nil {
		return nil, fmt.Errorf("unknown snapshot type: %w", err)
	}

	parts := strings.Split(u.Path, "/")

	var (
		source   ID
		snapName string
	)

	switch len(parts) {
	case 3:
		// "/<source>/<snapshot>": the volume number falls back to 0.
		source = ID{ResourceName: parts[1]}
		snapName = parts[2]
	case 4:
		// "/<source>/<volume number>/<snapshot>".
		source, err = ParseVolumeId(parts[1] + "/" + parts[2])
		if err != nil {
			return nil, fmt.Errorf("failed to parse snapshot source volume: %w", err)
		}

		snapName = parts[3]
	default:
		return nil, fmt.Errorf("expected SnapshotId to contain a source and snapshot name, got '%s' instead", u.Path)
	}

	return &SnapshotId{
		Type:         ty,
		Remote:       u.Host,
		SourceName:   source.ResourceName,
		VolumeNumber: source.VolumeNumber,
		SnapshotName: snapName,
	}, nil
}

type Snapshot struct {
	SnapshotId
	CreationTime time.Time
	SizeBytes    int64
	Failed       bool
	ReadyToUse   bool
}

// VolumeStats provides details about filesystem usage.
type VolumeStats struct {
	AvailableBytes  int64
	TotalBytes      int64
	UsedBytes       int64
	AvailableInodes int64
	TotalInodes     int64
	UsedInodes      int64
}

type VolumeStatus struct {
	Info
	Conditions *csi.VolumeCondition
}

// Add the given prefix to the property name.
// If the property is already prefixed (with "Aux/"), no modification is made.
func maybeAddTopologyPrefix(prefix string, props ...string) []string {
	const auxPrefix = lc.NamespcAuxiliary + "/"

	result := make([]string, len(props))
	for i, prop := range props {
		if strings.HasPrefix(prop, auxPrefix) {
			result[i] = prop
		} else {
			result[i] = prefix + "/" + prop
		}
	}

	return result
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
