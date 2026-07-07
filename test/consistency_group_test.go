// Package test's consistency_group_test.go is the consistency-group e2e harness (the CG analog of
// `make sanity`): it owns the *driver.Driver, drives the real CSI RPCs against a real LINSTOR cluster, and
// supplies PVC membership via a fake dynamic Kubernetes client. Each test skips unless CG_TEST_CONTROLLER is
// set, so `go test ./...` compiles it and skips at runtime; run it for real with `make sanity-cg`.
package test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	lapi "github.com/LINBIT/golinstor/client"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/driver"
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	hlc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

const (
	miB = 1024 * 1024

	// filesystemTypeProperty is the LINSTOR property holding a volume's filesystem type, resolved per member.
	filesystemTypeProperty = "FileSystem/Type"

	pvcNameParam = "csi.storage.k8s.io/pvc/name"
	pvcNsParam   = "csi.storage.k8s.io/pvc/namespace"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

func testNode() string { return envOr("CG_TEST_NODE", "sanity-host") }

func testStoragePool() string { return envOr("CG_TEST_STORAGE_POOL", "thin-pool") }

// uniqueGroup gives every run a fresh group (hence a fresh derived resource), avoiding idempotency aliasing.
func uniqueGroup(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

// realLinstor connects to the LINSTOR controller brought up by `make sanity-cg`.
func realLinstor(t *testing.T) (*client.Linstor, *hlc.HighLevelClient) {
	t.Helper()

	testController, ok := os.LookupEnv("CG_TEST_CONTROLLER")
	if !ok {
		t.Skip("no CG_TEST_CONTROLLER was set")
	}

	u, err := url.Parse(testController)
	require.NoError(t, err)

	hl, err := hlc.NewHighLevelClient(lapi.BaseURL(u))
	require.NoError(t, err)

	cl, err := client.NewLinstor(client.APIClient(hl))
	require.NoError(t, err)

	return cl, hl
}

// pvc builds an unstructured PVC; a non-empty group stamps the consistency-group label.
func pvc(namespace, name, group string) *unstructured.Unstructured {
	meta := map[string]interface{}{
		"name":      name,
		"namespace": namespace,
	}
	if group != "" {
		meta["labels"] = map[string]interface{}{
			"linstor.csi.linbit.com/consistency-group": group,
		}
	}

	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata":   meta,
	}}
}

func fakeKube(objs ...runtime.Object) dynamic.Interface {
	// The fake dynamic client must know the list kind for every resource the driver LISTs; register them.
	gvrToListKind := map[schema.GroupVersionResource]string{
		{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}:                        "PersistentVolumeClaimList",
		{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotcontents"}: "VolumeSnapshotContentList",
		{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}:  "VolumeSnapshotClassList",
		{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}:        "VolumeSnapshotList",
	}

	return dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

// newDriver builds an in-process driver with the fake Kubernetes client injected; extra options apply last.
func newDriver(t *testing.T, cl *client.Linstor, kube dynamic.Interface, extra ...func(*driver.Driver) error) *driver.Driver {
	t.Helper()

	opts := append([]func(*driver.Driver) error{
		driver.NodeID(testNode()),
		driver.LogLevel("debug"),
		driver.KubeClient(kube),
	}, extra...)

	drv, err := driver.NewDriver(cl, opts...)
	require.NoError(t, err)

	return drv
}

// createReq builds a CreateVolume request; pvcName/namespace go in as the extra-create-metadata params.
func createReq(name, namespace, pvcName, fsType string, sizeBytes int64, block bool) *csi.CreateVolumeRequest {
	volCap := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
	if block {
		volCap.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
	} else {
		volCap.AccessType = &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: fsType}}
	}

	params := map[string]string{
		"linstor.csi.linbit.com/placementCount": "1",
		"linstor.csi.linbit.com/storagePool":    testStoragePool(),
		"linstor.csi.linbit.com/layerList":      "storage",
	}
	if pvcName != "" {
		params[pvcNameParam] = pvcName
		params[pvcNsParam] = namespace
	}

	return &csi.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: sizeBytes},
		VolumeCapabilities: []*csi.VolumeCapability{volCap},
		Parameters:         params,
	}
}

func mustParseID(t *testing.T, id string) (string, int) {
	t.Helper()

	parsed, err := volume.ParseVolumeId(id)
	require.NoError(t, err)

	return parsed.ResourceName, parsed.VolumeNumber
}

func runInParallel[T any](t *testing.T, funcs ...func() (T, error)) []T {
	t.Helper()

	resps := make([]T, len(funcs))
	errs := make([]error, len(funcs))

	var wg sync.WaitGroup

	for i, f := range funcs {
		// wg.Go handles Add/Done; each goroutine writes only its own index, so no locking is needed.
		wg.Go(func() {
			resps[i], errs[i] = f()
		})
	}

	wg.Wait()

	// Assert on the test goroutine: require/FailNow must not be called from the spawned goroutines.
	for _, err := range errs {
		require.NoError(t, err)
	}

	return resps
}

func volumeDefs(t *testing.T, hl *hlc.HighLevelClient, resource string) []lapi.VolumeDefinition {
	t.Helper()

	vds, err := hl.ResourceDefinitions.GetVolumeDefinitions(t.Context(), resource)
	require.NoError(t, err)

	return vds
}

func vdByMember(vds []lapi.VolumeDefinition, csiName string) (lapi.VolumeDefinition, bool) {
	for _, vd := range vds {
		if vd.Props[linstor.PropertyCSIVolumeName] == csiName {
			return vd, true
		}
	}

	return lapi.VolumeDefinition{}, false
}

func resourceExists(ctx context.Context, hl *hlc.HighLevelClient, resource string) bool {
	_, err := hl.ResourceDefinitions.Get(ctx, resource)

	return err == nil
}

// TestCGAllocation: two PVCs sharing a CG label land in one resource as distinct tagged volume numbers, and
// re-creating a member is idempotent.
func TestCGAllocation(t *testing.T) {
	ctx := t.Context()
	cl, hl := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("alloc")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "pvc-a", group), pvc(ns, "pvc-b", group)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("pvc-a", ns, "pvc-a", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("pvc-b", ns, "pvc-b", "ext4", 128*miB, false))
		},
	)
	respA, respB := resps[0], resps[1]

	idA, idB := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idB})
			},
		)
	})

	resA, nrA := mustParseID(t, idA)
	resB, nrB := mustParseID(t, idB)

	assert.Equal(t, resA, resB, "both members must live in one resource")
	assert.NotEqual(t, nrA, nrB, "members must get distinct volume numbers")

	vds := volumeDefs(t, hl, resA)
	assert.Len(t, vds, 2, "shared resource should hold two volume definitions")

	vdA, okA := vdByMember(vds, "pvc-a")
	vdB, okB := vdByMember(vds, "pvc-b")
	require.True(t, okA && okB, "both members must be tagged with %s", linstor.PropertyCSIVolumeName)
	assert.EqualValues(t, nrA, *vdA.VolumeNumber)
	assert.EqualValues(t, nrB, *vdB.VolumeNumber)

	// Idempotency: re-issuing CreateVolume for pvc-a returns the same ID and allocates no new volume number.
	respA2, err := drv.CreateVolume(ctx, createReq("pvc-a", ns, "pvc-a", "ext4", 128*miB, false))
	require.NoError(t, err)
	assert.Equal(t, idA, respA2.GetVolume().GetVolumeId())
	assert.Len(t, volumeDefs(t, hl, resA), 2, "idempotent retry must not allocate a new volume number")
}

// TestCGMemberDeleteGC: deleting one member keeps the shared resource; deleting the last (even concurrently)
// garbage-collects it.
func TestCGMemberDeleteGC(t *testing.T) {
	ctx := t.Context()
	cl, hl := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("gc")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "gc-a", group), pvc(ns, "gc-b", group), pvc(ns, "gc-c", group)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("gc-a", ns, "gc-a", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("gc-b", ns, "gc-b", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("gc-c", ns, "gc-c", "ext4", 128*miB, false))
		},
	)
	respA, respB, respC := resps[0], resps[1], resps[2]

	resource, _ := mustParseID(t, respA.GetVolume().GetVolumeId())

	_, err := drv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: respA.GetVolume().GetVolumeId()})
	require.NoError(t, err)
	assert.True(t, resourceExists(ctx, hl, resource), "resource must survive while a member remains")
	assert.Len(t, volumeDefs(t, hl, resource), 2, "deleting a member must remove only its volume definition")

	// Delete the last two members concurrently: the "last member" GC decision must converge.
	runInParallel(t,
		func() (*csi.DeleteVolumeResponse, error) {
			return drv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: respB.GetVolume().GetVolumeId()})
		},
		func() (*csi.DeleteVolumeResponse, error) {
			return drv.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: respC.GetVolume().GetVolumeId()})
		},
	)
	assert.False(t, resourceExists(ctx, hl, resource), "resource must be GC'd once the last member is deleted")
}

// TestCGNamespaceIsolation: the same group name in two namespaces yields two distinct resources (keyed by
// namespace+group, not the label value alone).
func TestCGNamespaceIsolation(t *testing.T) {
	cl, _ := realLinstor(t)

	group := uniqueGroup("iso")
	drv := newDriver(t, cl, fakeKube(pvc("ns-one", "pvc-x", group), pvc("ns-two", "pvc-x", group)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(t.Context(), createReq("pvc-x-1", "ns-one", "pvc-x", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(t.Context(), createReq("pvc-x-2", "ns-two", "pvc-x", "ext4", 128*miB, false))
		},
	)
	respA, respB := resps[0], resps[1]

	idA, idB := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: respA.GetVolume().GetVolumeId()})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: respB.GetVolume().GetVolumeId()})
			},
		)
	})

	resA, _ := mustParseID(t, idA)
	resB, _ := mustParseID(t, idB)
	assert.NotEqual(t, resA, resB, "same group in different namespaces must not collide")
}

// TestCGPerMemberFsType: members of one group may use different filesystems, recorded per volume definition.
func TestCGPerMemberFsType(t *testing.T) {
	cl, hl := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("fstype")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "fs-ext4", group), pvc(ns, "fs-xfs", group)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(t.Context(), createReq("fs-ext4", ns, "fs-ext4", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			// xfs needs a larger minimum volume than ext4.
			return drv.CreateVolume(t.Context(), createReq("fs-xfs", ns, "fs-xfs", "xfs", 512*miB, false))
		},
	)
	respA, respB := resps[0], resps[1]

	idA, idB := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idB})
			},
		)
	})

	resource, _ := mustParseID(t, idA)
	vds := volumeDefs(t, hl, resource)

	vdExt4, ok := vdByMember(vds, "fs-ext4")
	require.True(t, ok)
	assert.Equal(t, "ext4", vdExt4.Props[filesystemTypeProperty], "ext4 member's volume definition must record its own fsType")

	vdXfs, ok := vdByMember(vds, "fs-xfs")
	require.True(t, ok)
	assert.Equal(t, "xfs", vdXfs.Props[filesystemTypeProperty], "xfs member's volume definition must record its own fsType")
}

// TestCGGroupSnapshotDedup: a group snapshot of members sharing a resource is one resource-scoped LINSTOR
// snapshot (the crash-consistency invariant), still exposed as one CSI snapshot per member.
func TestCGGroupSnapshotDedup(t *testing.T) {
	ctx := t.Context()
	cl, hl := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("gsnap")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "gs-a", group), pvc(ns, "gs-b", group)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("gs-a", ns, "gs-a", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("gs-b", ns, "gs-b", "ext4", 128*miB, false))
		},
	)
	respA, respB := resps[0], resps[1]

	idA, idB := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idB})
			},
		)
	})

	resource, _ := mustParseID(t, idA)

	gresp, err := drv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
		Name:            uniqueGroup("groupsnap"),
		SourceVolumeIds: []string{idA, idB},
	})
	require.NoError(t, err)

	groupSnap := gresp.GetGroupSnapshot()

	t.Cleanup(func() {
		_, _ = drv.DeleteVolumeGroupSnapshot(context.Background(), &csi.DeleteVolumeGroupSnapshotRequest{GroupSnapshotId: groupSnap.GetGroupSnapshotId()})
	})

	require.Len(t, groupSnap.GetSnapshots(), 2, "group snapshot must expose one CSI snapshot per member")

	sources := map[string]bool{}

	for _, snap := range groupSnap.GetSnapshots() {
		assert.Equal(t, groupSnap.GetGroupSnapshotId(), snap.GetGroupSnapshotId())
		sources[snap.GetSourceVolumeId()] = true
	}

	assert.True(t, sources[idA] && sources[idB], "each member must be represented by its own source volume ID")

	// The invariant: both members were captured by a SINGLE resource-scoped LINSTOR snapshot.
	snaps, err := hl.Resources.GetSnapshots(ctx, resource)
	require.NoError(t, err)
	require.Len(t, snaps, 1, "a group snapshot of one shared resource must be exactly one LINSTOR snapshot")

	// And it must cover both volume numbers (so every member is restorable from it).
	assert.Len(t, snaps[0].VolumeDefinitions, 2, "the single snapshot must capture both members' volume definitions")
}

// TestCGDeleteSnapshotRejectsGroupMember: members share one snapshot, so DeleteSnapshot on a single member is
// refused (FailedPrecondition); only DeleteVolumeGroupSnapshot removes the shared snapshot.
func TestCGDeleteSnapshotRejectsGroupMember(t *testing.T) {
	ctx := t.Context()
	cl, hl := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("delsnap")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "ds-a", group), pvc(ns, "ds-b", group)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("ds-a", ns, "ds-a", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("ds-b", ns, "ds-b", "ext4", 128*miB, false))
		},
	)
	respA, respB := resps[0], resps[1]

	idA, idB := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idB})
			},
		)
	})

	resource, _ := mustParseID(t, idA)

	gresp, err := drv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
		Name:            uniqueGroup("delsnap-gs"),
		SourceVolumeIds: []string{idA, idB},
	})
	require.NoError(t, err)

	groupSnap := gresp.GetGroupSnapshot()

	deleted := false

	t.Cleanup(func() {
		if deleted {
			return
		}

		_, _ = drv.DeleteVolumeGroupSnapshot(context.Background(), &csi.DeleteVolumeGroupSnapshotRequest{GroupSnapshotId: groupSnap.GetGroupSnapshotId()})
	})

	require.Len(t, groupSnap.GetSnapshots(), 2, "group snapshot must expose one CSI snapshot per member")

	// Each member is bound to the group (GroupSnapshotId set); the guard below enforces that at the RPC layer.
	for _, snap := range groupSnap.GetSnapshots() {
		require.Equal(t, groupSnap.GetGroupSnapshotId(), snap.GetGroupSnapshotId(), "member %s must be bound to the group", snap.GetSnapshotId())

		_, err := drv.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snap.GetSnapshotId()})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err),
			"DeleteSnapshot on group member %s must be refused with FailedPrecondition", snap.GetSnapshotId())
	}

	// The refused deletes must not have touched the shared LINSTOR snapshot.
	after, err := hl.Resources.GetSnapshots(ctx, resource)
	require.NoError(t, err)
	require.Len(t, after, 1, "the shared snapshot must survive every rejected DeleteSnapshot")

	// Only DeleteVolumeGroupSnapshot may remove it.
	_, err = drv.DeleteVolumeGroupSnapshot(ctx, &csi.DeleteVolumeGroupSnapshotRequest{GroupSnapshotId: groupSnap.GetGroupSnapshotId()})
	require.NoError(t, err)

	deleted = true

	gone, err := hl.Resources.GetSnapshots(ctx, resource)
	require.NoError(t, err)
	assert.Empty(t, gone, "DeleteVolumeGroupSnapshot must remove the shared snapshot")
}

// TestCGGroupRestore: restoring a group snapshot into a fresh group; each member claims the volume number
// carried in its snapshot ID, so restored members line up with their sources.
func TestCGGroupRestore(t *testing.T) {
	ctx := t.Context()
	cl, _ := realLinstor(t)

	const ns = "cgtest"

	srcGroup := uniqueGroup("restore-src")
	srcDrv := newDriver(t, cl, fakeKube(pvc(ns, "src-a", srcGroup), pvc(ns, "src-b", srcGroup)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return srcDrv.CreateVolume(ctx, createReq("src-a", ns, "src-a", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return srcDrv.CreateVolume(ctx, createReq("src-b", ns, "src-b", "ext4", 128*miB, false))
		},
	)
	respA, respB := resps[0], resps[1]

	idA, idB := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId()
	_, srcNrA := mustParseID(t, idA)
	_, srcNrB := mustParseID(t, idB)
	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return srcDrv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return srcDrv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idB})
			},
		)
	})

	gresp, err := srcDrv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
		Name:            uniqueGroup("restore-snap"),
		SourceVolumeIds: []string{idA, idB},
	})
	require.NoError(t, err)

	groupSnap := gresp.GetGroupSnapshot()

	t.Cleanup(func() {
		_, _ = srcDrv.DeleteVolumeGroupSnapshot(context.Background(), &csi.DeleteVolumeGroupSnapshotRequest{GroupSnapshotId: groupSnap.GetGroupSnapshotId()})
	})

	snapForSource := map[string]string{}
	for _, snap := range groupSnap.GetSnapshots() {
		snapForSource[snap.GetSourceVolumeId()] = snap.GetSnapshotId()
	}

	// Restore into a NEW, all-fresh group. The first member to arrive triggers the whole-resource restore.
	restoreGroup := uniqueGroup("restore-dst")
	dstDrv := newDriver(t, cl, fakeKube(pvc(ns, "dst-a", restoreGroup), pvc(ns, "dst-b", restoreGroup)))

	reqA := createReq("dst-a", ns, "dst-a", "ext4", 128*miB, false)
	reqA.VolumeContentSource = snapshotSource(snapForSource[idA])

	reqB := createReq("dst-b", ns, "dst-b", "ext4", 128*miB, false)
	reqB.VolumeContentSource = snapshotSource(snapForSource[idB])

	// Restore the members concurrently: the first triggers the whole-resource restore, the other claims its number.
	rresps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) { return dstDrv.CreateVolume(ctx, reqA) },
		func() (*csi.CreateVolumeResponse, error) { return dstDrv.CreateVolume(ctx, reqB) },
	)
	rrA, rrB := rresps[0], rresps[1]

	restoreIDA, restoreIDB := rrA.GetVolume().GetVolumeId(), rrB.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return dstDrv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: restoreIDA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return dstDrv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: restoreIDB})
			},
		)
	})

	resA, nrA := mustParseID(t, restoreIDA)
	resB, nrB := mustParseID(t, restoreIDB)

	assert.Equal(t, resA, resB, "restored members must share one resource")
	assert.Equal(t, srcNrA, nrA, "restored member must claim the volume number carried in its snapshot ID")
	assert.Equal(t, srcNrB, nrB, "restored member must claim the volume number carried in its snapshot ID")
}

// TestCGListVolumes: ListVolumes emits one entry per tagged member alongside ordinary volumes.
func TestCGListVolumes(t *testing.T) {
	ctx := t.Context()
	cl, _ := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("list")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "ls-a", group), pvc(ns, "ls-b", group), pvc(ns, "ls-plain", "")))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("ls-a", ns, "ls-a", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("ls-b", ns, "ls-b", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("ls-plain", ns, "ls-plain", "ext4", 128*miB, false))
		},
	)
	respA, respB, respP := resps[0], resps[1], resps[2]

	idA, idB, idP := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId(), respP.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idB})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idP})
			},
		)
	})

	resp, err := drv.ListVolumes(ctx, &csi.ListVolumesRequest{})
	require.NoError(t, err)

	ids := map[string]bool{}
	for _, entry := range resp.GetEntries() {
		ids[entry.GetVolume().GetVolumeId()] = true
	}

	assert.True(t, ids[idA], "tagged member %q must be enumerated", idA)
	assert.True(t, ids[idB], "tagged member %q must be enumerated", idB)
	assert.True(t, ids[idP], "ordinary volume %q must be enumerated", idP)
}

// TestCGNodePublish: attach + publish are volume-number aware, so two members resolve to distinct devices.
// Needs root for real attach/mounts; skipped otherwise.
func TestCGNodePublish(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("node publish needs root for real attach + mount")
	}

	ctx := t.Context()
	cl, hl := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("publish")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "pub-a", group), pvc(ns, "pub-b", group)))

	resps := runInParallel(t,
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("pub-a", ns, "pub-a", "ext4", 128*miB, false))
		},
		func() (*csi.CreateVolumeResponse, error) {
			return drv.CreateVolume(ctx, createReq("pub-b", ns, "pub-b", "ext4", 128*miB, false))
		},
	)
	respA, respB := resps[0], resps[1]

	idA, idB := respA.GetVolume().GetVolumeId(), respB.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		runInParallel(t,
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
			},
			func() (*csi.DeleteVolumeResponse, error) {
				return drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idB})
			},
		)
	})

	resource, nrA := mustParseID(t, idA)
	_, nrB := mustParseID(t, idB)

	for _, resp := range []*csi.CreateVolumeResponse{respA, respB} {
		id := resp.GetVolume().GetVolumeId()

		mountCap := &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		}

		pubResp, err := drv.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId:         id,
			NodeId:           testNode(),
			VolumeCapability: mountCap,
		})
		require.NoError(t, err, "attach %s", id)

		target := t.TempDir()
		_, err = drv.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
			VolumeId:         id,
			TargetPath:       target,
			VolumeCapability: mountCap,
			PublishContext:   pubResp.GetPublishContext(),
			VolumeContext:    resp.GetVolume().GetVolumeContext(),
		})
		require.NoError(t, err, "publish %s", id)

		t.Cleanup(func() {
			_, _ = drv.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: id, TargetPath: target})
			_, _ = drv.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{VolumeId: id, NodeId: testNode()})
		})
	}

	// Members share a resource but have distinct volume numbers, so per-vnr resolution yields different devices.
	volA, err := hl.Resources.GetVolume(ctx, resource, testNode(), nrA)
	require.NoError(t, err)
	volB, err := hl.Resources.GetVolume(ctx, resource, testNode(), nrB)
	require.NoError(t, err)

	assert.NotEmpty(t, volA.DevicePath, "member device path must resolve")
	assert.NotEqual(t, volA.DevicePath, volB.DevicePath, "members must resolve to different devices per volume number")
}

// TestCGRejectRWXOverNFS: a CG member requesting RWX-over-NFS is rejected with InvalidArgument, since it would
// commandeer volume numbers within the shared resource.
func TestCGRejectRWXOverNFS(t *testing.T) {
	ctx := t.Context()
	cl, _ := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("rwxnfs")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "nfs-a", group)),
		driver.ConfigureRWX(kubefake.NewSimpleClientset(), ns, "reactor-config"))

	req := createReq("nfs-a", ns, "nfs-a", "ext4", 128*miB, false)
	// RWX-over-NFS needs a DRBD layer, else CreateVolume rejects MULTI_NODE_MULTI_WRITER earlier for a different reason.
	req.Parameters["linstor.csi.linbit.com/layerList"] = "drbd storage"
	req.VolumeCapabilities = []*csi.VolumeCapability{{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
	}}

	resp, err := drv.CreateVolume(ctx, req)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"a consistency-group member requesting RWX-over-NFS must be rejected")

	if err == nil {
		// Defensive: if the guard regresses and the member was created, still clean it up.
		t.Cleanup(func() {
			_, _ = drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: resp.GetVolume().GetVolumeId()})
		})
	}
}

// TestCGRejectConflictingPlacement: a member resolving to a different resource group (StorageClass) than the
// group's shared resource is rejected with InvalidArgument, leaving the resource untouched.
func TestCGRejectConflictingPlacement(t *testing.T) {
	ctx := t.Context()
	cl, hl := realLinstor(t)

	const ns = "cgtest"

	group := uniqueGroup("conflict")
	drv := newDriver(t, cl, fakeKube(pvc(ns, "cf-a", group), pvc(ns, "cf-b", group)))

	respA, err := drv.CreateVolume(ctx, createReq("cf-a", ns, "cf-a", "ext4", 128*miB, false))
	require.NoError(t, err)

	idA := respA.GetVolume().GetVolumeId()

	t.Cleanup(func() {
		_, _ = drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: idA})
	})

	resource, _ := mustParseID(t, idA)

	// A different placementCount derives a different resource-group name, so member B resolves elsewhere.
	reqB := createReq("cf-b", ns, "cf-b", "ext4", 128*miB, false)
	reqB.Parameters["linstor.csi.linbit.com/placementCount"] = "3"

	respB, err := drv.CreateVolume(ctx, reqB)
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "a member resolving to a different resource group must be rejected")

	if err == nil {
		// Defensive: if the guard regresses and the member was created, still clean it up.
		t.Cleanup(func() {
			_, _ = drv.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: respB.GetVolume().GetVolumeId()})
		})
	}

	assert.Len(t, volumeDefs(t, hl, resource), 1, "a rejected member must not be added to the shared resource")
}

func snapshotSource(snapshotID string) *csi.VolumeContentSource {
	return &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID},
		},
	}
}
