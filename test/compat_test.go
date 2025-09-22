package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/stretchr/testify/assert"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	hlc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

var (
	snapshotA = &volume.Snapshot{
		SnapshotId: volume.SnapshotId{
			Type:         volume.SnapshotTypeInCluster,
			SourceName:   "pvc-5113e62a-2874-421c-979a-ef08e1543581",
			SnapshotName: "snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956",
		},
		CreationTime: time.Unix(1607588002, 126000000),
		SizeBytes:    838860800,
		ReadyToUse:   true,
	}

	snapshotB = &volume.Snapshot{
		SnapshotId: volume.SnapshotId{
			Type:         volume.SnapshotTypeInCluster,
			SourceName:   "pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65",
			SnapshotName: "snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831",
		},
		CreationTime: time.Unix(1607588001, 392000000),
		SizeBytes:    524288000,
		ReadyToUse:   true,
	}

	fakeControllerResponses = []fakeControllerCfg{
		{
			Path:     "/v1/view/snapshots",
			Response: "[{\"name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"resource_name\":\"pvc-5113e62a-2874-421c-979a-ef08e1543581\",\"nodes\":[\"demo1.linstor-days.at.linbit.com\",\"demo2.linstor-days.at.linbit.com\",\"demo3.linstor-days.at.linbit.com\"],\"props\":{\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"id\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:07:44.79360651Z\\\",\\\"sizeBytes\\\":838860800,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\",\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"SequenceNumber\":\"1\"},\"flags\":[\"SUCCESSFUL\"],\"volume_definitions\":[{\"volume_number\":0,\"size_kib\":819200}],\"uuid\":\"0b733015-6d70-4b04-878e-08faa1992bc3\",\"snapshots\":[{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo1.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"8f19860f-d7f8-4082-a145-d28e2cd556cc\"},{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo2.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"2eee497e-b368-4957-b559-0de8baea0f36\"},{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo3.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"e2fe91c2-3ead-4d7f-b464-cd2595465417\"}]},{\"name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"resource_name\":\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\",\"nodes\":[\"demo1.linstor-days.at.linbit.com\",\"demo2.linstor-days.at.linbit.com\",\"demo3.linstor-days.at.linbit.com\"],\"props\":{\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"id\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:06:07.850996937Z\\\",\\\"sizeBytes\\\":524288000,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\",\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"SequenceNumber\":\"1\"},\"flags\":[\"SUCCESSFUL\"],\"volume_definitions\":[{\"volume_number\":0,\"size_kib\":512000}],\"uuid\":\"3a9be40d-441d-414a-8958-f6f0cb68289d\",\"snapshots\":[{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo1.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001392,\"uuid\":\"8c87e9be-18a1-41f3-8790-8e36a85f7950\"},{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo2.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001393,\"uuid\":\"b8f309fe-5b27-42b5-9c9c-e50cf025dd9e\"},{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo3.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001393,\"uuid\":\"8c6fea5f-7350-46b8-882e-179bd48b724e\"}]}]",
		},
		{
			Path:     "/v1/remotes",
			Response: "{}",
		},
		{
			Path:     "/v1/resource-definitions",
			Response: "[{\"name\":\"pvc-5113e62a-2874-421c-979a-ef08e1543581\",\"external_name\":\"pvc-5113e62a-2874-421c-979a-ef08e1543581\",\"props\":{\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"id\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:07:44.79360651Z\\\",\\\"sizeBytes\\\":838860800,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[{\\\"name\\\":\\\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\\\",\\\"csiSnapshot\\\":{\\\"size_bytes\\\":838860800,\\\"snapshot_id\\\":\\\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\\\",\\\"source_volume_id\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"creation_time\\\":{\\\"seconds\\\":1607588003,\\\"nanos\\\":699615502},\\\"ready_to_use\\\":true}}]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\"},\"layer_data\":[{\"type\":\"DRBD\",\"data\":{\"peer_slots\":7,\"al_stripes\":1,\"al_stripe_size_kib\":32,\"port\":7001,\"transport_type\":\"IP\",\"secret\":\"KTEO4qfkz4HIEPeCloRT\",\"down\":false}}],\"uuid\":\"470c748a-3888-4011-b63f-274f943890c2\",\"resource_group_name\":\"linstor-3-replicas\"},{\"name\":\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\",\"external_name\":\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\",\"props\":{\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"id\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:06:07.850996937Z\\\",\\\"sizeBytes\\\":524288000,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[{\\\"name\\\":\\\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\\\",\\\"csiSnapshot\\\":{\\\"size_bytes\\\":524288000,\\\"snapshot_id\\\":\\\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\\\",\\\"source_volume_id\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"creation_time\\\":{\\\"seconds\\\":1607588003,\\\"nanos\\\":274799909},\\\"ready_to_use\\\":true}}]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\"},\"layer_data\":[{\"type\":\"DRBD\",\"data\":{\"peer_slots\":7,\"al_stripes\":1,\"al_stripe_size_kib\":32,\"port\":7000,\"transport_type\":\"IP\",\"secret\":\"0yUVJALiaegVZtj/9f3D\",\"down\":false}}],\"uuid\":\"4f55956e-4cb1-4303-af51-3401173e0fd7\",\"resource_group_name\":\"linstor-3-replicas\"},{\"name\":\"pvc-a2c61755-25b4-4799-b6e1-36dc889a1604\",\"external_name\":\"pvc-a2c61755-25b4-4799-b6e1-36dc889a1604\",\"props\":{\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"SequenceNumber\":\"1\",\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-a2c61755-25b4-4799-b6e1-36dc889a1604\\\",\\\"id\\\":\\\"pvc-a2c61755-25b4-4799-b6e1-36dc889a1604\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:40:26.035193795Z\\\",\\\"sizeBytes\\\":858993459200,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\"},\"layer_data\":[{\"type\":\"DRBD\",\"data\":{\"peer_slots\":7,\"al_stripes\":1,\"al_stripe_size_kib\":32,\"port\":7002,\"transport_type\":\"IP\",\"secret\":\"UPRKVMupBCrS/GFOfFi1\",\"down\":false}}],\"uuid\":\"5c73fdfe-abf4-4f79-a1be-4a0b2f221cca\",\"resource_group_name\":\"linstor-3-replicas\"}]",
		},
		{
			Path:     "/v1/resource-definitions/pvc-a2c61755-25b4-4799-b6e1-36dc889a1604/snapshots",
			Response: "[]",
		},
		{
			Path:     "/v1/resource-definitions/pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65/snapshots",
			Response: "[{\"name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"resource_name\":\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\",\"nodes\":[\"demo1.linstor-days.at.linbit.com\",\"demo2.linstor-days.at.linbit.com\",\"demo3.linstor-days.at.linbit.com\"],\"props\":{\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"id\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:06:07.850996937Z\\\",\\\"sizeBytes\\\":524288000,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\",\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"SequenceNumber\":\"1\"},\"flags\":[\"SUCCESSFUL\"],\"volume_definitions\":[{\"volume_number\":0,\"size_kib\":512000}],\"uuid\":\"3a9be40d-441d-414a-8958-f6f0cb68289d\",\"snapshots\":[{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo1.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001392,\"uuid\":\"8c87e9be-18a1-41f3-8790-8e36a85f7950\"},{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo2.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001393,\"uuid\":\"b8f309fe-5b27-42b5-9c9c-e50cf025dd9e\"},{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo3.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001393,\"uuid\":\"8c6fea5f-7350-46b8-882e-179bd48b724e\"}]}]",
		},
		{
			Path:     "/v1/resource-definitions/pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65/snapshots/snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831",
			Response: "{\"name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"resource_name\":\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\",\"nodes\":[\"demo1.linstor-days.at.linbit.com\",\"demo2.linstor-days.at.linbit.com\",\"demo3.linstor-days.at.linbit.com\"],\"props\":{\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"id\\\":\\\"pvc-9f0cceb1-6ef7-425e-9f29-15c482b3ac65\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:06:07.850996937Z\\\",\\\"sizeBytes\\\":524288000,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\",\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"SequenceNumber\":\"1\"},\"flags\":[\"SUCCESSFUL\"],\"volume_definitions\":[{\"volume_number\":0,\"size_kib\":512000}],\"uuid\":\"3a9be40d-441d-414a-8958-f6f0cb68289d\",\"snapshots\":[{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo1.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001392,\"uuid\":\"8c87e9be-18a1-41f3-8790-8e36a85f7950\"},{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo2.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001393,\"uuid\":\"b8f309fe-5b27-42b5-9c9c-e50cf025dd9e\"},{\"snapshot_name\":\"snapshot-2255bcf5-6e8a-43ba-8856-a3e330424831\",\"node_name\":\"demo3.linstor-days.at.linbit.com\",\"create_timestamp\":1607588001393,\"uuid\":\"8c6fea5f-7350-46b8-882e-179bd48b724e\"}]}",
		},
		{
			Path:     "/v1/resource-definitions/pvc-5113e62a-2874-421c-979a-ef08e1543581/snapshots",
			Response: "[{\"name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"resource_name\":\"pvc-5113e62a-2874-421c-979a-ef08e1543581\",\"nodes\":[\"demo1.linstor-days.at.linbit.com\",\"demo2.linstor-days.at.linbit.com\",\"demo3.linstor-days.at.linbit.com\"],\"props\":{\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"id\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:07:44.79360651Z\\\",\\\"sizeBytes\\\":838860800,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\",\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"SequenceNumber\":\"1\"},\"flags\":[\"SUCCESSFUL\"],\"volume_definitions\":[{\"volume_number\":0,\"size_kib\":819200}],\"uuid\":\"0b733015-6d70-4b04-878e-08faa1992bc3\",\"snapshots\":[{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo1.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"8f19860f-d7f8-4082-a145-d28e2cd556cc\"},{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo2.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"2eee497e-b368-4957-b559-0de8baea0f36\"},{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo3.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"e2fe91c2-3ead-4d7f-b464-cd2595465417\"}]}]",
		},
		{
			Path:     "/v1/resource-definitions/pvc-5113e62a-2874-421c-979a-ef08e1543581/snapshots/snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956",
			Response: "{\"name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"resource_name\":\"pvc-5113e62a-2874-421c-979a-ef08e1543581\",\"nodes\":[\"demo1.linstor-days.at.linbit.com\",\"demo2.linstor-days.at.linbit.com\",\"demo3.linstor-days.at.linbit.com\"],\"props\":{\"Aux/csi-volume-annotations\":\"{\\\"name\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"id\\\":\\\"pvc-5113e62a-2874-421c-979a-ef08e1543581\\\",\\\"createdBy\\\":\\\"linstor.csi.linbit.com\\\",\\\"creationTime\\\":\\\"2020-12-10T08:07:44.79360651Z\\\",\\\"sizeBytes\\\":838860800,\\\"readonly\\\":false,\\\"parameters\\\":{\\\"autoPlace\\\":\\\"3\\\",\\\"resourceGroup\\\":\\\"linstor-3-replicas\\\",\\\"storagePool\\\":\\\"vdb\\\"},\\\"snapshots\\\":[]}\",\"DrbdOptions/Resource/on-no-quorum\":\"io-error\",\"DrbdOptions/Resource/quorum\":\"majority\",\"DrbdPrimarySetOn\":\"DEMO3.LINSTOR-DAYS.AT.LINBIT.COM\",\"SequenceNumber\":\"1\"},\"flags\":[\"SUCCESSFUL\"],\"volume_definitions\":[{\"volume_number\":0,\"size_kib\":819200}],\"uuid\":\"0b733015-6d70-4b04-878e-08faa1992bc3\",\"snapshots\":[{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo1.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"8f19860f-d7f8-4082-a145-d28e2cd556cc\"},{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo2.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"2eee497e-b368-4957-b559-0de8baea0f36\"},{\"snapshot_name\":\"snapshot-a1b89a9c-f59d-40f1-843a-e4240e98d956\",\"node_name\":\"demo3.linstor-days.at.linbit.com\",\"create_timestamp\":1607588002126,\"uuid\":\"e2fe91c2-3ead-4d7f-b464-cd2595465417\"}]}",
		},
	}
)

type fakeControllerCfg struct {
	Path     string
	Response string
}

func prepareFakeClient(t *testing.T) *client.Linstor {
	handler := &http.ServeMux{}
	for i := range fakeControllerResponses {
		cfg := fakeControllerResponses[i]
		handler.HandleFunc(cfg.Path, func(writer http.ResponseWriter, request *http.Request) {
			_, _ = writer.Write([]byte(cfg.Response))
		})
	}

	httpMock := httptest.NewServer(handler)
	t.Cleanup(httpMock.Close)

	baseURL, err := url.Parse(httpMock.URL)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	compatLinstorClient, err := hlc.NewHighLevelClient(lapi.HTTPClient(httpMock.Client()), lapi.BaseURL(baseURL))

	compatClient, err := client.NewLinstor(client.APIClient(compatLinstorClient))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	return compatClient
}

func TestCompatListSnaps(t *testing.T) {
	ctx := context.Background()

	compatClient := prepareFakeClient(t)

	snaps, err := compatClient.ListSnaps(ctx, 0, 0)
	assert.NoError(t, err)

	expectedSnaps := []*volume.Snapshot{snapshotA, snapshotB}
	assert.ElementsMatch(t, expectedSnaps, snaps)
}

func TestCompatFindSnapByID(t *testing.T) {
	ctx := context.Background()

	compatClient := prepareFakeClient(t)

	empty, err := compatClient.FindSnapByID(ctx, "none")
	assert.NoError(t, err)
	assert.Empty(t, empty)

	actualByInProgressId, err := compatClient.FindSnapByID(ctx, snapshotA.SnapshotName)
	assert.NoError(t, err)
	assert.Equal(t, snapshotA, actualByInProgressId)

	actualByCompleteId, err := compatClient.FindSnapByID(ctx, snapshotB.String())
	assert.NoError(t, err)
	assert.Equal(t, snapshotB, actualByCompleteId)
}

func TestCompatFindSnapBySource(t *testing.T) {
	ctx := context.Background()

	compatClient := prepareFakeClient(t)

	fakeVol := &volume.Info{ID: "fake"}
	empty, err := compatClient.FindSnapsBySource(ctx, fakeVol, 0, 0)
	assert.NoError(t, err)
	assert.Empty(t, empty)

	volB := &volume.Info{ID: snapshotB.SourceName}
	actual, err := compatClient.FindSnapsBySource(ctx, volB, 0, 0)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*volume.Snapshot{snapshotB}, actual)
}
