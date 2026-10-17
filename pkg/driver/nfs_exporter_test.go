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

package driver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"

	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

const (
	testNamespace = "piraeus-datastore"
	testService   = "linstor-csi-nfs"
	testConfigMap = "linstor-csi-nfs-server-reactor-config"
	testExport    = "pvc-target"
)

func TestNfsExporterExportRetriesServiceConflicts(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := fake.NewClientset(
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: testService, Namespace: testNamespace},
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{
				Name:     "dummy",
				Port:     1000,
				Protocol: corev1.ProtocolTCP,
			}}},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: testConfigMap, Namespace: testNamespace},
			Data:       map[string]string{"pvc-concurrent.toml": "concurrent config"},
		},
	)

	serviceUpdates := 0

	client.PrependReactor("update", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		serviceUpdates++
		if serviceUpdates != 1 {
			return false, nil, nil
		}

		addServicePort(t, client, corev1.ServicePort{
			Name:        "pvc-concurrent",
			Port:        1001,
			Protocol:    corev1.ProtocolTCP,
			AppProtocol: ptr.To("nfs"),
		})

		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "services"},
			testService,
			errors.New("concurrent service update"),
		)
	})

	configMapPatches := 0

	client.PrependReactor("patch", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		configMapPatches++
		patchAction, ok := action.(k8stesting.PatchAction)
		require.True(t, ok)
		assert.Equal(t, types.MergePatchType, patchAction.GetPatchType())

		return false, nil, nil
	})

	exporter := testNfsExporter(client)
	exportURL, err := exporter.Export(ctx, &volume.Info{
		ID: volume.ID{ResourceName: testExport},
	}, &volume.Parameters{
		NfsServiceName:        testService,
		NfsConfigTemplatePath: "/etc/nfs-helper/default-config.tmpl",
		NfsSquash:             "no_root_squash",
	})
	require.NoError(t, err)
	assert.Equal(t, "nfs://linstor-csi-nfs.piraeus-datastore.svc:1002/pvc-target", exportURL.String())
	assert.Equal(t, 2, serviceUpdates)
	assert.Equal(t, 1, configMapPatches)

	service, err := client.CoreV1().Services(testNamespace).Get(ctx, testService, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, map[string]int32{
		"dummy":          1000,
		"pvc-concurrent": 1001,
		testExport:       1002,
	}, servicePorts(service))

	configMap, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, testConfigMap, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "concurrent config", configMap.Data["pvc-concurrent.toml"])
	assert.Contains(t, configMap.Data, testExport+".toml")
}

func TestNfsExporterUnexportRetriesServiceConflicts(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	raw, err := toml.Marshal(&ReactorConfig{Promoters: []PromoterConfig{{
		ResourceConfig: map[string]ResourceConfig{
			testExport: {
				Metadata: NfsMetadata{ServiceName: testService, Port: 1001, ExportId: 1001},
			},
		},
	}}})
	require.NoError(t, err)

	client := fake.NewClientset(
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: testService, Namespace: testNamespace},
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{
				{Name: "dummy", Port: 1000, Protocol: corev1.ProtocolTCP},
				{Name: testExport, Port: 1001, Protocol: corev1.ProtocolTCP, AppProtocol: ptr.To("nfs")},
				{Name: "pvc-existing", Port: 1002, Protocol: corev1.ProtocolTCP, AppProtocol: ptr.To("nfs")},
			}},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: testConfigMap, Namespace: testNamespace},
			Data: map[string]string{
				testExport + ".toml":  string(raw),
				"pvc-existing.toml":   "existing config",
				"pvc-concurrent.toml": "concurrent config",
			},
		},
	)

	serviceUpdates := 0

	client.PrependReactor("update", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		serviceUpdates++
		if serviceUpdates != 1 {
			return false, nil, nil
		}

		addServicePort(t, client, corev1.ServicePort{
			Name:        "pvc-concurrent",
			Port:        1003,
			Protocol:    corev1.ProtocolTCP,
			AppProtocol: ptr.To("nfs"),
		})

		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "services"},
			testService,
			errors.New("concurrent service update"),
		)
	})

	configMapPatches := 0

	client.PrependReactor("patch", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		configMapPatches++
		patchAction, ok := action.(k8stesting.PatchAction)
		require.True(t, ok)
		assert.Equal(t, types.MergePatchType, patchAction.GetPatchType())

		return false, nil, nil
	})

	err = testNfsExporter(client).Unexport(ctx, volume.ID{ResourceName: testExport})
	require.NoError(t, err)
	assert.Equal(t, 2, serviceUpdates)
	assert.Equal(t, 1, configMapPatches)

	service, err := client.CoreV1().Services(testNamespace).Get(ctx, testService, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, map[string]int32{
		"dummy":          1000,
		"pvc-existing":   1002,
		"pvc-concurrent": 1003,
	}, servicePorts(service))

	configMap, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, testConfigMap, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotContains(t, configMap.Data, testExport+".toml")
	assert.Equal(t, "existing config", configMap.Data["pvc-existing.toml"])
	assert.Equal(t, "concurrent config", configMap.Data["pvc-concurrent.toml"])
}

func TestNfsExporterReactorConfigUsesMergePatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialData   map[string]string
		operation     func(context.Context, *NfsExporter) error
		expectedPatch string
		expectedData  map[string]string
	}{
		{
			name:        "set export",
			initialData: map[string]string{"pvc-existing.toml": "existing config"},
			operation: func(ctx context.Context, exporter *NfsExporter) error {
				return exporter.setReactorConfig(ctx, testExport, "target config")
			},
			expectedPatch: `{"data":{"pvc-target.toml":"target config"}}`,
			expectedData: map[string]string{
				"pvc-existing.toml": "existing config",
				"pvc-target.toml":   "target config",
			},
		},
		{
			name: "remove export",
			initialData: map[string]string{
				"pvc-existing.toml": "existing config",
				"pvc-target.toml":   "target config",
			},
			operation: func(ctx context.Context, exporter *NfsExporter) error {
				return exporter.removeReactorConfig(ctx, testExport)
			},
			expectedPatch: `{"data":{"pvc-target.toml":null}}`,
			expectedData:  map[string]string{"pvc-existing.toml": "existing config"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := fake.NewClientset(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: testConfigMap, Namespace: testNamespace},
				Data:       tt.initialData,
			})

			err := tt.operation(t.Context(), testNfsExporter(client))
			require.NoError(t, err)

			actions := client.Actions()
			require.Len(t, actions, 1)
			patchAction, ok := actions[0].(k8stesting.PatchAction)
			require.True(t, ok)
			assert.Equal(t, types.MergePatchType, patchAction.GetPatchType())
			assert.JSONEq(t, tt.expectedPatch, string(patchAction.GetPatch()))

			configMap, err := client.CoreV1().ConfigMaps(testNamespace).Get(t.Context(), testConfigMap, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.expectedData, configMap.Data)
		})
	}
}

func TestNfsExporterReactorConfigPatchErrors(t *testing.T) {
	t.Parallel()

	t.Run("set reports config map and export", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset()
		err := testNfsExporter(client).setReactorConfig(t.Context(), testExport, "target config")

		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to update config map '"+testConfigMap+"' for export '"+testExport+"'")
		assert.True(t, apierrors.IsNotFound(err))
	})

	t.Run("remove ignores missing config map", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset()
		err := testNfsExporter(client).removeReactorConfig(t.Context(), testExport)

		require.NoError(t, err)
	})

	t.Run("remove reports config map and export", func(t *testing.T) {
		t.Parallel()

		client := fake.NewClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: testConfigMap, Namespace: testNamespace},
		})
		client.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("patch failed")
		})

		err := testNfsExporter(client).removeReactorConfig(t.Context(), testExport)

		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to update config map '"+testConfigMap+"' for export '"+testExport+"'")
	})
}

func TestNfsExporterServiceGetErrorKeepsContext(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset()
	_, err := testNfsExporter(client).ensureServicePort(t.Context(), testService, testExport)

	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to find service '"+testService+"'")
	assert.True(t, apierrors.IsNotFound(err))
}

func TestServiceUpdateBackoffHandlesHighFanoutContention(t *testing.T) {
	const (
		writers            = 16
		contentionDuration = 150 * time.Millisecond
	)

	type result struct {
		attempts int
		err      error
	}

	run := func(retryFn func(func() error) error) []result {
		var ready sync.WaitGroup

		start := make(chan struct{})
		results := make(chan result, writers)
		contentionEnds := time.Time{}

		ready.Add(writers)

		for range writers {
			go func() {
				ready.Done()
				<-start

				attempts := 0

				err := retryFn(func() error {
					attempts++

					if time.Now().Before(contentionEnds) {
						return apierrors.NewConflict(
							schema.GroupResource{Resource: "services"},
							testService,
							errors.New("service is busy with concurrent writers"),
						)
					}

					return nil
				})

				results <- result{attempts: attempts, err: err}
			}()
		}

		ready.Wait()

		contentionEnds = time.Now().Add(contentionDuration)

		close(start)

		completed := make([]result, 0, writers)
		for range writers {
			completed = append(completed, <-results)
		}

		return completed
	}

	// The flat policy spends all five attempts in about 40 ms, while high fanout keeps the shared Service busy.
	flatResults := run(func(fn func() error) error {
		return retry.RetryOnConflict(retry.DefaultRetry, fn)
	})
	for _, result := range flatResults {
		assert.Error(t, result.err)
		assert.Equal(t, retry.DefaultRetry.Steps, result.attempts)
	}

	// Exponential backoff keeps an attempt in reserve until after the contention window has cleared.
	backoffResults := run(retryServiceUpdate)
	for _, result := range backoffResults {
		require.NoError(t, result.err)
		assert.Greater(t, result.attempts, 1)
	}
}

func TestNfsExporterConcurrentExportsPreserveAllUpdates(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	baseClient := fake.NewClientset(
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: testService, Namespace: testNamespace, ResourceVersion: "1"},
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{
				Name:     "dummy",
				Port:     1000,
				Protocol: corev1.ProtocolTCP,
			}}},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: testConfigMap, Namespace: testNamespace, ResourceVersion: "1"},
		},
	)

	exports := []string{"pvc-a", "pvc-b", "pvc-c"}
	client, conflicts := newConflictClient(
		baseClient,
		newGetBarrier(0, len(exports)),
	)

	type result struct {
		exportURL string
		name      string
		err       error
	}

	results := make(chan result, len(exports))
	params := &volume.Parameters{
		NfsServiceName:        testService,
		NfsConfigTemplatePath: "/etc/nfs-helper/default-config.tmpl",
		NfsSquash:             "no_root_squash",
	}

	for _, name := range exports {
		go func() {
			exportURL, err := testNfsExporter(client).Export(ctx, &volume.Info{
				ID: volume.ID{ResourceName: name},
			}, params)
			if err != nil {
				results <- result{name: name, err: err}

				return
			}

			results <- result{name: name, exportURL: exportURL.String()}
		}()
	}

	completed := make([]result, 0, len(exports))
	for range exports {
		completed = append(completed, <-results)
	}

	urls := make(map[string]string, len(exports))

	for _, result := range completed {
		require.NoError(t, result.err)
		urls[result.name] = result.exportURL
	}

	service, err := client.CoreV1().Services(testNamespace).Get(ctx, testService, metav1.GetOptions{})
	require.NoError(t, err)

	ports := servicePorts(service)
	assert.Equal(t, int32(1000), ports["dummy"])
	assert.Len(t, ports, len(exports)+1)

	configMap, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, testConfigMap, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Len(t, configMap.Data, len(exports))

	allocated := make(map[int32]string, len(exports))
	for _, name := range exports {
		port, ok := ports[name]
		require.True(t, ok, "missing service port for %s", name)
		assert.NotContains(t, allocated, port, "port %d allocated to both %s and %s", port, allocated[port], name)
		allocated[port] = name
		assert.Equal(t, fmt.Sprintf("nfs://%s.%s.svc:%d/%s", testService, testNamespace, port, name), urls[name])

		var config ReactorConfig

		_, err := toml.Decode(configMap.Data[name+".toml"], &config)
		require.NoError(t, err)
		require.Len(t, config.Promoters, 1)
		resourceConfig, ok := config.Promoters[0].ResourceConfig[name]
		require.True(t, ok)
		assert.Equal(t, uint16(port), resourceConfig.Metadata.Port)
	}

	assert.GreaterOrEqual(t, conflicts.service, len(exports)-1)
}

func TestNfsExporterConcurrentUnexportsPreserveUnrelatedState(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	exports := []string{"pvc-a", "pvc-b", "pvc-c"}
	ports := []corev1.ServicePort{{Name: "dummy", Port: 1000, Protocol: corev1.ProtocolTCP}}
	data := map[string]string{"pvc-unrelated.toml": "unrelated config"}

	for i, name := range exports {
		port := int32(1001 + i)
		ports = append(ports, corev1.ServicePort{
			Name:        name,
			Port:        port,
			Protocol:    corev1.ProtocolTCP,
			AppProtocol: ptr.To("nfs"),
		})

		raw, err := toml.Marshal(&ReactorConfig{Promoters: []PromoterConfig{{
			ResourceConfig: map[string]ResourceConfig{
				name: {Metadata: NfsMetadata{ServiceName: testService, Port: uint16(port), ExportId: uint16(port)}},
			},
		}}})
		require.NoError(t, err)

		data[name+".toml"] = string(raw)
	}

	baseClient := fake.NewClientset(
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: testService, Namespace: testNamespace, ResourceVersion: "1"},
			Spec:       corev1.ServiceSpec{Ports: ports},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: testConfigMap, Namespace: testNamespace, ResourceVersion: "1"},
			Data:       data,
		},
	)

	client, conflicts := newConflictClient(
		baseClient,
		newGetBarrier(0, len(exports)),
	)

	results := make(chan error, len(exports))
	for _, name := range exports {
		go func() {
			results <- testNfsExporter(client).Unexport(ctx, volume.ID{ResourceName: name})
		}()
	}

	errs := make([]error, 0, len(exports))
	for range exports {
		errs = append(errs, <-results)
	}

	for _, err := range errs {
		require.NoError(t, err)
	}

	service, err := client.CoreV1().Services(testNamespace).Get(ctx, testService, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, map[string]int32{"dummy": 1000}, servicePorts(service))

	configMap, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, testConfigMap, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pvc-unrelated.toml": "unrelated config"}, configMap.Data)

	assert.GreaterOrEqual(t, conflicts.service, len(exports)-1)
}

func testNfsExporter(client kubernetes.Interface) *NfsExporter {
	return &NfsExporter{
		cl:               client,
		namespace:        testNamespace,
		reactorConfigMap: testConfigMap,
		log:              logrus.NewEntry(logrus.New()),
	}
}

func addServicePort(t *testing.T, client *fake.Clientset, port corev1.ServicePort) {
	t.Helper()

	resource := corev1.SchemeGroupVersion.WithResource("services")
	obj, err := client.Tracker().Get(resource, testNamespace, testService)
	require.NoError(t, err)

	service := obj.(*corev1.Service).DeepCopy()
	service.Spec.Ports = append(service.Spec.Ports, port)
	require.NoError(t, client.Tracker().Update(resource, service, testNamespace))
}

func servicePorts(service *corev1.Service) map[string]int32 {
	ports := make(map[string]int32, len(service.Spec.Ports))
	for _, port := range service.Spec.Ports {
		ports[port.Name] = port.Port
	}

	return ports
}

type resourceVersionConflicts struct {
	service int
}

type conflictClient struct {
	kubernetes.Interface
	core typedcorev1.CoreV1Interface
}

func newConflictClient(
	client kubernetes.Interface,
	serviceBarrier *getBarrier,
) (*conflictClient, *resourceVersionConflicts) {
	conflicts := &resourceVersionConflicts{}
	core := &conflictCore{
		CoreV1Interface: client.CoreV1(),
		serviceBarrier:  serviceBarrier,
		conflicts:       conflicts,
	}

	return &conflictClient{Interface: client, core: core}, conflicts
}

func (c *conflictClient) CoreV1() typedcorev1.CoreV1Interface {
	return c.core
}

type conflictCore struct {
	typedcorev1.CoreV1Interface
	serviceBarrier  *getBarrier
	serviceUpdateMu sync.Mutex
	conflicts       *resourceVersionConflicts
}

func (c *conflictCore) Services(namespace string) typedcorev1.ServiceInterface {
	return &conflictServices{
		ServiceInterface: c.CoreV1Interface.Services(namespace),
		barrier:          c.serviceBarrier,
		updateMu:         &c.serviceUpdateMu,
		conflicts:        c.conflicts,
	}
}

type conflictServices struct {
	typedcorev1.ServiceInterface
	barrier   *getBarrier
	updateMu  *sync.Mutex
	conflicts *resourceVersionConflicts
}

func (s *conflictServices) Get(
	ctx context.Context,
	name string,
	options metav1.GetOptions,
) (*corev1.Service, error) {
	service, err := s.ServiceInterface.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}

	if err := s.barrier.wait(); err != nil {
		return nil, err
	}

	return service, nil
}

func (s *conflictServices) Update(
	ctx context.Context,
	service *corev1.Service,
	options metav1.UpdateOptions,
) (*corev1.Service, error) {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	current, err := s.ServiceInterface.Get(ctx, service.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if service.ResourceVersion != current.ResourceVersion {
		s.conflicts.service++

		return nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "services"},
			service.Name,
			errors.New("resource version changed"),
		)
	}

	service = service.DeepCopy()
	service.ResourceVersion = nextResourceVersion(current.ResourceVersion)

	return s.ServiceInterface.Update(ctx, service, options)
}

func nextResourceVersion(current string) string {
	version, err := strconv.Atoi(current)
	if err != nil {
		panic(fmt.Sprintf("invalid resource version %q: %v", current, err))
	}

	return strconv.Itoa(version + 1)
}

type getBarrier struct {
	mu        sync.Mutex
	skip      int
	remaining int
	release   chan struct{}
}

func newGetBarrier(skip, count int) *getBarrier {
	return &getBarrier{
		skip:      skip,
		remaining: count,
		release:   make(chan struct{}),
	}
}

func (b *getBarrier) wait() error {
	b.mu.Lock()

	if b.skip > 0 {
		b.skip--
		b.mu.Unlock()

		return nil
	}

	if b.remaining == 0 {
		b.mu.Unlock()

		return nil
	}

	b.remaining--
	if b.remaining == 0 {
		close(b.release)
	}

	release := b.release
	b.mu.Unlock()

	select {
	case <-release:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("timed out synchronizing concurrent reads")
	}
}
