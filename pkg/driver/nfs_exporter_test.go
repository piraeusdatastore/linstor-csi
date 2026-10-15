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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

const (
	testNamespace = "piraeus-datastore"
	testService   = "linstor-csi-nfs"
	testConfigMap = "linstor-csi-nfs-server-reactor-config"
	testExport    = "pvc-target"
)

func TestNfsExporterExportRetriesConflicts(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := fake.NewSimpleClientset(
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

	configMapUpdates := 0

	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		configMapUpdates++
		if configMapUpdates != 1 {
			return false, nil, nil
		}

		setConfigMapEntry(t, client, "pvc-concurrent.toml", "concurrent config")

		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "configmaps"},
			testConfigMap,
			errors.New("concurrent config map update"),
		)
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
	assert.Equal(t, 2, configMapUpdates)

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

func TestNfsExporterUnexportRetriesConflicts(t *testing.T) {
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

	client := fake.NewSimpleClientset(
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
				testExport + ".toml": string(raw),
				"pvc-existing.toml":  "existing config",
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

	configMapUpdates := 0

	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		configMapUpdates++
		if configMapUpdates != 1 {
			return false, nil, nil
		}

		setConfigMapEntry(t, client, "pvc-concurrent.toml", "concurrent config")

		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "configmaps"},
			testConfigMap,
			errors.New("concurrent config map update"),
		)
	})

	err = testNfsExporter(client).Unexport(ctx, volume.ID{ResourceName: testExport})
	require.NoError(t, err)
	assert.Equal(t, 2, serviceUpdates)
	assert.Equal(t, 2, configMapUpdates)

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

func TestNfsExporterConcurrentExportsPreserveAllUpdates(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	baseClient := fake.NewSimpleClientset(
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
	assert.GreaterOrEqual(t, conflicts.configMap, len(exports)-1)
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

	baseClient := fake.NewSimpleClientset(
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: testService, Namespace: testNamespace, ResourceVersion: "1"},
			Spec:       corev1.ServiceSpec{Ports: ports},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: testConfigMap, Namespace: testNamespace, ResourceVersion: "1"},
			Data:       data,
		},
	)

	// Unexport first reads the ConfigMap to discover the Service name. Synchronize the following reads that
	// perform the actual ConfigMap removals.
	client, conflicts := newConflictClient(
		baseClient,
		newGetBarrier(0, len(exports)),
		newGetBarrier(len(exports), len(exports)),
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
	assert.GreaterOrEqual(t, conflicts.configMap, len(exports)-1)
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

func setConfigMapEntry(t *testing.T, client *fake.Clientset, key, value string) {
	t.Helper()

	resource := corev1.SchemeGroupVersion.WithResource("configmaps")
	obj, err := client.Tracker().Get(resource, testNamespace, testConfigMap)
	require.NoError(t, err)

	configMap := obj.(*corev1.ConfigMap).DeepCopy()
	if configMap.Data == nil {
		configMap.Data = make(map[string]string)
	}

	configMap.Data[key] = value
	require.NoError(t, client.Tracker().Update(resource, configMap, testNamespace))
}

func servicePorts(service *corev1.Service) map[string]int32 {
	ports := make(map[string]int32, len(service.Spec.Ports))
	for _, port := range service.Spec.Ports {
		ports[port.Name] = port.Port
	}

	return ports
}

type resourceVersionConflicts struct {
	service   int
	configMap int
}

type conflictClient struct {
	kubernetes.Interface
	core typedcorev1.CoreV1Interface
}

func newConflictClient(
	client kubernetes.Interface,
	serviceBarrier, configMapBarrier *getBarrier,
) (*conflictClient, *resourceVersionConflicts) {
	conflicts := &resourceVersionConflicts{}
	core := &conflictCore{
		CoreV1Interface:  client.CoreV1(),
		serviceBarrier:   serviceBarrier,
		configMapBarrier: configMapBarrier,
		conflicts:        conflicts,
	}

	return &conflictClient{Interface: client, core: core}, conflicts
}

func (c *conflictClient) CoreV1() typedcorev1.CoreV1Interface {
	return c.core
}

type conflictCore struct {
	typedcorev1.CoreV1Interface
	serviceBarrier    *getBarrier
	configMapBarrier  *getBarrier
	serviceUpdateMu   sync.Mutex
	configMapUpdateMu sync.Mutex
	conflicts         *resourceVersionConflicts
}

func (c *conflictCore) Services(namespace string) typedcorev1.ServiceInterface {
	return &conflictServices{
		ServiceInterface: c.CoreV1Interface.Services(namespace),
		barrier:          c.serviceBarrier,
		updateMu:         &c.serviceUpdateMu,
		conflicts:        c.conflicts,
	}
}

func (c *conflictCore) ConfigMaps(namespace string) typedcorev1.ConfigMapInterface {
	return &conflictConfigMaps{
		ConfigMapInterface: c.CoreV1Interface.ConfigMaps(namespace),
		barrier:            c.configMapBarrier,
		updateMu:           &c.configMapUpdateMu,
		conflicts:          c.conflicts,
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

type conflictConfigMaps struct {
	typedcorev1.ConfigMapInterface
	barrier   *getBarrier
	updateMu  *sync.Mutex
	conflicts *resourceVersionConflicts
}

func (c *conflictConfigMaps) Get(
	ctx context.Context,
	name string,
	options metav1.GetOptions,
) (*corev1.ConfigMap, error) {
	configMap, err := c.ConfigMapInterface.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}

	if err := c.barrier.wait(); err != nil {
		return nil, err
	}

	return configMap, nil
}

func (c *conflictConfigMaps) Update(
	ctx context.Context,
	configMap *corev1.ConfigMap,
	options metav1.UpdateOptions,
) (*corev1.ConfigMap, error) {
	c.updateMu.Lock()
	defer c.updateMu.Unlock()

	current, err := c.ConfigMapInterface.Get(ctx, configMap.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if configMap.ResourceVersion != current.ResourceVersion {
		c.conflicts.configMap++

		return nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "configmaps"},
			configMap.Name,
			errors.New("resource version changed"),
		)
	}

	configMap = configMap.DeepCopy()
	configMap.ResourceVersion = nextResourceVersion(current.ResourceVersion)

	return c.ConfigMapInterface.Update(ctx, configMap, options)
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
