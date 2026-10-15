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
	"errors"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
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

func testNfsExporter(client *fake.Clientset) *NfsExporter {
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
