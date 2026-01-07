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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestValidateRWXBlockAttachment(t *testing.T) {
	testCases := []struct {
		name        string
		pods        []*unstructured.Unstructured
		pvcName     string
		namespace   string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "no pods using PVC",
			pods:        []*unstructured.Unstructured{},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "single pod using PVC",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "two pods same VM (live migration)",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("virt-launcher-vm1-abc", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("virt-launcher-vm1-xyz", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "two pods different VMs (should fail)",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("virt-launcher-vm1-abc", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("virt-launcher-vm2-xyz", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: true,
			errorMsg:    "different VMs",
		},
		{
			name: "pod without KubeVirt label when multiple pods exist (strict mode)",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("pod2", "default", "test-pvc", map[string]string{}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: true,
			errorMsg:    "does not have the vm.kubevirt.io/name label",
		},
		{
			name: "completed pods should be ignored",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("pod2", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Succeeded"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "failed pods should be ignored",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("pod2", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Failed"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "pods in different namespace should not conflict",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("pod2", "other", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "pods using different PVCs should not conflict",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("pod2", "default", "other-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "three pods from same VM (multi-node live migration scenario)",
			pods: []*unstructured.Unstructured{
				createUnstructuredPod("virt-launcher-vm1-a", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("virt-launcher-vm1-b", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createUnstructuredPod("virt-launcher-vm1-c", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Pending"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name:        "empty PVC name should skip validation",
			pods:        []*unstructured.Unstructured{},
			pvcName:     "",
			namespace:   "default",
			expectError: false,
		},
		{
			name:        "empty namespace should skip validation",
			pods:        []*unstructured.Unstructured{},
			pvcName:     "test-pvc",
			namespace:   "",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create fake dynamic client with test pods
			scheme := runtime.NewScheme()

			objects := make([]runtime.Object, len(tc.pods))
			for i, pod := range tc.pods {
				objects[i] = pod
			}

			gvrToListKind := map[schema.GroupVersionResource]string{
				podGVR: "PodList",
			}
			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objects...)

			// Create driver with fake client
			logger := logrus.NewEntry(logrus.New())
			logger.Logger.SetLevel(logrus.DebugLevel)

			driver := &Driver{
				kubeClient: client,
				log:        logger,
			}

			// Run validation
			err := driver.validateRWXBlockAttachment(context.Background(), tc.namespace, tc.pvcName)

			if tc.expectError {
				assert.Error(t, err)

				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRWXBlockAttachmentNoKubeClient(t *testing.T) {
	// When not running in Kubernetes (no client), validation should be skipped
	logger := logrus.NewEntry(logrus.New())
	driver := &Driver{
		kubeClient: nil,
		log:        logger,
	}

	err := driver.validateRWXBlockAttachment(context.Background(), "default", "test-pvc")
	assert.NoError(t, err)
}

// createUnstructuredPod creates an unstructured pod object for testing.
func createUnstructuredPod(name, namespace, pvcName string, labels map[string]string, phase string) *unstructured.Unstructured {
	pod := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
				"labels":    toStringInterfaceMap(labels),
			},
			"spec": map[string]interface{}{
				"volumes": []interface{}{
					map[string]interface{}{
						"name": "data",
						"persistentVolumeClaim": map[string]interface{}{
							"claimName": pvcName,
						},
					},
				},
			},
			"status": map[string]interface{}{
				"phase": phase,
			},
		},
	}

	return pod
}

// toStringInterfaceMap converts map[string]string to map[string]interface{}.
func toStringInterfaceMap(m map[string]string) map[string]interface{} {
	result := make(map[string]interface{})

	for k, v := range m {
		result[k] = v
	}

	return result
}
