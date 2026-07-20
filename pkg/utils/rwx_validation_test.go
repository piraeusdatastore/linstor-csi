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

package utils

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
)

func TestValidateRWXBlockAttachment(t *testing.T) {
	testCases := []struct {
		name        string
		pods        []*corev1.Pod
		pvcName     string
		namespace   string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "no pods using PVC",
			pods:        []*corev1.Pod{},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "single pod using PVC",
			pods: []*corev1.Pod{
				createPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "two pods same VM (live migration)",
			pods: []*corev1.Pod{
				createPod("virt-launcher-vm1-abc", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("virt-launcher-vm1-xyz", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "two pods different VMs (should fail)",
			pods: []*corev1.Pod{
				createPod("virt-launcher-vm1-abc", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("virt-launcher-vm2-xyz", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: true,
			errorMsg:    "different VMs",
		},
		{
			name: "pod without KubeVirt label when multiple pods exist (strict mode)",
			pods: []*corev1.Pod{
				createPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("pod2", "default", "test-pvc", map[string]string{}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: true,
			errorMsg:    "does not have the vm.kubevirt.io/name label",
		},
		{
			name: "completed pods should be ignored",
			pods: []*corev1.Pod{
				createPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("pod2", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Succeeded"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "failed pods should be ignored",
			pods: []*corev1.Pod{
				createPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("pod2", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Failed"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "pods in different namespace should not conflict",
			pods: []*corev1.Pod{
				createPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("pod2", "other", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "pods using different PVCs should not conflict",
			pods: []*corev1.Pod{
				createPod("pod1", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("pod2", "default", "other-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "three pods from same VM (multi-node live migration scenario)",
			pods: []*corev1.Pod{
				createPod("virt-launcher-vm1-a", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("virt-launcher-vm1-b", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createPod("virt-launcher-vm1-c", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Pending"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "hotplug disk pod with virt-launcher (should succeed)",
			pods: []*corev1.Pod{
				createPod("virt-launcher-vm1-abc", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createHotplugDiskPod("hp-volume-xyz", "default", "test-pvc", "virt-launcher-vm1-abc", "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: false,
		},
		{
			name: "hotplug disks from different VMs (should fail)",
			pods: []*corev1.Pod{
				createPod("virt-launcher-vm1-abc", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
				createHotplugDiskPod("hp-volume-vm1", "default", "test-pvc", "virt-launcher-vm1-abc", "Running"),
				createPod("virt-launcher-vm2-xyz", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
				createHotplugDiskPod("hp-volume-vm2", "default", "test-pvc", "virt-launcher-vm2-xyz", "Running"),
			},
			pvcName:     "test-pvc",
			namespace:   "default",
			expectError: true,
			errorMsg:    "different VMs",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create PV object that references the PVC
			pv := createPV("test-volume-id", "test-volume-id", tc.namespace, tc.pvcName)

			objects := make([]runtime.Object, 0, len(tc.pods)+1)
			objects = append(objects, pv)

			for _, pod := range tc.pods {
				objects = append(objects, pod)
			}

			validator := newValidator(t, objects...)

			// Run validation
			vmName, err := validator.ValidateAttachment(t.Context(), "test-volume-id")

			if tc.expectError {
				assert.Error(t, err)

				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				// VM name is returned when there are pods using the volume
				if len(tc.pods) > 0 {
					assert.NotEmpty(t, vmName)
				}
			}
		})
	}
}

func TestValidateRWXBlockAttachmentPVNotFound(t *testing.T) {
	// When no PV matches the volume handle, validation should be skipped without error.
	validator := newValidator(t)

	vmName, err := validator.ValidateAttachment(t.Context(), "non-existent-pv")
	assert.NoError(t, err)
	assert.Empty(t, vmName)
}

func TestValidateRWXBlockAttachmentFindsPVByVolumeHandle(t *testing.T) {
	// The PV object name differs from the CSI volume handle; the validator must still resolve it
	// through the volume-handle index rather than assuming the handle is the PV name.
	validator := newValidator(t,
		createPV("different-pv-name", "test-volume-id", "default", "test-pvc"),
		createPod("virt-launcher-vm1-abc", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm1"}, "Running"),
		createPod("virt-launcher-vm2-xyz", "default", "test-pvc", map[string]string{KubeVirtVMLabel: "vm2"}, "Running"),
	)

	vmName, err := validator.ValidateAttachment(t.Context(), "test-volume-id")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "different VMs")
	assert.Empty(t, vmName)
}

// newValidator builds an RWXBlockValidator backed by a fake client seeded with the given objects.
func newValidator(t *testing.T, objects ...runtime.Object) *RWXBlockValidator {
	t.Helper()

	client := fake.NewClientset(objects...)

	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.DebugLevel)

	validator, err := NewRWXBlockValidator(t.Context(), client, 0, logger)
	require.NoError(t, err)

	return validator
}

// createPod creates a pod object for testing.
func createPod(name, namespace, pvcName string, labels map[string]string, phase corev1.PodPhase) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}

	return pod
}

// createPV creates a PersistentVolume object for testing. The object name and CSI volume handle are
// separate so tests can exercise the case where they differ.
func createPV(name, volumeHandle, pvcNamespace, pvcName string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Name:      pvcName,
				Namespace: pvcNamespace,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       linstor.DriverName,
					VolumeHandle: volumeHandle,
				},
			},
		},
	}

	return pv
}

// createHotplugDiskPod creates a hotplug disk pod that references a virt-launcher pod via ownerReferences.
func createHotplugDiskPod(name, namespace, pvcName, ownerPodName string, phase corev1.PodPhase) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"kubevirt.io": "hotplug-disk",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "v1",
				Kind:               "Pod",
				Name:               ownerPodName,
				Controller:         new(true),
				BlockOwnerDeletion: new(true),
			}},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}

	return pod
}
