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
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// KubeVirtVMLabel is the label that KubeVirt adds to pods to identify the VM they belong to.
const KubeVirtVMLabel = "vm.kubevirt.io/name"

// KubeVirtHotplugDiskLabel is the label that KubeVirt adds to hotplug disk pods.
const KubeVirtHotplugDiskLabel = "kubevirt.io"

// PodGVR is the GroupVersionResource for pods.
var PodGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

// PVGVR is the GroupVersionResource for persistent volumes.
var PVGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumes"}

// ValidateRWXBlockAttachment checks that RWX block volumes are only used by pods belonging to the same VM.
// This prevents misuse of allow-two-primaries while still permitting live migration.
// Returns the VM name if validation passes, or an error if:
// - Multiple pods from different VMs are trying to use the same volume
// - A pod without the KubeVirt VM label is trying to use a volume already attached elsewhere (strict mode)
// Returns empty string for VM name when no pods are using the volume or validation is skipped.
func ValidateRWXBlockAttachment(ctx context.Context, kubeClient dynamic.Interface, log *logrus.Entry, volumeID string) (string, error) {
	log.WithField("volumeID", volumeID).Info("validateRWXBlockAttachment called")

	if kubeClient == nil {
		// Not running in Kubernetes, skip validation
		log.Warn("validateRWXBlockAttachment: kubeClient is nil, skipping validation")
		return "", nil
	}

	// Get PV to find PVC reference
	pv, err := kubeClient.Resource(PVGVR).Get(ctx, volumeID, metav1.GetOptions{})
	if err != nil {
		log.WithError(err).Warn("cannot validate RWX attachment: failed to get PV")
		return "", nil
	}

	// Verify that PV's volumeHandle matches the volumeID
	volumeHandle, found, err := unstructured.NestedString(pv.Object, "spec", "csi", "volumeHandle")
	if err != nil {
		log.WithError(err).Warnf("cannot validate RWX attachment: failed to read volumeHandle for PV %s", volumeID)

		return "", nil
	}

	if !found {
		log.Warnf("cannot validate RWX attachment: volumeHandle not found for PV %s", volumeID)

		return "", nil
	}

	if volumeHandle != volumeID {
		log.WithFields(logrus.Fields{
			"volumeID":     volumeID,
			"volumeHandle": volumeHandle,
		}).Warn("cannot validate RWX attachment: PV volumeHandle does not match volumeID")

		return "", nil
	}

	// Extract claimRef from PV
	claimRef, found, _ := unstructured.NestedMap(pv.Object, "spec", "claimRef")
	if !found {
		log.Warn("cannot validate RWX attachment: PV has no claimRef")
		return "", nil
	}

	pvcName, _, _ := unstructured.NestedString(claimRef, "name")
	pvcNamespace, _, _ := unstructured.NestedString(claimRef, "namespace")

	if pvcNamespace == "" || pvcName == "" {
		log.Warn("cannot validate RWX attachment: PVC name or namespace is empty in claimRef")
		return "", nil
	}

	// List all pods in the namespace
	podList, err := kubeClient.Resource(PodGVR).Namespace(pvcNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list pods in namespace %s: %w", pvcNamespace, err)
	}

	// Filter pods that use this PVC and are in a running/pending state
	type podInfo struct {
		name   string
		vmName string
	}

	var podsUsingPVC []podInfo

	for _, item := range podList.Items {
		// Get pod phase from status
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase == "Succeeded" || phase == "Failed" {
			continue
		}

		// Check if pod uses the PVC
		volumes, found, _ := unstructured.NestedSlice(item.Object, "spec", "volumes")
		if !found {
			continue
		}

		for _, vol := range volumes {
			volMap, ok := vol.(map[string]interface{})
			if !ok {
				continue
			}

			claimName, found, _ := unstructured.NestedString(volMap, "persistentVolumeClaim", "claimName")
			if !found || claimName != pvcName {
				continue
			}

			// Extract VM name, handling both regular and hotplug disk pods
			vmName, err := GetVMNameFromPod(ctx, kubeClient, log, &item)
			if err != nil {
				log.WithError(err).WithField("pod", item.GetName()).Warn("failed to get VM name from pod")
				// Continue with empty vmName - will be caught by strict mode check
				vmName = ""
			}

			podsUsingPVC = append(podsUsingPVC, podInfo{
				name:   item.GetName(),
				vmName: vmName,
			})

			break
		}
	}

	// If 0 or 1 pod uses the PVC, no conflict possible
	if len(podsUsingPVC) <= 1 {
		// Return VM name if there's exactly one pod
		if len(podsUsingPVC) == 1 {
			log.WithFields(logrus.Fields{
				"volumeID":     volumeID,
				"vmName":       podsUsingPVC[0].vmName,
				"podCount":     1,
				"pvcNamespace": pvcNamespace,
				"pvcName":      pvcName,
			}).Info("validateRWXBlockAttachment: single pod found, returning VM name")

			return podsUsingPVC[0].vmName, nil
		}

		log.WithFields(logrus.Fields{
			"volumeID":     volumeID,
			"pvcNamespace": pvcNamespace,
			"pvcName":      pvcName,
		}).Info("validateRWXBlockAttachment: no pods found using PVC")

		return "", nil
	}

	// Check that all pods belong to the same VM
	var vmName string
	for _, pod := range podsUsingPVC {
		if pod.vmName == "" {
			// Strict mode: if any pod doesn't have the KubeVirt label and there are multiple pods,
			// deny the attachment
			return "", fmt.Errorf("RWX block volume %s/%s is used by multiple pods but pod %s does not have the %s label; "+
				"RWX block volumes with allow-two-primaries are only supported for KubeVirt live migration",
				pvcNamespace, pvcName, pod.name, KubeVirtVMLabel)
		}

		if vmName == "" {
			vmName = pod.vmName
		} else if vmName != pod.vmName {
			// Different VMs are trying to use the same volume
			return "", fmt.Errorf("RWX block volume %s/%s is being used by pods from different VMs (%s and %s); "+
				"this is not supported - RWX block volumes with allow-two-primaries are only for live migration of a single VM",
				pvcNamespace, pvcName, vmName, pod.vmName)
		}
	}

	log.WithFields(logrus.Fields{
		"pvcNamespace": pvcNamespace,
		"pvcName":      pvcName,
		"vmName":       vmName,
		"podCount":     len(podsUsingPVC),
	}).Debug("RWX block attachment validated: all pods belong to the same VM (likely live migration)")

	return vmName, nil
}

// GetVMNameFromPod extracts the VM name from a pod, handling both regular virt-launcher pods
// and hotplug disk pods (which reference the virt-launcher pod via ownerReferences).
func GetVMNameFromPod(ctx context.Context, kubeClient dynamic.Interface, log *logrus.Entry, pod *unstructured.Unstructured) (string, error) {
	labels := pod.GetLabels()
	if labels == nil {
		return "", nil
	}

	// Direct case: pod has vm.kubevirt.io/name label (virt-launcher pod)
	if vmName, ok := labels[KubeVirtVMLabel]; ok && vmName != "" {
		return vmName, nil
	}

	// Hotplug disk case: pod has kubevirt.io: hotplug-disk label
	// Follow ownerReferences to find the virt-launcher pod
	if hotplugValue, ok := labels[KubeVirtHotplugDiskLabel]; ok && hotplugValue == "hotplug-disk" {
		ownerRefs := pod.GetOwnerReferences()
		for _, owner := range ownerRefs {
			if owner.Kind != "Pod" || owner.Controller == nil || !*owner.Controller {
				continue
			}

			// Get the owner pod (virt-launcher)
			ownerPod, err := kubeClient.Resource(PodGVR).Namespace(pod.GetNamespace()).Get(ctx, owner.Name, metav1.GetOptions{})
			if err != nil {
				return "", fmt.Errorf("failed to get owner pod %s: %w", owner.Name, err)
			}

			// Extract VM name from owner pod
			ownerLabels := ownerPod.GetLabels()
			if ownerLabels != nil {
				if vmName, ok := ownerLabels[KubeVirtVMLabel]; ok && vmName != "" {
					log.WithFields(logrus.Fields{
						"hotplugPod":   pod.GetName(),
						"virtLauncher": owner.Name,
						"vmName":       vmName,
					}).Debug("resolved VM name from hotplug disk pod via owner reference")

					return vmName, nil
				}
			}

			return "", fmt.Errorf("owner pod %s does not have %s label", owner.Name, KubeVirtVMLabel)
		}

		return "", fmt.Errorf("hotplug disk pod %s has no controller owner reference", pod.GetName())
	}

	return "", nil
}
