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
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// KubeVirtVMLabel is the label that KubeVirt adds to pods to identify the VM they belong to.
const KubeVirtVMLabel = "vm.kubevirt.io/name"

// KubeVirtHotplugDiskLabel is the label that KubeVirt adds to hotplug disk pods.
const KubeVirtHotplugDiskLabel = "kubevirt.io"

// volumeHandleIndex indexes PersistentVolumes by their CSI volume handle.
const volumeHandleIndex = "byVolumeHandle"

// RWXBlockValidator checks that RWX block volumes are only used by pods belonging to the same KubeVirt VM.
// It resolves the PersistentVolume for a CSI volume handle through an informer-backed index, so the volume
// handle does not need to match the PV object name.
type RWXBlockValidator struct {
	pvIndexer cache.Indexer
	client    kubernetes.Interface
	log       *logrus.Entry
}

// NewRWXBlockValidator builds a PersistentVolume informer indexed by CSI volume handle, starts it, and
// blocks until its cache has synced. The informer keeps the index current for the lifetime of ctx.
func NewRWXBlockValidator(ctx context.Context, client kubernetes.Interface, resyncAfter time.Duration, log *logrus.Entry) (*RWXBlockValidator, error) {
	factory := informers.NewSharedInformerFactory(client, resyncAfter)
	informer := factory.Core().V1().PersistentVolumes().Informer()

	if err := informer.AddIndexers(cache.Indexers{volumeHandleIndex: indexPersistentVolumeByVolumeHandle}); err != nil {
		return nil, fmt.Errorf("failed to add volume handle index: %w", err)
	}

	factory.StartWithContext(ctx)

	if !cache.WaitForNamedCacheSyncWithContext(ctx, informer.HasSynced) {
		return nil, fmt.Errorf("failed to sync persistent volume informer cache")
	}

	return &RWXBlockValidator{
		pvIndexer: informer.GetIndexer(),
		client:    client,
		log:       log.WithField("component", "RWXBlockValidator"),
	}, nil
}

// indexPersistentVolumeByVolumeHandle keys CSI-backed PersistentVolumes by their volume handle.
func indexPersistentVolumeByVolumeHandle(obj interface{}) ([]string, error) {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok || pv.Spec.CSI == nil {
		return nil, nil
	}

	return []string{pv.Spec.CSI.VolumeHandle}, nil
}

// persistentVolumeForVolumeHandle returns the PersistentVolume whose CSI volume handle matches volumeID.
func (v *RWXBlockValidator) persistentVolumeForVolumeHandle(volumeID string) (*corev1.PersistentVolume, error) {
	objs, err := v.pvIndexer.ByIndex(volumeHandleIndex, volumeID)
	if err != nil {
		return nil, err
	}

	if len(objs) != 1 {
		return nil, fmt.Errorf("unexpected number of PVs found with matching volume handle: %d != 1", len(objs))
	}

	pv, ok := objs[0].(*corev1.PersistentVolume)
	if !ok {
		return nil, fmt.Errorf("unexpected PV type: %T", pv)
	}

	// An informer may change this, so create a deep copy
	return pv.DeepCopy(), nil
}

// ValidateAttachment checks that an RWX block volume is only used by pods belonging to the same VM.
// This prevents misuse of allow-two-primaries while still permitting live migration.
// Returns the VM name if validation passes, or an error if:
// - Multiple pods from different VMs are trying to use the same volume
// - A pod without the KubeVirt VM label is trying to use a volume already attached elsewhere (strict mode)
// Returns empty string for VM name when no pods are using the volume or validation is skipped.
func (v *RWXBlockValidator) ValidateAttachment(ctx context.Context, volumeID string) (string, error) {
	log := v.log.WithField("volumeID", volumeID)
	log.Info("validateRWXBlockAttachment called")

	// Resolve the PV by its CSI volume handle; the handle need not match the PV object name.
	pv, err := v.persistentVolumeForVolumeHandle(volumeID)
	if err != nil {
		log.WithError(err).Warn("cannot validate RWX attachment: failed to look up PV by volume handle")
		return "", nil
	}

	if pv.Spec.ClaimRef == nil {
		log.Warn("cannot validate RWX attachment: PV has no claimRef")
		return "", nil
	}

	pvcName := pv.Spec.ClaimRef.Name
	pvcNamespace := pv.Spec.ClaimRef.Namespace

	// List all pods in the namespace
	podList, err := v.client.CoreV1().Pods(pvcNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list pods in namespace %s: %w", pvcNamespace, err)
	}

	// Filter pods that use this PVC and are in a running/pending state
	type podInfo struct {
		name   string
		vmName string
	}

	var podsUsingPVC []podInfo

	for i := range podList.Items {
		pod := &podList.Items[i]

		// Get pod phase from status
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}

		for j := range pod.Spec.Volumes {
			vol := &pod.Spec.Volumes[j]

			if vol.PersistentVolumeClaim == nil {
				continue
			}

			if vol.PersistentVolumeClaim.ClaimName != pvcName {
				continue
			}

			// Extract VM name, handling both regular and hotplug disk pods
			vmName, err := GetVMNameFromPod(ctx, v.client, log, pod)
			if err != nil {
				log.WithError(err).WithField("pod", pod.Name).Warn("failed to get VM name from pod")
				// Continue with empty vmName - will be caught by strict mode check
				vmName = ""
			}

			podsUsingPVC = append(podsUsingPVC, podInfo{
				name:   pod.Name,
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
				"vmName":       podsUsingPVC[0].vmName,
				"podCount":     1,
				"pvcNamespace": pvcNamespace,
				"pvcName":      pvcName,
			}).Info("validateRWXBlockAttachment: single pod found, returning VM name")

			return podsUsingPVC[0].vmName, nil
		}

		log.WithFields(logrus.Fields{
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
func GetVMNameFromPod(ctx context.Context, kubeClient kubernetes.Interface, log *logrus.Entry, pod *corev1.Pod) (string, error) {
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
			ownerPod, err := kubeClient.CoreV1().Pods(pod.GetNamespace()).Get(ctx, owner.Name, metav1.GetOptions{})
			if err != nil {
				return "", fmt.Errorf("failed to get owner pod %s: %w", owner.Name, err)
			}

			// Extract VM name from owner pod
			if vmName, ok := ownerPod.GetLabels()[KubeVirtVMLabel]; ok {
				log.WithFields(logrus.Fields{
					"hotplugPod":   pod.GetName(),
					"virtLauncher": owner.Name,
					"vmName":       vmName,
				}).Debug("resolved VM name from hotplug disk pod via owner reference")

				return vmName, nil
			}

			return "", fmt.Errorf("owner pod %s does not have %s label", owner.Name, KubeVirtVMLabel)
		}

		return "", fmt.Errorf("hotplug disk pod %s has no controller owner reference", pod.GetName())
	}

	return "", nil
}
