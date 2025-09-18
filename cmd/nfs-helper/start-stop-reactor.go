package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/piraeusdatastore/linstor-csi/pkg/utils"
)

// startStopReactor watches the Kubernetes node and starts DRBD reactor while the Kubernetes node is schedulable, and
// stops it again once the node is unschedulable.
//
// This ensures that a graceful node shutdown, which typically involves draining the node first: this does not stop the
// exports because the container will continue to run. By manually stopping the reactor, we also trigger shutdown of the
// system services.
//
// Additionally, it watches the DRBD Reactor configuration for changes and writes it to disk. This is faster than
// waiting for the periodic sync of kubelet for config map volumes.
func startStopReactor(ctx context.Context, _ []string) error {
	k8scl, _, err := utils.KubernetesClient()
	if err != nil {
		return fmt.Errorf("failed to get k8s client: %w", err)
	}

	log.Printf("Creating watcher for node '%s'\n", os.Getenv("NODE_NAME"))

	nodeInformer := coreinformers.NewFilteredNodeInformer(k8scl, 10*time.Minute, cache.Indexers{}, func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", os.Getenv("NODE_NAME")).String()
	})

	_, err = nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			startOrStopReactor(ctx, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			startOrStopReactor(ctx, newObj)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add node informer: %w", err)
	}

	configMapInformer := coreinformers.NewFilteredConfigMapInformer(k8scl, os.Getenv("POD_NAMESPACE"), 10*time.Minute, cache.Indexers{}, func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", os.Getenv("REACTOR_CONFIG_MAP")).String()
	})

	_, err = configMapInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			updateReactorConfig(ctx, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			updateReactorConfig(ctx, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			updateReactorConfig(ctx, &corev1.ConfigMap{})
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add config map event handler: %w", err)
	}

	var eg errgroup.Group
	eg.Go(func() error {
		nodeInformer.RunWithContext(ctx)
		return ctx.Err()
	})
	eg.Go(func() error {
		configMapInformer.RunWithContext(ctx)
		return ctx.Err()
	})

	return eg.Wait()
}

func startOrStopReactor(ctx context.Context, node any) {
	n, ok := node.(*corev1.Node)
	if !ok {
		log.Printf("node %v is not a Node\n", node)
		return
	}

	if n.Spec.Unschedulable {
		if err := exec.CommandContext(ctx, "systemctl", "stop", "drbd-reactor.service").Run(); err != nil {
			log.Printf("failed to stop drbd-reactor: %s\n", err)
		}
	} else {
		if err := exec.CommandContext(ctx, "systemctl", "start", "drbd-reactor.service").Run(); err != nil {
			log.Printf("failed to start drbd-reactor: %s\n", err)
		}
	}
}

func updateReactorConfig(ctx context.Context, configmap any) {
	cm, ok := configmap.(*corev1.ConfigMap)
	if !ok {
		log.Printf("configmap %v is not a ConfigMap\n", configmap)
		return
	}

	for item, content := range cm.Data {
		dest := filepath.Join("/etc/drbd-reactor.d", item)
		temp := dest + "~"

		if err := os.WriteFile(temp, []byte(content), 0o644); err != nil {
			log.Printf("failed to write '%s': %s\n", temp, err)
			return
		}

		if err := os.Rename(temp, dest); err != nil {
			log.Printf("failed to rename '%s' to '%s': %s\n", temp, dest, err)
			return
		}
	}

	files, err := os.ReadDir("/etc/drbd-reactor.d")
	if err != nil {
		log.Printf("failed to read /etc/drbd-reactor.d: %s\n", err)
		return
	}

	for _, f := range files {
		if _, ok := cm.Data[f.Name()]; !ok {
			if err := os.Remove(filepath.Join("/etc/drbd-reactor.d", f.Name())); err != nil {
				log.Printf("failed to remove '%s': %s\n", filepath.Join("/etc/drbd-reactor.d", f.Name()), err)
			}
		}
	}

	if err := exec.CommandContext(ctx, "systemctl", "reload", "drbd-reactor.service").Run(); err != nil {
		log.Printf("failed to reload drbd-reactor: %s\n", err)
	}
}
