package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"github.com/piraeusdatastore/linstor-csi/pkg/utils"
)

// startStopReactor watches the Kubernetes node and starts DRBD reactor while the Kubernetes node is schedule, and
// stops it again once the node is unschedulable. This ensures that a graceful node shutdown, which typically involves
// draining the node first, does not cause a "real" failover.
func startStopReactor(ctx context.Context, _ []string) error {
	k8scl, _, err := utils.KubernetesClient()
	if err != nil {
		return fmt.Errorf("failed to get k8s client: %w", err)
	}

	log.Printf("Creating watcher for node %s=%s\n", corev1.LabelHostname, os.Getenv("NODE_NAME"))
	informerFactory := informers.NewSharedInformerFactoryWithOptions(k8scl, 10*time.Minute, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
		options.LabelSelector = fmt.Sprintf("%s=%s", corev1.LabelHostname, os.Getenv("NODE_NAME"))
	}))
	nodeInformer := informerFactory.Core().V1().Nodes().Informer()
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

	log.Printf("Starting watcher\n")
	nodeInformer.Run(ctx.Done())
	return nil
}

func startOrStopReactor(ctx context.Context, node any) {
	n, ok := node.(*corev1.Node)
	if !ok {
		log.Printf("node %v is not a Node\n", node)
		return
	}

	if n.Spec.Unschedulable {
		err := exec.CommandContext(ctx, "systemctl", "stop", "drbd-reactor.service", "drbd-reactor-reload.path").Run()
		if err != nil {
			log.Printf("failed to stop drbd-reactor: %s\n", err)
		}
	} else {
		err := exec.CommandContext(ctx, "systemctl", "start", "drbd-reactor.service", "drbd-reactor-reload.path").Run()
		if err != nil {
			log.Printf("failed to start drbd-reactor: %s\n", err)
		}
	}
}
