package utils

import (
	"fmt"

	snapclientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func KubernetesClient() (kubernetes.Interface, snapclientset.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read cluster config: %w", err)
	}

	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create typed cluster client: %w", err)
	}

	s, err := snapclientset.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create snapshot cluster client: %w", err)
	}

	return c, s, nil
}
