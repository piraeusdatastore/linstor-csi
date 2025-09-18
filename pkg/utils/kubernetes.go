package utils

import (
	"fmt"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func KubernetesClient() (kubernetes.Interface, dynamic.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read cluster config: %w", err)
	}

	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create typed cluster client: %w", err)
	}

	d, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dynamic cluster client: %w", err)
	}

	return c, d, nil
}
