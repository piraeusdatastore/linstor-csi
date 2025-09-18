package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/piraeusdatastore/linstor-csi/pkg/utils"
)

func advertise(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("expected command: start|stop|clean")
	}

	switch args[0] {
	case "start":
		return advertiseStart(ctx, args[1:])
	case "stop":
		return advertiseStop(ctx, args[1:])
	case "clean":
		return advertiseClean(ctx, args[1:])
	}

	return fmt.Errorf("expected command: start|stop|clean")
}

// advertiseStart applies the resource needed to connect the Kubernetes Service to this NFS server.
func advertiseStart(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("expected two arguments: <resource-name> <outfile>")
	}

	resource, output := args[0], args[1]

	config, err := readMetadata(resource)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	k8scl, _, err := utils.KubernetesClient()
	if err != nil {
		return fmt.Errorf("failed to get k8s client: %w", err)
	}

	endpointslice, err := k8scl.DiscoveryV1().EndpointSlices(os.Getenv("POD_NAMESPACE")).Create(
		ctx,
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resource,
				Namespace: os.Getenv("POD_NAMESPACE"),
				Labels: map[string]string{
					"kubernetes.io/service-name":             config.ServiceName,
					"endpointslice.kubernetes.io/managed-by": "nfs-helper.linstor.csi.linbit.com",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "v1",
						Kind:               "Pod",
						Name:               os.Getenv("POD_NAME"),
						UID:                types.UID(os.Getenv("POD_UID")),
						BlockOwnerDeletion: ptr.To(true),
						Controller:         ptr.To(true),
					},
				},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses: []string{os.Getenv("POD_IP")},
			}},
			Ports: []discoveryv1.EndpointPort{{
				Name: &resource,
				Port: ptr.To(int32(config.Port)),
			}},
		},
		metav1.CreateOptions{FieldManager: "nfs-helper.linstor.csi.linbit.com"},
	)
	if err != nil {
		return fmt.Errorf("failed to create endpoint slice: %w", err)
	}

	f, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(endpointslice)
	if err != nil {
		return fmt.Errorf("failed to encode json: %w", err)
	}

	return nil
}

// advertiseStop removes the resource connecting this NFS server with a Kubernetes Service. If the resource changed,
// because some other node already took over and applied changes, the resource is left as-is.
func advertiseStop(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected one argument: <filename>")
	}

	infile := args[0]

	f, err := os.Open(infile)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	var endpointslice discoveryv1.EndpointSlice

	err = json.NewDecoder(f).Decode(&endpointslice)
	if err != nil {
		return fmt.Errorf("failed to decode json: %w", err)
	}

	k8scl, _, err := utils.KubernetesClient()
	if err != nil {
		return fmt.Errorf("failed to get k8s client: %w", err)
	}

	err = k8scl.DiscoveryV1().EndpointSlices(os.Getenv("POD_NAMESPACE")).Delete(
		ctx,
		endpointslice.Name,
		metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &endpointslice.UID,
				ResourceVersion: &endpointslice.ResourceVersion,
			},
		},
	)
	if err != nil {
		if errors.IsNotFound(err) || errors.IsConflict(err) {
			return nil
		}

		return fmt.Errorf("failed to delete endpoint slice: %w", err)
	}

	return nil
}

// advertiseClean unconditionally removes the resource connecting an NFS server to a Kubernetes Service.
// This is used as the first step during resource promotion, effectively disconnecting the failed NFS server if it was
// not possible to do that from the failed node.
func advertiseClean(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected one argument: <resource-name>")
	}

	resource := args[0]

	k8scl, _, err := utils.KubernetesClient()
	if err != nil {
		return fmt.Errorf("failed to get k8s client: %w", err)
	}

	err = k8scl.DiscoveryV1().EndpointSlices(os.Getenv("POD_NAMESPACE")).Delete(ctx, resource, metav1.DeleteOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}

		return fmt.Errorf("failed to delete endpoint slice: %w", err)
	}

	return nil
}
