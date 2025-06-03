package driver

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/kubernetes-csi/external-snapshotter/v8/pkg/utils"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func ReconcileVolumeSnapshotClass(ctx context.Context, client dynamic.Interface, snapshotter volume.SnapshotCreateDeleter, log *logrus.Entry, resyncAfter time.Duration) error {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(client, resyncAfter)
	volumeSnapshotClassIndexer := factory.ForResource(schema.GroupVersionResource{
		Group:    "snapshot.storage.k8s.io",
		Version:  "v1",
		Resource: "volumesnapshotclasses",
	}).Informer()

	err := volumeSnapshotClassIndexer.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
		if errors.IsNotFound(err) {
			log.WithError(err).Debug("volumesnapshotclasses.snapshot.storage.k8s.io not deployed in cluster, ignoring")
		} else {
			cache.DefaultWatchErrorHandler(ctx, r, err)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to set error handler: %w", err)
	}

	_, err = volumeSnapshotClassIndexer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			params, err := snapshotParamsFromUnstructured(ctx, client, obj.(*unstructured.Unstructured))
			if err != nil {
				log.WithError(err).Warn("failed to parse unstructured object")
				return
			}

			if params != nil {
				err := snapshotter.ReconcileRemote(ctx, params)
				if err != nil {
					log.WithError(err).Warnf("failed to reconcile remote for volumesnapshotclass '%s'", obj.(*unstructured.Unstructured).GetName())
					return
				}
			}
		},
		UpdateFunc: func(_, newObj any) {
			newParams, err := snapshotParamsFromUnstructured(ctx, client, newObj.(*unstructured.Unstructured))
			if err != nil {
				log.WithError(err).Warn("failed to parse unstructured (new) object")
				return
			}

			if newParams != nil {
				err := snapshotter.ReconcileRemote(ctx, newParams)
				if err != nil {
					log.WithError(err).Warnf("failed to reconcile remote for volumesnapshotclass '%s'", newObj.(*unstructured.Unstructured).GetName())
					return
				}
			}
		},
	}, resyncAfter)
	if err != nil {
		return err
	}

	factory.Start(ctx.Done())

	return nil
}

func snapshotParamsFromUnstructured(ctx context.Context, client dynamic.Interface, obj *unstructured.Unstructured) (*volume.SnapshotParameters, error) {
	driver, _, err := unstructured.NestedString(obj.Object, "driver")
	if err != nil {
		return nil, fmt.Errorf("failed to get driver name: %w", err)
	}

	if driver != linstor.DriverName {
		return nil, nil
	}

	params, _, err := unstructured.NestedStringMap(obj.Object, "parameters")
	if err != nil {
		return nil, fmt.Errorf("failed to get driver parameters: %w", err)
	}

	secretName, _, err := unstructured.NestedString(obj.Object, "parameters", utils.PrefixedSnapshotterSecretNameKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get driver secret name: %w", err)
	}

	secretNamespace, _, err := unstructured.NestedString(obj.Object, "parameters", utils.PrefixedSnapshotterSecretNamespaceKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get driver secret namespace: %w", err)
	}

	var secretMap map[string]string

	if secretName != "" {
		secret, err := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "secrets"}).Namespace(secretNamespace).Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get secret '%s': %w", secretName, err)
		}

		secretMap, _, err = unstructured.NestedStringMap(secret.Object, "data")
		if err != nil {
			return nil, fmt.Errorf("failed to access secret '%s': %w", secretName, err)
		}

		for k, v := range secretMap {
			b, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, fmt.Errorf("failed to decode secret value '%s/data/%s': %w", secretName, k, err)
			}

			secretMap[k] = string(b)
		}
	}

	return volume.NewSnapshotParameters(params, secretMap)
}
