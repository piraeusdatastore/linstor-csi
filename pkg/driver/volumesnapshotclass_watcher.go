package driver

import (
	"context"
	"fmt"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	snapinformers "github.com/kubernetes-csi/external-snapshotter/client/v8/informers/externalversions"
	snaputils "github.com/kubernetes-csi/external-snapshotter/v8/pkg/utils"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func ReconcileVolumeSnapshotClass(ctx context.Context, snapClient snapclientset.Interface, secretClient kubernetes.Interface, snapshotter *client.Linstor, log *logrus.Entry, resyncAfter time.Duration) error {
	factory := snapinformers.NewSharedInformerFactory(snapClient, resyncAfter)
	volumeSnapshotClassInformer := factory.Snapshot().V1().VolumeSnapshotClasses().Informer()

	err := volumeSnapshotClassInformer.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
		if errors.IsNotFound(err) {
			log.WithError(err).Debug("volumesnapshotclasses.snapshot.storage.k8s.io not deployed in cluster, ignoring")
		} else {
			cache.DefaultWatchErrorHandler(ctx, r, err)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to set error handler: %w", err)
	}

	reconcile := func(obj any) {
		class, ok := obj.(*snapv1.VolumeSnapshotClass)
		if !ok {
			log.Warnf("expected *VolumeSnapshotClass, got %T", obj)
			return
		}

		params, err := snapshotParamsFromClass(ctx, secretClient, class)
		if err != nil {
			log.WithError(err).Warn("failed to parse volume snapshot class")
			return
		}

		if params == nil {
			return
		}

		if err := snapshotter.ReconcileRemote(ctx, params); err != nil {
			log.WithError(err).Warnf("failed to reconcile remote for volumesnapshotclass '%s'", class.GetName())
		}
	}

	_, err = volumeSnapshotClassInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    reconcile,
		UpdateFunc: func(_, newObj any) { reconcile(newObj) },
	}, resyncAfter)
	if err != nil {
		return err
	}

	factory.Start(ctx.Done())

	return nil
}

func snapshotParamsFromClass(ctx context.Context, secretClient kubernetes.Interface, class *snapv1.VolumeSnapshotClass) (*volume.SnapshotParameters, error) {
	if class.Driver != linstor.DriverName {
		return nil, nil
	}

	secretName := class.Parameters[snaputils.PrefixedSnapshotterSecretNameKey]
	secretNamespace := class.Parameters[snaputils.PrefixedSnapshotterSecretNamespaceKey]

	var secretMap map[string]string

	if secretName != "" {
		secret, err := secretClient.CoreV1().Secrets(secretNamespace).Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get secret '%s': %w", secretName, err)
		}

		secretMap = make(map[string]string, len(secret.Data))
		for k, v := range secret.Data {
			secretMap[k] = string(v)
		}
	}

	return volume.NewSnapshotParameters(class.Parameters, secretMap)
}
