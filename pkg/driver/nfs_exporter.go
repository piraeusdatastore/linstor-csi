package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"slices"

	"github.com/BurntSushi/toml"
	"github.com/coreos/go-systemd/v22/unit"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"

	"github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

type NfsExporter struct {
	cl               kubernetes.Interface
	namespace        string
	reactorConfigMap string
	log              *logrus.Entry
}

type ReactorConfig struct {
	Promoters []PromoterConfig `toml:"promoter"`
}

type NfsMetadata struct {
	ServiceName        string `toml:"service-name"`
	Port               uint16 `toml:"port"`
	ExportId           uint16 `toml:"export-id"`
	Squash             string `toml:"squash"`
	ConfigTemplatePath string `toml:"config-template-path"`
}

type PromoterConfig struct {
	ResourceConfig map[string]ResourceConfig `toml:"resources"`
}

type ResourceConfig struct {
	Metadata           NfsMetadata `toml:"nfs-metadata"`
	Runner             string      `toml:"runner"`
	StopServicesOnExit bool        `toml:"stop-services-on-exit"`
	Start              []string    `toml:"start"`
	TargetAs           string      `toml:"target-as"`
	DependenciesAs     string      `toml:"dependencies-as"`
}

func (n *NfsExporter) Enabled() bool {
	return n != nil
}

func (n *NfsExporter) ensureServicePort(ctx context.Context, service, export string) (uint16, error) {
	var port uint16

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		svc, err := n.cl.CoreV1().Services(n.namespace).Get(ctx, service, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to find service '%s': %w", service, err)
		}

		if i := slices.IndexFunc(svc.Spec.Ports, func(p corev1.ServicePort) bool {
			return p.Name == export
		}); i != -1 {
			port = uint16(svc.Spec.Ports[i].Port)

			return nil
		}

		// Ports is guaranteed to contain at least one entry. The first port is a dummy entry.
		previous := svc.Spec.Ports[0].Port - 1
		insertAt := 0

		// Keep ports contiguous. Recomputing this after every conflict ensures a concurrent export cannot claim the
		// same port or be overwritten by this update.
		for _, p := range svc.Spec.Ports {
			if p.Port != previous+1 {
				break
			}

			previous = p.Port
			insertAt++
		}

		if previous+1 >= 1<<16 {
			return fmt.Errorf("service '%s', has no more ports to allocate", svc.Name)
		}

		svc.Spec.Ports = slices.Insert(svc.Spec.Ports, insertAt, corev1.ServicePort{
			Name:        export,
			Port:        previous + 1,
			AppProtocol: ptr.To("nfs"),
			Protocol:    corev1.ProtocolTCP,
		})

		_, err = n.cl.CoreV1().Services(n.namespace).Update(ctx, svc, metav1.UpdateOptions{FieldManager: linstor.DriverName})
		if err != nil {
			return fmt.Errorf("failed to update service '%s': %w", service, err)
		}

		port = uint16(previous + 1)

		return nil
	})
	if err != nil {
		return 0, err
	}

	return port, nil
}

func (n *NfsExporter) removeServicePort(ctx context.Context, service, export string) error {
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		svc, err := n.cl.CoreV1().Services(n.namespace).Get(ctx, service, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}

			return fmt.Errorf("failed to get service '%s': %w", service, err)
		}

		if !slices.ContainsFunc(svc.Spec.Ports, func(port corev1.ServicePort) bool {
			return port.Name == export
		}) {
			return nil
		}

		svc.Spec.Ports = slices.DeleteFunc(svc.Spec.Ports, func(port corev1.ServicePort) bool {
			return port.Name == export
		})

		_, err = n.cl.CoreV1().Services(n.namespace).Update(ctx, svc, metav1.UpdateOptions{FieldManager: linstor.DriverName})
		if err != nil {
			return fmt.Errorf("failed to update service '%s': %w", service, err)
		}

		return nil
	})

	return err
}

func (n *NfsExporter) patchReactorConfig(ctx context.Context, export string, content *string) error {
	patch, err := json.Marshal(struct {
		Data map[string]*string `json:"data"`
	}{
		Data: map[string]*string{export + ".toml": content},
	})
	if err != nil {
		return fmt.Errorf("failed to marshal config map patch for export '%s': %w", export, err)
	}

	_, err = n.cl.CoreV1().ConfigMaps(n.namespace).Patch(
		ctx,
		n.reactorConfigMap,
		types.MergePatchType,
		patch,
		metav1.PatchOptions{FieldManager: linstor.DriverName},
	)

	return err
}

func (n *NfsExporter) setReactorConfig(ctx context.Context, export, content string) error {
	err := n.patchReactorConfig(ctx, export, &content)
	if err != nil {
		return fmt.Errorf("failed to update config map '%s' for export '%s': %w", n.reactorConfigMap, export, err)
	}

	return nil
}

func (n *NfsExporter) removeReactorConfig(ctx context.Context, export string) error {
	err := n.patchReactorConfig(ctx, export, nil)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}

		return fmt.Errorf("failed to update config map '%s' for export '%s': %w", n.reactorConfigMap, export, err)
	}

	return nil
}

func (n *NfsExporter) Export(ctx context.Context, info *volume.Info, params *volume.Parameters) (*url.URL, error) {
	if !n.Enabled() {
		return nil, fmt.Errorf("NFS Exporter disabled by configuration")
	}

	log := n.log.WithField("export", info.ID)

	log.WithField("service", params.NfsServiceName).Debug("Ensuring port on Service exists")

	port, err := n.ensureServicePort(ctx, params.NfsServiceName, info.ResourceName)
	if err != nil {
		return nil, err
	}

	log.Debug("Creating DRBD Reactor configuration")

	cfg := ReactorConfig{
		Promoters: []PromoterConfig{{ResourceConfig: map[string]ResourceConfig{
			info.ResourceName: {
				Metadata: NfsMetadata{
					ServiceName:        params.NfsServiceName,
					ConfigTemplatePath: params.NfsConfigTemplatePath,
					Port:               port,
					ExportId:           port,
					Squash:             params.NfsSquash,
				},
				Runner:             "systemd",
				StopServicesOnExit: true,
				TargetAs:           "BindsTo",
				DependenciesAs:     "BindsTo",
				Start: []string{
					fmt.Sprintf("prepare-device-links@%s.service", unit.UnitNameEscape(info.ResourceName)),
					fmt.Sprintf("clean-nfs-endpoint@%s.service", unit.UnitNameEscape(info.ResourceName)),
					fmt.Sprintf("mount-recovery@%s.service", unit.UnitNameEscape(info.ResourceName)),
					fmt.Sprintf("mount-export@%s.service", unit.UnitNameEscape(info.ResourceName)),
					fmt.Sprintf("growfs@%s.timer", unit.UnitNamePathEscape(fmt.Sprintf("/srv/exports/%s", info.ID))),
					fmt.Sprintf("chmod@%s.service", unit.UnitNamePathEscape(fmt.Sprintf("/srv/exports/%s", info.ID))),
					fmt.Sprintf("nfs-ganesha@%s.service", unit.UnitNameEscape(info.ResourceName)),
					fmt.Sprintf("advertise-nfs-endpoint@%s.service", unit.UnitNameEscape(info.ResourceName)),
				},
			},
		}}},
	}

	raw, err := toml.Marshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal DRBD Reactor config for '%s': %w", info.ID, err)
	}

	if err := n.setReactorConfig(ctx, info.ResourceName, string(raw)); err != nil {
		return nil, err
	}

	return &url.URL{
		Scheme: "nfs",
		Host:   fmt.Sprintf("%s.%s.svc:%d", params.NfsServiceName, n.namespace, port),
		Path:   "/" + info.ResourceName,
	}, nil
}

func (n *NfsExporter) Unexport(ctx context.Context, export volume.ID) error {
	if !n.Enabled() {
		return nil
	}

	log := n.log.WithField("export", export)

	log.Debug("Checking if export exists in config map")

	configMap, err := n.cl.CoreV1().ConfigMaps(n.namespace).Get(ctx, n.reactorConfigMap, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Debug("Config map does not exist, nothing to unexport")
			return nil
		}

		return fmt.Errorf("failed to get config map for export '%s': %w", export, err)
	}

	content, ok := configMap.Data[export.ResourceName+".toml"]
	if !ok {
		log.Debug("Config map has no matching entry, nothing to unexport")
		return nil
	}

	var reactorConfig ReactorConfig

	_, err = toml.Decode(content, &reactorConfig)
	if err != nil {
		return fmt.Errorf("failed to decode reactor config: %w", err)
	}

	if len(reactorConfig.Promoters) != 1 {
		return fmt.Errorf("unexpected number of promoter configuration, expected 1, got %d", len(reactorConfig.Promoters))
	}

	config, ok := reactorConfig.Promoters[0].ResourceConfig[export.ResourceName]
	if !ok {
		return fmt.Errorf("expected resource '%s' to exist in config, got [%s] instead", export, slices.Collect(maps.Keys(reactorConfig.Promoters[0].ResourceConfig)))
	}

	log.WithField("metadata", config.Metadata).Debug("parsed metadata")

	if err := n.removeServicePort(ctx, config.Metadata.ServiceName, export.ResourceName); err != nil {
		return err
	}

	log.Debug("removing reactor config from config map")

	return n.removeReactorConfig(ctx, export.ResourceName)
}
