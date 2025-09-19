package driver

import (
	"context"
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
	"k8s.io/client-go/kubernetes"
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

func (n *NfsExporter) Export(ctx context.Context, info *volume.Info, params *volume.Parameters) (*url.URL, error) {
	if !n.Enabled() {
		return nil, fmt.Errorf("NFS Exporter disabled by configuration")
	}

	log := n.log.WithField("export", info.ID)

	log.WithField("service", params.NfsServiceName).Debug("Ensuring port on Service exists")

	svc, err := n.cl.CoreV1().Services(n.namespace).Get(ctx, params.NfsServiceName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to find service '%s': %w", params.NfsServiceName, err)
	}

	var port uint16

	if i := slices.IndexFunc(svc.Spec.Ports, func(p corev1.ServicePort) bool {
		return p.Name == info.ID
	}); i != -1 {
		log.Debug("service port exists")

		port = uint16(svc.Spec.Ports[i].Port)
	} else {
		log.Debug("service port does not exist, creating")
		// Ports is guaranteed to contain at least one entry
		// The first port is a dummy entry
		previous := svc.Spec.Ports[0].Port - 1
		insertAt := 0

		// We keep our ports contiguous
		for _, p := range svc.Spec.Ports {
			if p.Port != previous+1 {
				break
			}

			previous = p.Port
			insertAt++
		}

		if previous+1 > (1 << 16) {
			return nil, fmt.Errorf("service '%s', has no more ports to allocate", svc.Name)
		}

		svc.Spec.Ports = slices.Insert(svc.Spec.Ports, insertAt, corev1.ServicePort{
			Name:        info.ID,
			Port:        previous + 1,
			AppProtocol: ptr.To("nfs"),
			Protocol:    corev1.ProtocolTCP,
		})

		_, err := n.cl.CoreV1().Services(n.namespace).Update(ctx, svc, metav1.UpdateOptions{FieldManager: linstor.DriverName})
		if err != nil {
			return nil, fmt.Errorf("failed to update service '%s': %w", params.NfsServiceName, err)
		}

		port = uint16(previous + 1)
	}

	log.Debug("Creating DRBD Reactor configuration")

	cfg := ReactorConfig{
		Promoters: []PromoterConfig{{ResourceConfig: map[string]ResourceConfig{
			info.ID: {
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
					fmt.Sprintf("prepare-device-links@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("clean-nfs-endpoint@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("mount-recovery@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("mount-export@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("growfs@%s.timer", unit.UnitNamePathEscape(fmt.Sprintf("/srv/exports/%s", info.ID))),
					fmt.Sprintf("chmod@%s.service", unit.UnitNamePathEscape(fmt.Sprintf("/srv/exports/%s", info.ID))),
					fmt.Sprintf("nfs-ganesha@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("advertise-nfs-endpoint@%s.service", unit.UnitNameEscape(info.ID)),
				},
			},
		}}},
	}

	raw, err := toml.Marshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal DRBD Reactor config for '%s': %w", info.ID, err)
	}

	configMap, err := n.cl.CoreV1().ConfigMaps(n.namespace).Get(ctx, n.reactorConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get config map for export '%s': %w", info.ID, err)
	}

	if configMap.Data == nil {
		configMap.Data = make(map[string]string)
	}

	if configMap.Data[info.ID+".toml"] != string(raw) {
		log.WithField("config", info.ID+".toml").Debug("updating config map data")
		configMap.Data[info.ID+".toml"] = string(raw)

		_, err = n.cl.CoreV1().ConfigMaps(n.namespace).Update(ctx, configMap, metav1.UpdateOptions{FieldManager: linstor.DriverName})
		if err != nil {
			return nil, fmt.Errorf("failed to update config map for export '%s': %w", info.ID, err)
		}
	} else {
		log.WithField("config", info.ID+".toml").Debug("config exists")
	}

	return &url.URL{
		Scheme: "nfs",
		Host:   fmt.Sprintf("%s.%s.svc:%d", params.NfsServiceName, n.namespace, port),
		Path:   "/" + info.ID,
	}, nil
}

func (n *NfsExporter) Unexport(ctx context.Context, export string) error {
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

	content, ok := configMap.Data[export+".toml"]
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

	config, ok := reactorConfig.Promoters[0].ResourceConfig[export]
	if !ok {
		return fmt.Errorf("expected resource '%s' to exist in config, got [%s] instead", export, slices.Collect(maps.Keys(reactorConfig.Promoters[0].ResourceConfig)))
	}

	log.WithField("metadata", config.Metadata).Debug("parsed metadata")

	svc, err := n.cl.CoreV1().Services(n.namespace).Get(ctx, config.Metadata.ServiceName, metav1.GetOptions{})

	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to get service '%s': %w", config.Metadata.ServiceName, err)
	}

	if err == nil {
		log.Debug("remove export port from service")

		log.WithField("svc", svc).WithField("port", export).Info("removing export port")
		svc.Spec.Ports = slices.DeleteFunc(svc.Spec.Ports, func(port corev1.ServicePort) bool {
			return port.Name == export
		})
		log.WithField("svc", svc).Info("removed export port")

		log.Debug("updating service to remove export port")

		_, err := n.cl.CoreV1().Services(svc.Namespace).Update(ctx, svc, metav1.UpdateOptions{
			FieldManager: linstor.DriverName,
		})
		if err != nil {
			return fmt.Errorf("failed to update service '%s': %w", config.Metadata.ServiceName, err)
		}
	}

	log.Debug("removing reactor config from config map")

	delete(configMap.Data, export+".toml")

	_, err = n.cl.CoreV1().ConfigMaps(configMap.Namespace).Update(ctx, configMap, metav1.UpdateOptions{
		FieldManager: linstor.DriverName,
	})
	if err != nil {
		return fmt.Errorf("failed to update config map: %w", err)
	}

	return nil
}
