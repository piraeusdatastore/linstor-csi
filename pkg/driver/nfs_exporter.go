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
}

func (n *NfsExporter) Enabled() bool {
	return n != nil
}

func (n *NfsExporter) Export(ctx context.Context, info *volume.Info, minor uint16, params *volume.Parameters) (*url.URL, error) {
	if !n.Enabled() {
		return nil, fmt.Errorf("NFS Exporter disabled by configuration")
	}

	log := n.log.WithField("export", info.ID)

	log.Debug("Creating DRBD Reactor configuration")
	cfg := ReactorConfig{
		Promoters: []PromoterConfig{{ResourceConfig: map[string]ResourceConfig{
			info.ID: {
				Metadata: NfsMetadata{
					ServiceName:        params.NfsServiceName,
					ConfigTemplatePath: params.NfsConfigTemplatePath,
					Port:               minor,
					ExportId:           minor,
					Squash:             params.NfsSquash,
				},
				Runner:             "systemd",
				StopServicesOnExit: true,
				TargetAs:           "BindsTo",
				Start: []string{
					fmt.Sprintf("prepare-device-links@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("clean-nfs-endpoint@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("mount-recovery@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("mount-export@%s.service", unit.UnitNameEscape(info.ID)),
					fmt.Sprintf("growfs@%s.timer", unit.UnitNamePathEscape(fmt.Sprintf("/var/lib/nfs/ganesha/%s", info.ID))),
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
		configMap, err = n.cl.CoreV1().ConfigMaps(n.namespace).Update(ctx, configMap, metav1.UpdateOptions{FieldManager: linstor.DriverName})
		if err != nil {
			return nil, fmt.Errorf("failed to update config map for export '%s': %w", info.ID, err)
		}
	} else {
		log.WithField("config", info.ID+".toml").Debug("config exists")
	}

	log.WithField("service", params.NfsServiceName).Debug("Creating port on service")
	svc, err := n.cl.CoreV1().Services(n.namespace).Get(ctx, params.NfsServiceName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		log.Debug("service does not exist, creating")
		svc, err = n.cl.CoreV1().Services(n.namespace).Create(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      params.NfsServiceName,
				Namespace: n.namespace,
				Labels:    configMap.Labels,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(configMap, corev1.SchemeGroupVersion.WithKind("ConfigMap")),
				},
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{
					Name:        info.ID,
					Port:        int32(minor),
					AppProtocol: ptr.To("nfs"),
					Protocol:    corev1.ProtocolTCP,
				}},
				Type: params.NfsServiceType,
			},
		}, metav1.CreateOptions{FieldManager: linstor.DriverName})
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get or create service '%s': %w", params.NfsServiceName, err)
	}

	if i := slices.IndexFunc(svc.Spec.Ports, func(p corev1.ServicePort) bool {
		return p.Name == info.ID
	}); i != -1 {
		log.Debug("service exists, updating")
		svc.Spec.Ports[i] = corev1.ServicePort{
			Name:        info.ID,
			Port:        int32(minor),
			AppProtocol: ptr.To("nfs"),
			Protocol:    corev1.ProtocolTCP,
		}
	} else {
		log.Debug("service port does not exist, creating")
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:        info.ID,
			Port:        int32(minor),
			AppProtocol: ptr.To("nfs"),
			Protocol:    corev1.ProtocolTCP,
		})
	}

	log.Debug("need to update service")
	_, err = n.cl.CoreV1().Services(n.namespace).Update(ctx, svc, metav1.UpdateOptions{FieldManager: linstor.DriverName})
	if err != nil {
		return nil, fmt.Errorf("failed to update service '%s': %w", params.NfsServiceName, err)
	}

	log.Debug("created or updated export resource")

	return &url.URL{
		Scheme: "nfs",
		Host:   fmt.Sprintf("%s.%s.svc:%d", params.NfsServiceName, n.namespace, minor),
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

		if len(svc.Spec.Ports) == 0 {
			log.Debug("removing last port from service, deleting")
			err := n.cl.CoreV1().Services(svc.Namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID:             &svc.UID,
					ResourceVersion: &svc.ResourceVersion,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to delete service '%s': %w", config.Metadata.ServiceName, err)
			}
		} else {
			log.Debug("updating service to remove export port")
			_, err := n.cl.CoreV1().Services(svc.Namespace).Update(ctx, svc, metav1.UpdateOptions{
				FieldManager: linstor.DriverName,
			})
			if err != nil {
				return fmt.Errorf("failed to update service '%s': %w", config.Metadata.ServiceName, err)
			}
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
