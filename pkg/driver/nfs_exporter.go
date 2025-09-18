package driver

import (
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
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
