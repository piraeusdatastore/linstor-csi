package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/BurntSushi/toml"

	"github.com/piraeusdatastore/linstor-csi/pkg/driver"
)

func generateGaneshaConfig(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("expected two arguments: <resource-name> <outfile>")
	}

	resource, outfile := args[0], args[1]

	config, err := readMetadata(resource)
	if err != nil {
		return fmt.Errorf("failed to get devices for resource: %w", err)
	}

	t, err := template.ParseFiles(config.ConfigTemplatePath)
	if err != nil {
		return fmt.Errorf("failed to parse NFS Ganesha config template: %w", err)
	}

	out, err := os.Create(outfile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer out.Close()

	err = t.Execute(out, map[string]interface{}{
		"Resource":   resource,
		"ExportID":   config.ExportId,
		"Port":       config.Port,
		"DbusPrefix": strings.ReplaceAll(resource, "-", "_"),
		"Squash":     config.Squash,
	})
	if err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	return nil
}

var ReactorConfigLocation = "/etc/drbd-reactor.d"

func readMetadata(resource string) (*driver.NfsMetadata, error) {
	var reactorConfig driver.ReactorConfig

	_, err := toml.DecodeFile(filepath.Join(ReactorConfigLocation, resource+".toml"), &reactorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reactor configuration file: %w", err)
	}

	for i := range reactorConfig.Promoters {
		config, ok := reactorConfig.Promoters[i].ResourceConfig[resource]
		if ok {
			return &config.Metadata, nil
		}
	}

	return nil, fmt.Errorf("failed to parse reactor configuration file, resource '%s' not present in config", resource)
}
