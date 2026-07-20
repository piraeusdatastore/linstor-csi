/*
CSI Driver for Linstor
Copyright © 2018 LINBIT USA, LLC

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	linstor "github.com/LINBIT/golinstor"
	lapicache "github.com/LINBIT/golinstor/cache"
	lapi "github.com/LINBIT/golinstor/client"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/driver"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/utils"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func main() {
	var (
		lsEndpoint                = flag.String("linstor-endpoint", "", "Controller API endpoint for LINSTOR")
		lsSkipTLSVerification     = flag.Bool("linstor-skip-tls-verification", false, "If true, do not verify tls")
		csiEndpoint               = flag.String("csi-endpoint", "unix:///var/lib/kubelet/plugins/linstor.csi.linbit.com/csi.sock", "CSI endpoint")
		node                      = flag.String("node", "", "Node ID to pass to node service")
		logLevel                  = flag.String("log-level", "info", "Enable debug log output. Choose from: panic, fatal, error, warn, info, debug")
		rps                       = flag.Float64("linstor-api-requests-per-second", 0, "Maximum allowed number of LINSTOR API requests per second. Default: Unlimited")
		burst                     = flag.Int("linstor-api-burst", 1, "Maximum number of API requests allowed before being limited by requests-per-second. Default: 1 (no bursting)")
		bearerTokenFile           = flag.String("bearer-token", "", "Read the bearer token from the given file and use it for authentication.")
		propNs                    = flag.String("property-namespace", linstor.NamespcAuxiliary, "Limit the reported topology keys to properties from the given namespace.")
		labelBySP                 = flag.Bool("label-by-storage-pool", true, "Set to false to disable labeling of nodes based on their configured storage pools.")
		nodeCacheTimeout          = flag.Duration("node-cache-timeout", 1*time.Minute, "Duration for which the results of node and storage pool related API responses should be cached.")
		resourceCacheTimeout      = flag.Duration("resource-cache-timeout", 30*time.Second, "Duration for which the results of resource related API responses should be cached.")
		resyncAfter               = flag.Duration("resync-after", 5*time.Minute, "Duration after which reconciliations (such as for VolumeSnapshotClasses) should be rerun.")
		enableRWX                 = flag.Bool("enable-rwx", false, "Enable RWX support via NFS (requires running in Kubernetes).")
		namespace                 = flag.String("nfs-service-namespace", "", "The namespace the NFS service is running in.")
		reactorConfigMapName      = flag.String("nfs-reactor-config-map-name", "linstor-csi-nfs-reactor-config", "Name of the config map used to store promoter configuration")
		disableRWXBlockValidation = flag.Bool("disable-rwx-block-validation", false, "Disable KubeVirt VM ownership validation for RWX block volumes.")
		enableConsistencyGroups   = flag.Bool("enable-consistency-groups", false, "Place PVCs sharing a linstor.csi.linbit.com/consistency-group label as separate volumes of one LINSTOR resource (requires running in Kubernetes).")
		enableSnapshotClasses     = flag.Bool("enable-volume-snapshot-classes", true, "Enable VolumeSnapshotClass handling: reconcile VolumeSnapshotClasses and read snapshot-class parameters (requires running in Kubernetes).")
	)

	flag.Var(&volume.DefaultRemoteAccessPolicy, "default-remote-access-policy", "")

	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// TODO: Take log outputs and options from the command line.
	logOut := os.Stderr
	logFmt := &log.TextFormatter{}

	// Setup logging incase there are errors external to the driver/client.
	log.SetFormatter(logFmt)
	log.SetOutput(logOut)

	logger := log.NewEntry(log.New())
	level, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatal(err)
	}

	logger.Logger.SetLevel(level)
	logger.Logger.SetOutput(logOut)
	logger.Logger.SetFormatter(logFmt)

	r := rate.Limit(*rps)
	if r <= 0 {
		r = rate.Inf
	}

	linstorOpts := []lapi.Option{
		lapi.Limiter(rate.NewLimiter(r, *burst)),
		lapi.UserAgent("linstor-csi/" + driver.Version),
		lapi.Log(logger),
		lapicache.WithCaches(
			&lapicache.NodeCache{Timeout: *nodeCacheTimeout},
			&lapicache.ResourceCache{Timeout: *resourceCacheTimeout},
		),
	}

	if *lsEndpoint != "" {
		u, err := url.Parse(*lsEndpoint)
		if err != nil {
			log.Fatal(err)
		}

		linstorOpts = append(linstorOpts, lapi.BaseURL(u))
	}

	if *lsSkipTLSVerification {
		linstorOpts = append(linstorOpts, lapi.HTTPClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}))
	}

	if *bearerTokenFile != "" {
		token, err := os.ReadFile(*bearerTokenFile)
		if err != nil {
			log.Fatal(err)
		}

		linstorOpts = append(linstorOpts, lapi.BearerToken(string(token)))
	}

	c, err := lc.NewHighLevelClient(linstorOpts...)
	if err != nil {
		log.Fatal(err)
	}

	linstorClient, err := client.NewLinstor(
		client.APIClient(c),
		client.LogFmt(logFmt),
		client.LogLevel(*logLevel),
		client.LogOut(logOut),
		client.PropertyNamespace(*propNs),
		client.LabelBySP(*labelBySP),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Build the Kubernetes clients once. They are nil when not running in Kubernetes
	kubeTyped, kubeDynamic, kubeErr := utils.KubernetesClient()

	opts := []func(*driver.Driver) error{
		driver.LogLevel(*logLevel),
		driver.LogOut(logOut),
		driver.NodeID(*node),
		driver.TopologyPrefix(*propNs),
	}

	if *enableSnapshotClasses {
		if kubeErr != nil {
			log.Warnf("Failed to enable VolumeSnapshotClass handling, continuing without it: %s", kubeErr)
		} else {
			opts = append(opts, driver.SnapshotClient(kubeDynamic))

			if err := driver.ReconcileVolumeSnapshotClass(ctx, kubeDynamic, linstorClient, logger, *resyncAfter); err != nil {
				log.Fatalf("Failed to start VolumeSnapshotClass reconciler: %s", err)
			}
		}
	}

	if *enableRWX {
		if kubeErr != nil {
			log.Warnf("Failed to enable RWX support, continuing without it: %s", kubeErr)
		} else {
			if *namespace == "" {
				content, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
				if err != nil {
					log.Fatalf("RWX enabled but no valid service namespace could be found: %s", err)
				}

				*namespace = strings.TrimSpace(string(content))
			}

			opts = append(opts, driver.ConfigureRWX(kubeTyped, *namespace, *reactorConfigMapName))
		}
	}

	if !*disableRWXBlockValidation {
		if kubeErr != nil {
			log.Warnf("Failed to enable RWX block validation support, continuing without it: %s", kubeErr)
		} else {
			opts = append(opts, driver.EnableRWXBlockValidation(kubeTyped))
		}
	}

	if *enableConsistencyGroups {
		if kubeErr != nil {
			log.Warnf("Failed to enable consistency-group support, continuing without it: %s", kubeErr)
		} else {
			opts = append(opts, driver.ConfigureConsistencyGroups(kubeTyped))
		}
	}

	drv, err := driver.NewDriver(linstorClient, opts...)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := driver.Listen(*csiEndpoint)
	if err != nil {
		log.Fatal(err)
	}

	if err := drv.Serve(ctx, listener); err != nil {
		log.Fatal(err)
	}
}
