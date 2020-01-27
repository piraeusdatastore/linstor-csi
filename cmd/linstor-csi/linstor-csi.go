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
	"crypto/tls"
	"flag"
	"net/http"
	"net/url"
	"os"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/LINBIT/linstor-csi/pkg/driver"
	lc "github.com/LINBIT/linstor-csi/pkg/linstor/highlevelclient"
)

func main() {
	var (
		lsEndpoint            = flag.String("linstor-endpoint", "http://localhost:3070", "Controller API endpoint for LINSTOR")
		lsSkipTLSVerification = flag.Bool("linstor-skip-tls-verification", false, "If true, do not verify tls")
		csiEndpoint           = flag.String("csi-endpoint", "unix:///var/lib/kubelet/plugins/linstor.csi.linbit.com/csi.sock", "CSI endpoint")
		node                  = flag.String("node", "", "Node ID to pass to node service")
		logLevel              = flag.String("log-level", "info", "Enable debug log output. Choose from: panic, fatal, error, warn, info, debug")
		rps                   = flag.Float64("linstor-api-requests-per-second", 0, "Maximum allowed number of LINSTOR API requests per second. Default: Unlimited")
		burst                 = flag.Int("linstor-api-burst", 1, "Maximum number of API requests allowed before being limited by requests-per-second. Default: 1 (no bursting)")
	)
	flag.Parse()

	// TODO: Take log outputs and options from the command line.
	logOut := os.Stderr
	logFmt := &log.TextFormatter{}

	// Setup logging incase there are errors external to the driver/client.
	log.SetFormatter(logFmt)
	log.SetOutput(logOut)

	// Setup API Client and High-Level Client.
	u, err := url.Parse(*lsEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	r := rate.Limit(*rps)
	if r <= 0 {
		r = rate.Inf
	}
	c, err := lc.NewHighLevelClient(
		lapi.BaseURL(u),
		lapi.BasicAuth(&lapi.BasicAuthCfg{Username: os.Getenv("LS_USERNAME"), Password: os.Getenv("LS_PASSWORD")}),
		lapi.HTTPClient(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: *lsSkipTLSVerification}}}),
		lapi.Limit(r, *burst),
		lapi.Log(&lapi.LogCfg{Level: *logLevel, Out: logOut, Formatter: logFmt}),
	)
	if err != nil {
		log.Fatal(err)
	}

	linstorClient, err := client.NewLinstor(
		client.APIClient(c),
		client.LogFmt(logFmt),
		client.LogLevel(*logLevel),
		client.LogOut(logOut),
	)
	if err != nil {
		log.Fatal(err)
	}

	drv, err := driver.NewDriver(
		driver.Assignments(linstorClient),
		driver.Endpoint(*csiEndpoint),
		driver.LogLevel(*logLevel),
		driver.LogOut(logOut),
		driver.Mounter(linstorClient),
		driver.NodeID(*node),
		driver.Snapshots(linstorClient),
		driver.Storage(linstorClient),
		driver.VolumeStatter(linstorClient),
	)
	if err != nil {
		log.Fatal(err)
	}

	//nolint:errcheck
	defer drv.Stop()

	if err := drv.Run(); err != nil {
		log.Fatal(err)
	}
}
