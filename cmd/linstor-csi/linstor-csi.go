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
	"flag"
	"net/url"
	"os"

	log "github.com/sirupsen/logrus"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/LINBIT/linstor-csi/pkg/driver"
)

func main() {
	var (
		lsEndpoint  = flag.String("linstor-endpoint", "http://localhost:3070", "Controller API endpoint for LINSTOR")
		csiEndpoint = flag.String("csi-endpoint", "unix:///var/lib/kubelet/plugins/linstor.csi.linbit.com/csi.sock", "CSI endpoint")
		node        = flag.String("node", "", "Node ID to pass to node service")
		logLevel    = flag.String("log-level", "info", "Enable debug log output. Choose from: panic, fatal, error, warn, info, debug")
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
	c, err := lapi.NewClient(
		lapi.BaseURL(u),
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
	)
	if err != nil {
		log.Fatal(err)
	}

	defer drv.Stop()

	if err := drv.Run(); err != nil {
		log.Fatal(err)
	}
}
