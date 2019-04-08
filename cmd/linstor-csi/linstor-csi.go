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
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/LINBIT/linstor-csi/pkg/driver"
)

func main() {
	var (
		controllers = flag.String("controllers", "", "Controller endpoints for LINSTOR")
		endpoint    = flag.String("endpoint", "unix:///var/lib/kubelet/plugins/io.drbd.linstor-csi/csi.sock", "CSI endpoint")
		node        = flag.String("node", "", "Node ID to pass to node service")
		debug       = flag.Bool("debug-logging", false, "Enable debut log output")
	)
	flag.Parse()

	// TODO: Take log outputs and options from the command line.
	logOut := os.Stderr
	logFmt := &log.TextFormatter{}

	// Setup logging incase there are errors external to the driver/client.
	log.SetFormatter(logFmt)
	log.SetOutput(logOut)

	linstorClient, err := client.NewLinstor(
		client.LogOut(logOut), client.LogFmt(logFmt),
		client.Controllers(*controllers),
	)
	if err != nil {
		log.Fatal(err)
	}

	drv, err := driver.NewDriver(
		driver.Endpoint(*endpoint), driver.NodeID(*node), driver.LogOut(logOut),
		driver.Storage(linstorClient), driver.Assignments(linstorClient),
		driver.Mounter(linstorClient), driver.Snapshots(linstorClient),
	)
	if err != nil {
		log.Fatal(err)
	}

	if *debug {
		client.Debug(linstorClient)
		driver.Debug(drv)
	}

	defer drv.Stop()

	if err := drv.Run(); err != nil {
		log.Fatal(err)
	}
}
