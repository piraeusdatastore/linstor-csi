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
	"log"
	"os"

	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/LINBIT/linstor-csi/pkg/driver"
)

func main() {
	var (
		controllers = flag.String("controllers", "", "Controller endpoints for LINSTOR")
		endpoint    = flag.String("endpoint", "unix:///var/lib/kubelet/plugins/io.drbd.linstor-csi/csi.sock", "CSI endpoint")
		storagePool = flag.String("storage-pool", "", "Linstor Storage Pool for use during testing")
		node        = flag.String("node", "", "Node ID to pass to node service")
	)
	flag.Parse()

	// TODO: This is messy, need to sort out what needs what configuration.
	driverCfg := driver.Config{
		Endpoint:           *endpoint,
		Node:               *node,
		LogOut:             os.Stderr,
		DefaultControllers: *controllers,
		DefaultStoragePool: *storagePool,
	}
	linstorClient := client.NewLinstor(driverCfg.LogOut, "csi-test-annotations")
	linstorClient.DefaultControllers = *controllers
	linstorClient.DefaultStoragePool = *storagePool
	driverCfg.Storage = linstorClient
	driverCfg.Assignments = linstorClient
	driverCfg.Mount = linstorClient

	drv, err := driver.NewDriver(driverCfg)

	if err != nil {
		log.Fatalln(err)
	}
	defer drv.Stop()

	if err := drv.Run(); err != nil {
		log.Fatalln(err)
	}
}
