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

	"github.com/LINBIT/linstor-csi/pkg/driver"
)

func main() {
	var (
		endpoint = flag.String("endpoint", "unix:///var/lib/kubelet/plugins/io.drbd.linstor-csi/csi.sock", "CSI endpoint")
	)
	flag.Parse()

	drv, err := driver.NewDriver(*endpoint, os.Stderr)
	if err != nil {
		log.Fatalln(err)
	}
	defer drv.Stop()

	if err := drv.Run(); err != nil {
		log.Fatalln(err)
	}
}
