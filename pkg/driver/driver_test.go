package driver

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/haySwim/csi-test/pkg/sanity"
)

var (
	controllers  = flag.String("controllers", "", "Run suite against a real LINSTOR cluster with the specificed controller endpoints")
	node         = flag.String("node", "fake.node", "Node ID to pass to tests, if you're running against a real LINSTOR cluster this needs to match the name of one of the real satellites")
	paramsFile   = flag.String("parameter-file", "", "File containing paramemers to pass to storage backend during testsing")
	endpoint     = flag.String("Endpoint", "unix:///tmp/csi.sock", "Unix socket for CSI communication")
	mountForReal = flag.Bool("mount-for-real", false, "Actually try to mount volumes, needs to be ran on on a kubelet (indicted by the node flag) with it's /dev dir bind mounted into the container")
)

func TestDriver(t *testing.T) {

	logFile, err := ioutil.TempFile("", "csi-test-logs")
	if err != nil {
		t.Fatal(err)
	}

	driverCfg := Config{
		Endpoint: *endpoint,
		NodeID:   *node,
		LogOut:   logFile,
		Debug:    true,
	}

	mockStorageBackend := &client.MockStorage{}
	driverCfg.Storage = mockStorageBackend
	driverCfg.Assignments = mockStorageBackend
	driverCfg.Mounter = mockStorageBackend

	if *controllers != "" {
		realStorageBackend := client.NewLinstor(client.LinstorConfig{
			LogOut: logFile,

			Debug:       true,
			Controllers: *controllers,
		})
		driverCfg.Storage = realStorageBackend
		driverCfg.Assignments = realStorageBackend
		if *mountForReal {
			driverCfg.Mounter = realStorageBackend
		}
	}

	driver, _ := NewDriver(driverCfg)
	driver.name = "io.drbd.linstor-csi-test"
	driver.version = "linstor-csi-test-version"
	defer driver.Stop()

	// run your driver
	go driver.Run()

	mntDir, err := ioutil.TempDir("", "mnt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(mntDir)

	mntStageDir, err := ioutil.TempDir("", "mnt-stage")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(mntStageDir)

	cfg := &sanity.Config{
		StagingPath:              mntStageDir,
		TargetPath:               mntDir,
		Address:                  *endpoint,
		TestVolumeParametersFile: *paramsFile,
	}

	// Now call the test suite
	sanity.Test(t, cfg)
}
