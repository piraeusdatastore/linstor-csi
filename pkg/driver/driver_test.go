package driver

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/kubernetes-csi/csi-test/pkg/sanity"
)

var (
	controllers     = flag.String("controllers", "", "Run suite against a real LINSTOR cluster with the specificed controller endpoints")
	node            = flag.String("node", "fake.node", "Node ID to pass to tests, if you're running against a real LINSTOR cluster this needs to match the name of one of the real satellites")
	paramsFile      = flag.String("parameter-file", "", "File containing paramemers to pass to storage backend during testsing")
	endpoint        = flag.String("Endpoint", "unix:///tmp/csi.sock", "Unix socket for CSI communication")
	mountForReal    = flag.Bool("mount-for-real", false, "Actually try to mount volumes, needs to be ran on on a kubelet (indicted by the node flag) with it's /dev dir bind mounted into the container")
	snapshotForReal = flag.Bool("snapshot-for-real", false, "Actually try to take snapshots, needs to be ran with against a storage pool (passed via paramsFile) that supports snaphosts")
)

func TestDriver(t *testing.T) {

	logFile, err := ioutil.TempFile("", "csi-test-logs")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := NewDriver(
		Endpoint(*endpoint), NodeID(*node), LogOut(logFile),
		Debug, Name("linstor.csi.linbit.com-test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	driver.version = "linstor-csi-test-version"
	defer driver.Stop()

	if *controllers != "" {
		realStorageBackend, err := client.NewLinstor(
			client.Debug, client.Controllers(*controllers), client.LogOut(logFile),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Coljures that return functions that set linstor backends on the driver.
		Storage(realStorageBackend)(driver)
		Assignments(realStorageBackend)(driver)

		if *mountForReal {
			Mounter(realStorageBackend)(driver)
		}
		if *snapshotForReal {
			Snapshots(realStorageBackend)(driver)
		}
	}

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
		TargetPath:               mntDir + "/csi-target",
		StagingPath:              mntStageDir + "/csi-staging",
		Address:                  *endpoint,
		TestVolumeParametersFile: *paramsFile,
		CreateTargetDir: func(targetPath string) (string, error) {
			return targetPath, createTargetDir(targetPath)
		},
		CreateStagingDir: func(targetPath string) (string, error) {
			return targetPath, createTargetDir(targetPath)
		},
	}

	// Now call the test suite
	sanity.Test(t, cfg)
}

// Make a custom function, so we don't have to worry about the test complaining
// the directories it created exist.
func createTargetDir(targetPath string) error {
	fileInfo, err := os.Stat(targetPath)
	if err != nil && os.IsNotExist(err) {
		return os.MkdirAll(targetPath, 0755)
	} else if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("Target location %s is not a directory", targetPath)
	}

	return nil
}
