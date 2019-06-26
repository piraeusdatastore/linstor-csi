package driver

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/kubernetes-csi/csi-test/pkg/sanity"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var (
	lsEndpoint   = flag.String("linstor-endpoint", "", "Run suite against a real LINSTOR cluster with the specificed controller API endpoint")
	node         = flag.String("node", "fake.node", "Node ID to pass to tests, if you're running against a real LINSTOR cluster this needs to match the name of one of the real satellites")
	paramsFile   = flag.String("parameter-file", "", "File containing paramemers to pass to storage backend during testsing")
	csiEndpoint  = flag.String("csi-endpoint", "unix:///tmp/csi.sock", "Unix socket for CSI communication")
	mountForReal = flag.Bool("mount-for-real", false, "Actually try to mount volumes, needs to be ran on on a kubelet (indicted by the node flag) with it's /dev dir bind mounted into the container")
	logLevel     = flag.String("log-level", "debug", "how much logging to do")
	rps          = flag.Float64("linstor-api-requests-per-second", 0, "Maximum allowed number of LINSTOR API requests per second. Default: Unlimited")
	burst        = flag.Int("linstor-api-burst", 1, "Maximum number of API requests allowed before being limited by requests-per-second. Default: 1 (no bursting)")
)

func TestDriver(t *testing.T) {

	logFile, err := ioutil.TempFile("", "csi-test-logs")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := NewDriver(
		Endpoint(*csiEndpoint),
		LogLevel(*logLevel),
		LogOut(logFile),
		Name("linstor.csi.linbit.com-test"),
		NodeID(*node),
	)
	if err != nil {
		t.Fatal(err)
	}
	driver.version = "linstor-csi-test-version"

	if *lsEndpoint != "" {
		u, err := url.Parse(*lsEndpoint)
		if err != nil {
			t.Fatal(err)
		}
		r := rate.Limit(*rps)
		if r <= 0 {
			r = rate.Inf
		}
		c, err := lapi.NewClient(
			lapi.BaseURL(u),
			lapi.Limit(r, *burst),
			lapi.Log(&lapi.LogCfg{Level: *logLevel, Out: logFile, Formatter: &logrus.TextFormatter{}}),
		)
		if err != nil {
			t.Fatal(err)
		}
		realStorageBackend, err := client.NewLinstor(
			client.APIClient(c),
			client.LogLevel(*logLevel),
			client.LogOut(logFile),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Clojures that return functions that set linstor backends on the driver.
		_ = Storage(realStorageBackend)(driver)
		_ = Assignments(realStorageBackend)(driver)
		_ = Snapshots(realStorageBackend)(driver)

		if *mountForReal {
			_ = Mounter(realStorageBackend)(driver)
		}
	}

	// run your driver
	//nolint:errcheck
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
		StagingPath: mntStageDir,
		TargetPath:  mntDir,
		Address:     *csiEndpoint,
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
