package driver

import (
	"crypto/tls"
	"flag"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"github.com/piraeusdatastore/linstor-csi/pkg/client"
	lc "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
)

var (
	lsEndpoint            = flag.String("sanity.linstor-endpoint", "", "Run suite against a real LINSTOR cluster with the specificed controller API endpoint")
	lsSkipTLSVerification = flag.Bool("sanity.linstor-skip-tls-verification", false, "If true, do not verify tls")
	node                  = flag.String("sanity.node", "fake.node", "Node ID to pass to tests, if you're running against a real LINSTOR cluster this needs to match the name of one of the real satellites")
	paramsFile            = flag.String("sanity.parameter-file", "", "File containing paramemers to pass to storage backend during testsing")
	csiEndpoint           = flag.String("sanity.csi-endpoint", "unix:///tmp/csi.sock", "Unix socket for CSI communication")
	mountForReal          = flag.Bool("sanity.mount-for-real", false, "Actually try to mount volumes, needs to be ran on on a kubelet (indicted by the node flag) with it's /dev dir bind mounted into the container")
	logLevel              = flag.String("sanity.log-level", "debug", "how much logging to do")
	rps                   = flag.Float64("sanity.linstor-api-requests-per-second", 0, "Maximum allowed number of LINSTOR API requests per second. Default: Unlimited")
	burst                 = flag.Int("sanity.linstor-api-burst", 1, "Maximum number of API requests allowed before being limited by requests-per-second. Default: 1 (no bursting)")
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
		logger := logrus.NewEntry(logrus.New())
		level, err := logrus.ParseLevel(*logLevel)
		if err != nil {
			t.Fatal(err)
		}
		logger.Logger.SetLevel(level)
		logger.Logger.SetOutput(logFile)
		logger.Logger.SetFormatter(&logrus.TextFormatter{})
		c, err := lc.NewHighLevelClient(
			lapi.BaseURL(u),
			lapi.BasicAuth(&lapi.BasicAuthCfg{Username: os.Getenv("LS_USERNAME"), Password: os.Getenv("LS_PASSWORD")}),
			lapi.HTTPClient(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: *lsSkipTLSVerification}}}),
			lapi.Limit(r, *burst),
			lapi.Log(logger),
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
		_ = Expander(realStorageBackend)(driver)

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

	cfg := sanity.NewTestConfig()
	cfg.Address = *csiEndpoint
	cfg.TargetPath = mntDir + "/csi-target"
	cfg.StagingPath = mntStageDir + "/csi-staging"
	cfg.TestVolumeParametersFile = *paramsFile

	// Now call the test suite
	sanity.Test(t, cfg)
}
