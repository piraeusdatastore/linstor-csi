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

func TestValidResourceName(t *testing.T) {
	all := "all"
	if err := validResourceName(all); err == nil {
		t.Fatalf("Expected '%s' to be be an invalid keyword", all)
	}

	tooLong := "abcdefghijklmnopqrstuvwyzABCDEFGHIJKLMNOPQRSTUVWXYZ_______" // 49
	if err := validResourceName(tooLong); err == nil {
		t.Fatalf("Expected '%s' to be too long", tooLong)
	}

	utf8rune := "hello🐱kitty"
	if err := validResourceName(utf8rune); err == nil {
		t.Fatalf("Expected '%s' to fail, because of an utf rune", utf8rune)
	}

	invalid := "_-"
	if err := validResourceName(invalid); err == nil {
		t.Fatalf("Expected '%s' to fail, because it is an invalid name", invalid)
	}

	valid := "rck23"
	if err := validResourceName(valid); err != nil {
		t.Fatalf("Expected '%s' to be valid", valid)
	}
}

func TestLinstorifyResourceName(t *testing.T) {
	var unitTests = []struct {
		in, out string
		errExp  bool
	}{
		{
			in:     "rck23",
			out:    "rck23",
			errExp: false,
		}, {
			in:     "hello🐱kitty",
			out:    "hello_kitty",
			errExp: false,
		}, {
			in:     "1be00fd3-d435-436f-be20-561418c62762",
			out:    "LS_1be00fd3-d435-436f-be20-561418c62762",
			errExp: false,
		}, {
			in:     "b1e00fd3-d435-436f-be20-561418c62762",
			out:    "b1e00fd3-d435-436f-be20-561418c62762",
			errExp: false,
		}, {
			in:     "abcdefghijklmnopqrstuvwyzABCDEFGHIJKLMNOPQRSTUVWXYZ_______", // 49
			out:    "should fail",
			errExp: true,
		},
	}

	for _, test := range unitTests {
		resName, err := linstorifyResourceName(test.in)
		if test.errExp && err == nil {
			t.Fatalf("Expected that rest '%s' returns an error\n", test.in)
		} else if !test.errExp && err != nil {
			t.Fatalf("Expected that rest '%s' does not return an error\n", test.in)
		} else if test.errExp && err != nil {
			continue
		}

		if resName != test.out {
			t.Fatalf("Expected that input '%s' transforms to '%s', but got '%s'\n", test.in, test.out, resName)
		}
	}

}
