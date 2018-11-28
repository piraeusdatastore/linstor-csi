package driver

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/LINBIT/linstor-csi/pkg/client"
	"github.com/kubernetes-csi/csi-test/pkg/sanity"
)

func TestDriver(t *testing.T) {
	endpoint := "unix:///tmp/csi.sock"
	node := "fake.node"

	mockStorage := &client.MockStorage{}
	driver, _ := NewDriver(Config{
		Endpoint:    endpoint,
		Node:        node,
		Storage:     mockStorage,
		Assignments: mockStorage,
		LogOut:      os.Stderr,
	})
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
		StagingPath: mntStageDir,
		TargetPath:  mntDir,
		Address:     endpoint,
	}

	// Now call the test suite
	sanity.Test(t, cfg)
}
