package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"

	"github.com/piraeusdatastore/linstor-csi/pkg/driver"
)

func run(ctx context.Context, args ...string) error {
	switch args[0] {
	case "start-stop-reactor":
		return startStopReactor(ctx, args[1:])
	case "generate-ganesha-config":
		return generateGaneshaConfig(ctx, args[1:])
	case "mount":
		return mount(ctx, args[1:])
	case "prepare-device-links":
		return prepareDeviceLinks(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func main() {
	logOut := os.Stderr
	logFmt := &log.TextFormatter{}
	log.SetFormatter(logFmt)
	log.SetOutput(logOut)

	if len(os.Args) < 2 {
		log.Fatal("usage: nfs-helper <command> [args...])")
	}

	log.WithField("version", driver.Version).WithField("args", os.Args[1:]).Info("Running NFS Helper")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err := run(ctx, os.Args[1:]...)
	if err != nil {
		log.WithError(err).Fatal("NFS helper exited with error")
	}
}
