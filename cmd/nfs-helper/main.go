package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	if len(os.Args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: nfs-helper <command> [args...]")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	var err error
	switch os.Args[1] {
	case "advertise":
		err = advertise(ctx, os.Args[2:])
	case "generate-ganesha-config":
		err = generateGaneshaConfig(ctx, os.Args[2:])
	case "prepare-device-links":
		err = prepareDeviceLinks(ctx, os.Args[2:])
	case "start-stop-reactor":
		err = startStopReactor(ctx, os.Args[2:])
	default:
		_, _ = fmt.Fprintf(os.Stderr, "Unknown command '%s'\n", os.Args[1])
		os.Exit(1)
	}

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
