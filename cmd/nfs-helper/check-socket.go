package main

import (
	"context"
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

func checkSocket(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("expected one argument: <resource-name>")
	}

	resource := args[0]

	config, err := readMetadata(resource)
	if err != nil {
		return fmt.Errorf("failed to read resource metadata: %w", err)
	}

	addr := fmt.Sprintf("localhost:%d", config.Port)

	log.WithField("addr", addr).Info("Waiting for NFS socket")

	dialer := &net.Dialer{Timeout: 1 * time.Second}
	for {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			conn.Close()
			log.WithField("addr", addr).Info("NFS socket is ready")

			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for NFS socket on %s: %w", addr, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
