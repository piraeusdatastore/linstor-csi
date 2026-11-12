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

package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLogInterceptorStripsSecrets(t *testing.T) {
	t.Parallel()

	req := &csi.DeleteSnapshotRequest{
		SnapshotId: "snapshot-1",
		Secrets: map[string]string{
			"access-key": "REDACTED-TEST-KEY",
			"secret-key": "REDACTED-TEST-SECRET",
		},
	}

	testcases := []struct {
		name       string
		handlerErr error
		level      logrus.Level
	}{
		{name: "request-logged-on-success", handlerErr: nil, level: logrus.DebugLevel},
		{name: "request-logged-on-error", handlerErr: errors.New("delete failed"), level: logrus.ErrorLevel},
	}

	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()

			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)

			interceptor := logInterceptor(logrus.NewEntry(logger))

			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				if tcase.handlerErr != nil {
					return nil, tcase.handlerErr
				}

				return &csi.DeleteSnapshotResponse{}, nil
			}

			_, err := interceptor(t.Context(), req, &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/DeleteSnapshot"}, handler)
			assert.Equal(t, tcase.handlerErr, err)

			require.Len(t, hook.Entries, 1)
			entry := hook.LastEntry()
			assert.Equal(t, tcase.level, entry.Level)

			loggedReq, ok := entry.Data["req"].(string)
			require.True(t, ok)
			assert.Contains(t, loggedReq, "snapshot-1")
			assert.Contains(t, loggedReq, "***stripped***")
			assert.NotContains(t, loggedReq, "REDACTED-TEST-KEY")
			assert.NotContains(t, loggedReq, "REDACTED-TEST-SECRET")
		})
	}
}
