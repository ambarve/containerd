package snapshots

import (
	"context"
	"os"

	"github.com/containerd/containerd/log"
	"github.com/sirupsen/logrus"
)

// OnErrorDirectoryCleanup removes the directory if given error is nil (i.e *err == nil)
// logs any errors if any.
func OnErrorDirectoryCleanup(ctx context.Context, dirPath string, err *error) {
	if *err != nil {
		if removeErr := os.Remove(dirPath); removeErr != nil {
			log.G(ctx).WithFields(logrus.Fields{
				"cleanup dir path": dirPath,
				"original error":   *err,
				"cleanup error":    removeErr,
			}).Warn("error while cleaning up after failure")
		}
	}
}
