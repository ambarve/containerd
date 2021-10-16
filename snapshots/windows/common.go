// +build windows

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package windows

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	winfs "github.com/Microsoft/go-winio/pkg/fs"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	rootfsSizeLabel           = "containerd.io/snapshot/io.microsoft.container.storage.rootfs.size-gb"
	reuseScratchLabel         = "containerd.io/snapshot/io.microsoft.container.storage.reuse-scratch"
	reuseScratchOwnerKeyLabel = "containerd.io/snapshot/io.microsoft.owner.key"
)

// windowsSnapshotterBase is the base snapshotter for both LCOW & WCOW snapshotters. It provides common
// methods required for both snapshotters.
type windowsSnapshotterBase struct {
	root string
	ms   *storage.MetaStore
}

// NewSnapshotter returns a new windows snapshotter
func newWindowsSnapshotter(root string) (*windowsSnapshotterBase, error) {
	fsType, err := winfs.GetFileSystemType(root)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(fsType) != "ntfs" {
		return nil, errors.Wrapf(errdefs.ErrInvalidArgument, "%s is not on an NTFS volume - only NTFS volumes are supported", root)
	}

	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	return &windowsSnapshotterBase{
		root: root,
		ms:   ms,
	}, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (s *windowsSnapshotterBase) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()

	_, info, _, err := storage.GetInfo(ctx, key)
	return info, err
}

func (s *windowsSnapshotterBase) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (s *windowsSnapshotterBase) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}
	id, info, usage, err := storage.GetInfo(ctx, key)
	t.Rollback() // transaction no longer needed at this point.

	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		path := s.getResolvedSnapshotDir(id, info)
		du, err := fs.DiskUsage(ctx, path)
		if err != nil {
			return snapshots.Usage{}, err
		}

		usage = snapshots.Usage(du)
	}

	return usage, nil
}

func (s *windowsSnapshotterBase) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	ctx, t, err := s.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	// grab the existing id
	id, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	usage, err := fs.DiskUsage(ctx, s.getResolvedSnapshotDir(id, info))
	if err != nil {
		return err
	}

	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}
	return t.Commit()
}

// Walk the committed snapshots.
func (s *windowsSnapshotterBase) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	ctx, t, err := s.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()

	return storage.WalkInfo(ctx, fn, fs...)
}

// Close closes the snapshotter
func (s *windowsSnapshotterBase) Close() error {
	return s.ms.Close()
}

func (s *windowsSnapshotterBase) mounts(sn storage.Snapshot) []mount.Mount {
	var (
		roFlag           string
		source           string
		parentLayerPaths []string
	)

	if sn.Kind == snapshots.KindView {
		roFlag = "ro"
	} else {
		roFlag = "rw"
	}

	if len(sn.ParentIDs) == 0 || sn.Kind == snapshots.KindActive {
		source = s.getSnapshotDir(sn.ID)
		parentLayerPaths = s.parentIDsToParentPaths(sn.ParentIDs)
	} else {
		source = s.getSnapshotDir(sn.ParentIDs[0])
		parentLayerPaths = s.parentIDsToParentPaths(sn.ParentIDs[1:])
	}

	// error is not checked here, as a string array will never fail to Marshal
	parentLayersJSON, _ := json.Marshal(parentLayerPaths)
	parentLayersOption := mount.ParentLayerPathsFlag + string(parentLayersJSON)

	var mounts []mount.Mount
	mounts = append(mounts, mount.Mount{
		Source: source,
		Options: []string{
			roFlag,
			parentLayersOption,
		},
	})

	return mounts
}

func (s *windowsSnapshotterBase) getSnapshotDir(id string) string {
	return filepath.Join(s.root, "snapshots", id)
}

func (s *windowsSnapshotterBase) getResolvedSnapshotDir(id string, snInfo snapshots.Info) string {
	scratchDir, ok := snInfo.Labels[snapshots.LabelScratchSnapshotLocation]
	if ok {
		return filepath.Join(scratchDir, id)
	}
	return filepath.Join(s.root, "snapshots", id)
}

func (s *windowsSnapshotterBase) parentIDsToParentPaths(parentIDs []string) []string {
	var parentLayerPaths []string
	for _, ID := range parentIDs {
		parentLayerPaths = append(parentLayerPaths, s.getSnapshotDir(ID))
	}
	return parentLayerPaths
}

// OnErrorDirectoryCleanup removes the directory if given error is nil (i.e *err == nil)
// logs any errors if any.
func onErrorDirectoryCleanup(ctx context.Context, dirPath string, err *error) {
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

func (s *windowsSnapshotterBase) createSnapshotDirectory(ctx context.Context, snInfo snapshots.Info, snKey, snID string) (_, _ string, err error) {
	snDir := s.getSnapshotDir(snID)

	// create all parent directories first
	if err = os.MkdirAll(filepath.Dir(snDir), 0700); err != nil {
		return "", "", err
	}

	// Check if a different path was provided for scratch
	snActualDir := ""
	scratchDir, ok := snInfo.Labels[snapshots.LabelScratchSnapshotLocation]
	if ok && !strings.Contains(snKey, snapshots.UnpackKeyPrefix) {
		// Create the new snapshot dir at given path
		log.G(ctx).WithFields(logrus.Fields{
			"snapshot id":                    snID,
			"snapshot scratch override path": scratchDir,
		}).Debug("overriding scratch snapshot location")

		snActualDir = filepath.Join(scratchDir, snID)
		if err = os.Mkdir(snActualDir, 0700); err != nil {
			return "", "", err
		}
		defer onErrorDirectoryCleanup(ctx, snActualDir, &err)

		// create a link to the actual snDir in s.root/snapshots directory
		if err := os.Symlink(snActualDir, snDir); err != nil {
			return "", "", err
		}
	} else {
		// Create the new snapshot dir
		if err := os.Mkdir(snDir, 0700); err != nil {
			return "", "", err
		}
	}
	return snDir, snActualDir, nil
}
