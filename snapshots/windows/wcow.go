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
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Microsoft/go-winio/vhd"
	"github.com/Microsoft/hcsshim"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/pkg/errors"
)

type wcowSnapshotter struct {
	*windowsSnapshotterBase
	info hcsshim.DriverInfo
}

// NewSnapshotter returns a new windows snapshotter
func NewWCOWSnapshotter(ic *plugin.InitContext) (snapshots.Snapshotter, error) {
	ws, err := newWindowsSnapshotter(ic.Root, ic.Config.(*WindowsSnapshotterConfig))
	if err != nil {
		return nil, err
	}
	return &wcowSnapshotter{
		info: hcsshim.DriverInfo{
			HomeDir: filepath.Join(ic.Root, "snapshots"),
		},
		windowsSnapshotterBase: ws,
	}, nil
}

func (w *wcowSnapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return w.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (w *wcowSnapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return w.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

func (w *wcowSnapshotter) wcowMounts(snapshot storage.Snapshot) []mount.Mount {
	marr := w.mounts(snapshot)
	for i := range marr {
		marr[i].Type = "windows-layer"
	}
	return marr
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (w *wcowSnapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	ctx, t, err := w.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	snapshot, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot mount")
	}
	return w.wcowMounts(snapshot), nil
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (w *wcowSnapshotter) Remove(ctx context.Context, key string) error {
	ctx, t, err := w.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer t.Rollback()

	id, snInfo, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return errors.Wrapf(errdefs.ErrFailedPrecondition, "failed to get snapshot info: %s", err)
	}

	_, _, err = storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	path := w.getSnapshotDir(id)
	overridePath := w.getResolvedSnapshotDir(id, snInfo)
	renamedID := "rm-" + id
	renamed := w.getSnapshotDir(renamedID)
	if err := os.Rename(path, renamed); err != nil {
		// Sometimes if there are some open handles to the files (especially VHD)
		// inside the snapshot directory the rename call will return "access
		// denied" or "file is being used by another process" errors.  Just
		// returning that error causes the entire snapshot garbage collection
		// operation to fail. To avoid that we return failed pre-condition error
		// here so that snapshot garbage collection can continue and can cleanup
		// other snapshots.
		if os.IsPermission(err) {
			// If permission denied, it's possible that the scratch is still mounted, an
			// artifact after a hard daemon crash for example. Worth a shot to try detaching it
			// before retrying the rename.
			if detachErr := vhd.DetachVhd(filepath.Join(path, "sandbox.vhdx")); detachErr != nil {
				return errors.Wrapf(errdefs.ErrFailedPrecondition, "failed to detach vhd during snapshot cleanup %s: %s", detachErr.Error(), err)

			}
			if rerr := os.Rename(path, renamed); rerr != nil {
				return errors.Wrapf(errdefs.ErrFailedPrecondition, "second rename attempt failed for snapshot %s with error %s", id, rerr)
			}
		} else {
			return errors.Wrap(errdefs.ErrFailedPrecondition, err.Error())
		}

	}

	if err := t.Commit(); err != nil {
		if err1 := os.Rename(renamed, path); err1 != nil {
			// May cause inconsistent data on disk
			log.G(ctx).WithError(err1).Errorf("failed to undo rename after failed commit")
		}
		return errors.Wrap(err, "failed to commit")
	}

	drInfo := w.info
	destroyID := renamedID
	if path != overridePath {
		drInfo.HomeDir = filepath.Dir(overridePath)
		// We don't renamed the override directory, so pass the actual ID in that case
		destroyID = id
	}
	if err := hcsshim.DestroyLayer(drInfo, destroyID); err != nil {
		// Must be cleaned up, any "rm-*" could be removed if no active transactions
		log.G(ctx).WithError(err).WithField("path", renamed).Warnf("Failed to remove root filesystem")
	}
	if err := os.RemoveAll(renamed); err != nil && !os.IsNotExist(err) {
		log.G(ctx).WithError(err).Warnf("failed to remove snapshot dir %s", renamed)
	}

	return nil
}

func (w *wcowSnapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	ctx, t, err := w.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	newSnapshot, snapshotInfo, err := w.createSnapshotCommon(ctx, kind, key, parent, opts)
	if err != nil {
		return nil, err
	}
	defer onErrorDirectoryCleanup(ctx, &err, w.getSnapshotDir(newSnapshot.ID), w.getResolvedSnapshotDir(newSnapshot.ID, snapshotInfo))

	if kind == snapshots.KindActive {
		// IO/disk space optimization
		//
		// We only need one sandbox.vhdx for the container. Skip making one for this
		// snapshot if this isn't the snapshot that just houses the final sandbox.vhd
		// that will be mounted as the containers scratch. Currently the key for a snapshot
		// where a layer will be extracted to will have the string `extract-` in it.
		if !strings.Contains(key, snapshots.UnpackKeyPrefix) {
			parentLayerPaths := w.parentIDsToParentPaths(newSnapshot.ParentIDs)

			var parentPath string
			if len(parentLayerPaths) != 0 {
				parentPath = parentLayerPaths[0]
			}

			if err := hcsshim.CreateSandboxLayer(w.info, newSnapshot.ID, parentPath, parentLayerPaths); err != nil {
				return nil, errors.Wrap(err, "failed to create sandbox layer")
			}

			var sizeGB int
			if sizeGBstr, ok := snapshotInfo.Labels[rootfsSizeLabel]; ok {
				i32, err := strconv.ParseInt(sizeGBstr, 10, 32)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to parse label %q=%q", rootfsSizeLabel, sizeGBstr)
				}
				sizeGB = int(i32)
			}

			if sizeGB > 0 {
				const gbToByte = 1024 * 1024 * 1024
				if err := hcsshim.ExpandSandboxSize(w.info, newSnapshot.ID, uint64(gbToByte*sizeGB)); err != nil {
					return nil, errors.Wrapf(err, "failed to expand scratch size to %d GB", sizeGB)
				}
			}
		}
	}

	if err := t.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit failed")
	}

	return w.wcowMounts(newSnapshot), nil
}
