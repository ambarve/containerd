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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Microsoft/hcsshim/pkg/go-runhcs"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/pkg/errors"
)

type lcowSnapshotter struct {
	*windowsSnapshotterBase
	scratchLock sync.Mutex
}

// NewSnapshotter returns a new windows snapshotter
func NewLCOWSnapshotter(ic *plugin.InitContext) (snapshots.Snapshotter, error) {
	ws, err := newWindowsSnapshotter(ic.Root, ic.Config.(*WindowsSnapshotterConfig))
	if err != nil {
		return nil, err
	}
	return &lcowSnapshotter{
		windowsSnapshotterBase: ws,
	}, nil
}

func (l *lcowSnapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return l.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (l *lcowSnapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return l.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

func (l *lcowSnapshotter) lcowMounts(snapshot storage.Snapshot) []mount.Mount {
	marr := l.mounts(snapshot)
	for i := range marr {
		marr[i].Type = "lcow-layer"
	}
	return marr
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (l *lcowSnapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	ctx, t, err := l.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	snapshot, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot mount")
	}
	m := l.lcowMounts(snapshot)
	return m, nil
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (l *lcowSnapshotter) Remove(ctx context.Context, key string) error {
	ctx, t, err := l.ms.TransactionContext(ctx, true)
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

	path := l.getSnapshotDir(id)
	overridePath := l.getResolvedSnapshotDir(id, snInfo)
	renamed := l.getSnapshotDir("rm-" + id)
	if err := os.Rename(path, renamed); err != nil && !os.IsNotExist(err) {
		// Sometimes if there are some open handles to the files (especially VHD)
		// inside the snapshot directory the rename call will return "access
		// denied" or "file is being used by another process" errors.  Just
		// returning that error causes the entire snapshot garbage collection
		// operation to fail. To avoid that we return failed pre-condition error
		// here so that snapshot garbage collection can continue and can cleanup
		// other snapshots.
		return errors.Wrap(errdefs.ErrFailedPrecondition, err.Error())
	}

	if err := t.Commit(); err != nil {
		if err1 := os.Rename(renamed, path); err1 != nil {
			// May cause inconsistent data on disk
			log.G(ctx).WithError(err1).WithField("path", renamed).Errorf("Failed to rename after failed commit")
		}
		return errors.Wrap(err, "failed to commit")
	}

	if path != overridePath {
		if err := os.RemoveAll(overridePath); err != nil {
			// Must be cleaned up, any "rm-*" could be removed if no active transactions
			log.G(ctx).WithError(err).WithField("path", overridePath).Warnf("Failed to remove root filesystem")
		}
	}
	if err := os.RemoveAll(renamed); err != nil && !os.IsNotExist(err) {
		log.G(ctx).WithError(err).Warnf("failed to remove snapshot dir: %s", renamed)
	}

	return nil
}

func (l *lcowSnapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	ctx, t, err := l.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	newSnapshot, snapshotInfo, err := l.createSnapshotCommon(ctx, kind, key, parent, opts)
	if err != nil {
		return nil, err
	}
	defer onErrorDirectoryCleanup(ctx, &err, l.getSnapshotDir(newSnapshot.ID), l.getResolvedSnapshotDir(newSnapshot.ID, snapshotInfo))

	if kind == snapshots.KindActive {
		// IO/disk space optimization
		//
		// We only need one sandbox.vhd for the container. Skip making one for this
		// snapshot if this isn't the snapshot that just houses the final sandbox.vhd
		// that will be mounted as the containers scratch. The key for a snapshot
		// where a layer.vhd will be extracted to it will have the substring `extract-` in it.
		// If this is changed this will also need to be changed.
		//
		// We save about 17MB per layer (if the default scratch vhd size of 20GB is used) and of
		// course the time to copy the vhdx per snapshot.
		if !strings.Contains(key, snapshots.UnpackKeyPrefix) {
			// This is the code path that handles re-using a scratch disk that has already been
			// made/mounted for an LCOW UVM. In the non sharing case, we create a new disk and mount this
			// into the LCOW UVM for every container but there are certain scenarios where we'd rather
			// just mount a single disk and then have every container share this one storage space instead of
			// every container having it's own xGB of space to play around with.
			//
			// This is accomplished by just making a symlink to the disk that we'd like to share and then
			// using ref counting later on down the stack in hcsshim if we see that we've already mounted this
			// disk.
			shareScratch := snapshotInfo.Labels[reuseScratchLabel]
			ownerKey := snapshotInfo.Labels[reuseScratchOwnerKeyLabel]
			snDir := l.getSnapshotDir(newSnapshot.ID)
			if shareScratch == "true" && ownerKey != "" {
				if err = l.handleSharing(ctx, ownerKey, snDir); err != nil {
					return nil, err
				}
			} else {
				var sizeGB int
				if sizeGBstr, ok := snapshotInfo.Labels[rootfsSizeLabel]; ok {
					i64, _ := strconv.ParseInt(sizeGBstr, 10, 32)
					sizeGB = int(i64)
				}

				scratchSource, err := l.openOrCreateScratch(ctx, sizeGB)
				if err != nil {
					return nil, err
				}
				defer scratchSource.Close()

				// Create the sandbox.vhdx for this snapshot from the cache
				destPath := filepath.Join(snDir, "sandbox.vhdx")
				dest, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE, 0700)
				if err != nil {
					return nil, errors.Wrap(err, "failed to create sandbox.vhdx in snapshot")
				}
				defer dest.Close()
				if _, err := io.Copy(dest, scratchSource); err != nil {
					dest.Close()
					os.Remove(destPath)
					return nil, errors.Wrap(err, "failed to copy cached scratch.vhdx to sandbox.vhdx in snapshot")
				}
			}
		}
	}

	if err := t.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit failed")
	}

	return l.lcowMounts(newSnapshot), nil
}

func (l *lcowSnapshotter) handleSharing(ctx context.Context, id, snDir string) error {
	var key string
	if err := l.Walk(ctx, func(ctx context.Context, info snapshots.Info) error {
		if strings.Contains(info.Name, id) {
			key = info.Name
		}
		return nil
	}); err != nil {
		return err
	}

	mounts, err := l.Mounts(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to get mounts for owner snapshot")
	}

	sandboxPath := filepath.Join(mounts[0].Source, "sandbox.vhdx")
	linkPath := filepath.Join(snDir, "sandbox.vhdx")
	if _, err := os.Stat(sandboxPath); err != nil {
		return errors.Wrap(err, "failed to find sandbox.vhdx in snapshot directory")
	}

	// We've found everything we need, now just make a symlink in our new snapshot to the
	// sandbox.vhdx in the scratch we're asking to share.
	if err := os.Symlink(sandboxPath, linkPath); err != nil {
		return errors.Wrap(err, "failed to create symlink for sandbox scratch space")
	}
	return nil
}

func (l *lcowSnapshotter) openOrCreateScratch(ctx context.Context, sizeGB int) (_ *os.File, err error) {
	// Create the scratch.vhdx cache file if it doesn't already exit.
	l.scratchLock.Lock()
	defer l.scratchLock.Unlock()

	vhdFileName := "scratch.vhdx"
	if sizeGB > 0 {
		vhdFileName = fmt.Sprintf("scratch_%d.vhdx", sizeGB)
	}

	scratchFinalPath := filepath.Join(l.root, vhdFileName)

	scratchSource, err := os.OpenFile(scratchFinalPath, os.O_RDONLY, 0700)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrapf(err, "failed to open vhd %s for read", vhdFileName)
		}

		log.G(ctx).Debugf("vhdx %s not found, creating a new one", vhdFileName)

		// Golang logic for ioutil.TempFile without the file creation
		r := uint32(time.Now().UnixNano() + int64(os.Getpid()))
		r = r*1664525 + 1013904223 // constants from Numerical Recipes

		scratchTempName := fmt.Sprintf("scratch-%s-tmp.vhdx", strconv.Itoa(int(1e9 + r%1e9))[1:])
		scratchTempPath := filepath.Join(l.root, scratchTempName)

		// Create the scratch
		rhcs := runhcs.Runhcs{
			Debug:     true,
			Log:       filepath.Join(l.root, "runhcs-scratch.log"),
			LogFormat: runhcs.JSON,
			Owner:     "containerd",
		}

		opt := runhcs.CreateScratchOpts{
			SizeGB: sizeGB,
		}

		if err := rhcs.CreateScratchWithOpts(ctx, scratchTempPath, &opt); err != nil {
			os.Remove(scratchTempPath)
			return nil, errors.Wrapf(err, "failed to create '%s' temp file", scratchTempName)
		}
		if err := os.Rename(scratchTempPath, scratchFinalPath); err != nil {
			os.Remove(scratchTempPath)
			return nil, errors.Wrapf(err, "failed to rename '%s' temp file to 'scratch.vhdx'", scratchTempName)
		}
		scratchSource, err = os.OpenFile(scratchFinalPath, os.O_RDONLY, 0700)
		if err != nil {
			os.Remove(scratchFinalPath)
			return nil, errors.Wrap(err, "failed to open scratch.vhdx for read after creation")
		}
	} else {
		log.G(ctx).Debugf("scratch vhd %s was already present. Retrieved from cache", vhdFileName)
	}
	return scratchSource, nil
}
