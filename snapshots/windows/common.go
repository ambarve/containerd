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
	// labelScratchSnapshotLocation is a label provided in snapshotter opts to specify
	// if the scratch snapshot should be stored in a different location specified by
	// this annotations. (Only supported on windows & lcow snapshotter as of now)
	labelScratchSnapshotLocation = "containerd.io/snapshot/io.microsoft.override-scratch"
)

// windowsSnapshotterBase is the base snapshotter for both LCOW & WCOW snapshotters. It provides common
// methods required for both snapshotters.
type windowsSnapshotterBase struct {
	root     string
	ms       *storage.MetaStore
	snConfig *WindowsSnapshotterConfig
}

// WindowsSnapshotterConfig is the configuration related to windows snapshotters (i.e LCOW & WCOW)
type WindowsSnapshotterConfig struct {
	// SnapshotterScratchLocation is the path on the host at which all the container
	// scratch snapshots should be stored. This is useful in cases when we need to
	// keep the scratch layers on a different volume/disk than the image layers
	SnapshotterScratchLocation string `toml:"snapshotter_scratch_location" json:"snapshotterScratchLocation"`
}

// NewSnapshotter returns a new windows snapshotter
func newWindowsSnapshotter(root string, snConfig *WindowsSnapshotterConfig) (*windowsSnapshotterBase, error) {
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
		root:     root,
		ms:       ms,
		snConfig: snConfig,
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
	scratchDir, ok := snInfo.Labels[labelScratchSnapshotLocation]
	if ok {
		return filepath.Join(scratchDir, id)
	}
	return s.getSnapshotDir(id)
}

func (s *windowsSnapshotterBase) parentIDsToParentPaths(parentIDs []string) []string {
	var parentLayerPaths []string
	for _, ID := range parentIDs {
		parentLayerPaths = append(parentLayerPaths, s.getSnapshotDir(ID))
	}
	return parentLayerPaths
}

// OnErrorDirectoryCleanup removes the directories if given error is nil (i.e *err == nil)
// logs any errors if any.
func onErrorDirectoryCleanup(ctx context.Context, err *error, dirPaths ...string) {
	if *err != nil {
		for _, dirPath := range dirPaths {
			if removeErr := os.Remove(dirPath); removeErr != nil && !os.IsNotExist(removeErr) {
				log.G(ctx).WithFields(logrus.Fields{
					"cleanupDir":    dirPath,
					"originalError": *err,
					"cleanupError":  removeErr,
				}).Warn("error while cleaning up after failure")
			}
		}
	}
}

// createSnapshotCommon creates a snapshot in the metadata db with the correct snapshot info.
// It also creates a directory for this snapshot if this is an Active snapshot.
// The context must be a transaction context.
func (s *windowsSnapshotterBase) createSnapshotCommon(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ storage.Snapshot, _ snapshots.Info, err error) {
	newSnapshot, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return storage.Snapshot{}, snapshots.Info{}, errors.Wrap(err, "failed to create snapshot")
	}

	// The snapshot scratch override location could be specified in the containerd.toml or it could
	// be specified in the container config. The one specified in the container config takes preference.
	// Get the correct override location, update that in the snapshot snapshotInfo and save it so that all other
	// operations will use the correct path.
	_, snapshotInfo, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return storage.Snapshot{}, snapshots.Info{}, errors.Wrap(err, "failed to get snapshot info")
	}

	_, ok := snapshotInfo.Labels[labelScratchSnapshotLocation]
	if !ok && !strings.Contains(key, snapshots.UnpackKeyPrefix) {
		// no label provided in container config and this is a scratch snapshot
		if s.snConfig.SnapshotterScratchLocation != "" {
			if snapshotInfo.Labels == nil {
				snapshotInfo.Labels = make(map[string]string)
			}
			snapshotInfo.Labels[labelScratchSnapshotLocation] = s.snConfig.SnapshotterScratchLocation
			snapshotInfo, err = storage.UpdateInfo(ctx, snapshotInfo)
			if err != nil {
				errors.Wrap(err, "failed to write updated info")
			}
		}
	}

	if kind == snapshots.KindActive {
		log.G(ctx).Debug("createSnapshot active")

		// Create the new snapshot dir
		_, _, err := s.createSnapshotDirectory(ctx, snapshotInfo, key, newSnapshot.ID)
		if err != nil {
			return storage.Snapshot{}, snapshots.Info{}, errors.Wrap(err, "failed to create snapshot directory")
		}
	}

	return newSnapshot, snapshotInfo, nil
}

// createSnapshotDirectory creates a directory for the snapshot by correctly handling the
// annotations / configs provided for overriding the location where scratch snapshots
// should be stored.
func (s *windowsSnapshotterBase) createSnapshotDirectory(ctx context.Context, snInfo snapshots.Info, snKey, snID string) (_, _ string, err error) {
	snDir := s.getSnapshotDir(snID)

	// create all parent directories first
	if err = os.MkdirAll(filepath.Dir(snDir), 0700); err != nil {
		return "", "", err
	}

	// Check if a different path was provided for scratch
	snActualDir := s.getResolvedSnapshotDir(snID, snInfo)
	if snActualDir != snDir && !strings.Contains(snKey, snapshots.UnpackKeyPrefix) {
		// Create the new snapshot dir at given path
		log.G(ctx).WithFields(logrus.Fields{
			"snID":           snID,
			"snOverridePath": snActualDir,
		}).Debug("overriding scratch snapshot location")

		if err = os.Mkdir(snActualDir, 0700); err != nil {
			return "", "", err
		}
		defer onErrorDirectoryCleanup(ctx, &err, snActualDir)

		// create a link to the actual snDir in s.root/snapshots directory
		if err = os.Symlink(snActualDir, snDir); err != nil {
			return "", "", err
		}
	} else {
		// Create the new snapshot dir
		if err = os.Mkdir(snDir, 0700); err != nil {
			return "", "", err
		}
	}
	return snDir, snActualDir, nil
}
