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
	"strings"

	"github.com/Microsoft/go-winio/vhd"
	"github.com/Microsoft/hcsshim"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/mylogger"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
)

// Composite image FileSystem (CimFS) is a new read-only filesystem (similar to unionFS on
// Linux) created specifically for storing container image layers on windows.
// cimfsSnapshotter is a snapshotter that uses CimFS to create read-only parent layer
// snapshots. Each snapshot is represented by a `<snapshot-id>.cim` file and some other
// files which hold contents of that snapshot.  Once a cim file for a layer is created it
// can only be used for reading by mounting it to a volume. Hence, CimFS will not be used
// when we are creating writable layers for container scratch and such. (However, scratch
// layer of a container can be exported to a cim layer and then be used as a parent layer
// for another container).  Since CimFS can not be used for scratch layers we still use
// the existing windows snapshotter to create writable scratch space snapshots.
// TODO(ambarve): Handle exporting a container scratch layer as a read-only parent cim layer.

// The `isReadOnlyParentLayer` function determines if the new snapshot is going to be a
// read-only parent layer or if it is going to be a scratch layer. Based on this
// information we decide whether to create a cim for this snapshot or to use the legacy
// windows snapshotter.

// cimfs snapshots give the best performance (due to caching) if we reuse the mounted cim
// layers. Hence, unlike other snapshotters instead of returning a mount that can be later
// mounted by the caller (or the shim) mounting of cim layers is handled by the
// snapshotter itself.  cimfsSnapshotter decides when to mount & unmount the snapshot cims
// (Currently we use a simple ref counting but that policy can be changed in the future).
// Due to this, the convention for mounts returned by the cimfs snapshotter is also
// different than other snapshotters. The convention is as follows: The `mount.Type` filed
// will always have the value "cimfs" to specify that this is a "cimfs" snapshotter
// mount. `mount.Source` field will have the full path of the scratch layer if this is a
// scratch layer mount and then the `mount.Options` will have the standard
// `parentLayerPaths` field which will contain the paths of all parent layers. But it will
// also include a new option `mountedCim` which will have the path of the mounted parent
// cim (in `\\?\Volume{guid}` format).  If this is a read only mount (e.g. a View snapshot
// on an image layer) then the `mount.Source` filed will be empty while the
// `mount.Options` field will have the `mountedCim` field which will contain the path to
// the mounted parent layer cim. If this is a snapshot created while writing read-only
// image layers then we don't need to mount the parent cims and so the `mount.Source` will
// have the full path of the snapshot and `mountedCim` will not be included. Note that if
// the `mountedCim` option is present then it must be the path of mounted cim of the
// immediate parent layer i.e the mounted location of the parentId[0]'th cim.
type cimfsSnapshotter struct {
	legacySn *legacySnapshotter
	// cimDir is the path to the directory which holds all of the layer cim files.
	// CimFS needs all the layer cim files to be present in the same directory hence
	// cim files of all the snapshots (even if they are of different images) will be
	// kept in the same directory.
	cimDir string
	// mount manager to manage cimfs mounts
	cmm *cimfsMountManager
}

// NewSnapshotter returns a new windows snapshotter
func NewCimfsSnapshotter(root string) (snapshots.Snapshotter, error) {
	if hcsshim.IsCimfsSupported() {
		ls, err := newSnapshotter(root)
		return &cimfsSnapshotter{
			legacySn: ls,
			cimDir:   filepath.Join(ls.info.HomeDir, "cim-layers"),
			cmm:      newCimfsMountManager(),
		}, err
	} else {
		return nil, errors.Errorf("cimfs not supported on this version of windows")
	}
}

// isScratchLayer returns true if this snapshot will be a read-only parent layer
// returns false otherwise.
// In case of image layer snapshots this will determined by looking at the UnpackKeyPrefix
// option present in the snapshot opts.
func isScratchLayer(key string) bool {
	// TODO(ambarve): Use the unpackkeyprefix here after rebase
	return !strings.Contains(key, "extract")
}

// detectSubType detects if this is going to be a read-only image layer (in which case it will
// be written to cimfs), or if this is going to be a r/w scratch layer of an container (in which case
// it will have parent cim layers but legacy windows scratch layers), or if this is going to be
// a read-only (view) snapshot of an existing cim layer.
// func detectSubType(key string, opts ...snapshots.Opt) (string, error) {
// 	if strings.Contains(key,
// }

// getCimLayerPath returns the path of the cim file for the given snapshot. Note that this function
// doesn't actually check if the cim layer exists it simply does string manipulation to generate the path
// isCimLayer can be used to verify if it is actually a cim layer.
func (s *cimfsSnapshotter) getCimLayerPath(snID string) string {
	return filepath.Join(s.cimDir, (snID + ".cim"))
}

// isCimLayer checks if the snapshot referred by the given key is actually a cim layer.
// With cimfs snapshotter all the read-only (i.e image) layers are stored in the cim format
// while the scratch layers (or writable layers) are same as standard windows snapshotter
// scratch layers.
func (s *cimfsSnapshotter) isCimLayer(ctx context.Context, key string) (bool, error) {
	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "failed to get snapshot mount")
	}
	snCimPath := s.getCimLayerPath(id)
	if _, err := os.Stat(snCimPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (s *cimfsSnapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	return s.legacySn.Stat(ctx, key)
}

func (s *cimfsSnapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	return s.legacySn.Update(ctx, info, fieldpaths...)
}

func (s *cimfsSnapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	legacyUsage, err := s.legacySn.Usage(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}
	//TODO(ambarve): combine the usage of legacy snapshotter and the cim files.
	return legacyUsage, nil
}

func (s *cimfsSnapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	m, err := s.legacySn.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
	if err != nil {
		return m, err
	}
	mylogger.LogFmt("forwarding from Prepare key: %s, parent: %s, mount: %+v to toCimfsMounts\n", key, parent, m[0])
	return s.toCimfsMounts(ctx, m, key, opts...)
}

func (s *cimfsSnapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	m, err := s.legacySn.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
	if err != nil {
		return m, err
	}
	mylogger.LogFmt("forwarding from View key: %s, parent: %s, mount: %+v to toCimfsMounts\n", key, parent, m[0])
	return s.toCimfsMounts(ctx, m, key, opts...)
}

func (s *cimfsSnapshotter) toCimfsMounts(ctx context.Context, m []mount.Mount, key string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	if len(m) != 1 {
		return m, errors.Errorf("expected exactly 1 mount from legacy windows snapshotter, found %d", len(m))
	}
	m[0].Type = "cimfs"

	ctx, t, err := s.legacySn.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	sn, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return m, errors.Wrap(err, "failed to get snapshot")
	}

	var mountedLocation string
	if sn.Kind == snapshots.KindView || isScratchLayer(key) {
		// mount the parent cim
		mountedLocation = s.cmm.getCimMountPath(s, sn.ParentIDs[0])
		mylogger.LogFmt("mountedLocation for sn ID: %s is %s\n", sn.ID, mountedLocation)
		if mountedLocation == "" {
			mountedLocation, err = s.cmm.mountSnapshot(s, sn.ParentIDs[0])
			if err != nil {
				return m, errors.Wrap(err, "failed to mount parent snapshot")
			}
		}
		m[0].Options = append(m[0].Options, mount.MountedCimFlag+mountedLocation)
		if sn.Kind == snapshots.KindView {
			m[0].Source = ""
		}
	}
	return m, nil
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (s *cimfsSnapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	lm, err := s.legacySn.Mounts(ctx, key)
	if err != nil {
		return nil, err
	}
	mylogger.LogFmt("forwarding from Mounts key: %s, mount: %+v to toCimfsMounts\n", key, lm[0])
	return s.toCimfsMounts(ctx, lm, key)
}

func (s *cimfsSnapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	ctx, t, err := s.legacySn.ms.TransactionContext(ctx, true)
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
	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	usage, err := fs.DiskUsage(ctx, s.legacySn.getSnapshotDir(id))
	if err != nil {
		return err
	}
	//TODO(ambarve): Add cim files disk usage here
	//TOOD(ambarve): Now when committing a scratch layer we must
	// write it to cimfs. Add that logic.

	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}
	return t.Commit()
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (s *cimfsSnapshotter) Remove(ctx context.Context, key string) error {
	mylogger.LogFmt("cimfs Remove with key: %s\n", key)
	ctx, t, err := s.legacySn.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer t.Rollback()

	isCimLayer, err := s.isCimLayer(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to check for cim layer")
	}

	id, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to get snapshot info")
	}

	mylogger.LogFmt("id: %s, info: %+v, isCimLayer: %t\n", id, info, isCimLayer)

	if info.Kind == snapshots.KindActive {
		// unmount the parent cim
		pid, _, _, err := storage.GetInfo(ctx, info.Parent)
		if err != nil {
			return errors.Wrapf(err, "failed to get info for snapshot: %s", info.Parent)
		}
		if err := s.cmm.unmountSnapshot(s, pid); err != nil {
			if !strings.Contains(err.Error(), "not mounted") {
				return errors.Wrap(err, "failed to unmount cim")
			}
		}
	} else {
		// unmount this cim first
		if err := s.cmm.unmountSnapshot(s, id); err != nil {
			if !strings.Contains(err.Error(), "not mounted") {
				return errors.Wrap(err, "failed to unmount cim")
			}
		}
		if s.cmm.inUse(s, id) {
			return errors.Errorf("can't remove snapshot %s when it is being used", id)
		}
		if err := hcsshim.DestroyCimLayer(s.legacySn.info, id); err != nil {
			return err
		}
	}

	_, _, err = storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	path := s.legacySn.getSnapshotDir(id)
	renamedID := "rm-" + id
	renamed := s.legacySn.getSnapshotDir(renamedID)
	if err := os.Rename(path, renamed); err != nil && !os.IsNotExist(err) {
		if !os.IsPermission(err) {
			return err
		}
		// If permission denied, it's possible that the scratch is still mounted, an
		// artifact after a hard daemon crash for example. Worth a shot to try detaching it
		// before retrying the rename.
		if detachErr := vhd.DetachVhd(filepath.Join(path, "sandbox.vhdx")); detachErr != nil {
			return errors.Wrapf(err, "failed to detach VHD: %s", detachErr)
		}
		if renameErr := os.Rename(path, renamed); renameErr != nil && !os.IsNotExist(renameErr) {
			return errors.Wrapf(err, "second rename attempt following detach failed: %s", renameErr)
		}
	}

	if err := t.Commit(); err != nil {
		if err1 := os.Rename(renamed, path); err1 != nil {
			// May cause inconsistent data on disk
			log.G(ctx).WithError(err1).WithField("path", renamed).Errorf("Failed to rename after failed commit")
		}
		return errors.Wrap(err, "failed to commit")
	}

	if err := hcsshim.DestroyLayer(s.legacySn.info, renamedID); err != nil {
		// Must be cleaned up, any "rm-*" could be removed if no active transactions
		log.G(ctx).WithError(err).WithField("path", renamed).Warnf("Failed to remove root filesystem")
	}

	return nil
}

// Walk the committed snapshots.
func (s *cimfsSnapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return s.legacySn.Walk(ctx, fn, fs...)
}

// Close closes the snapshotter
func (s *cimfsSnapshotter) Close() error {
	return s.legacySn.Close()
}

type mountedCimInfo struct {
	// ID of the snapshot
	snapshotID string
	// ref count for number of times this cim was mounted.
	refCount uint32
}

// A default mount manager that maintain mounts of cimfs snapshotter
// based snapshots. cimfsMountManager uses ref counting to decide
// how to mount / unmount snapshots but it can be replaced with
// some other policies.
type cimfsMountManager struct {
	// hostCimMounts map[string]*mountedCimInfo
}

func newCimfsMountManager() *cimfsMountManager {
	// TODO(ambarve): We probably should save the state of mount manager and restore it
	// if containerd restarts.
	return &cimfsMountManager{
		// hostCimMounts: make(map[string]*mountedCimInfo),
	}
}

func (cm *cimfsMountManager) mountSnapshot(s *cimfsSnapshotter, snID string) (string, error) {
	return hcsshim.MountCim(s.getCimLayerPath(snID))
}

func (cm *cimfsMountManager) unmountSnapshot(s *cimfsSnapshotter, snID string) error {
	return hcsshim.UnmountCimLayer(s.getCimLayerPath(snID))
}

// checks if the cim for given snapshot is still mounted
func (cm *cimfsMountManager) inUse(s *cimfsSnapshotter, snID string) bool {
	if _, err := hcsshim.GetCimMountPath(s.getCimLayerPath(snID)); err != nil {
		if strings.Contains(err.Error(), "not mounted") {
			return false
		}
	}
	return true
}

func (cm *cimfsMountManager) getCimMountPath(s *cimfsSnapshotter, snID string) string {
	mountPath, err := hcsshim.GetCimMountPath(s.getCimLayerPath(snID))
	if err != nil {
		return ""
	}
	return mountPath
}
