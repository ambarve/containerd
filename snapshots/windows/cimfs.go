//go:build windows
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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Microsoft/hcsshim"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"
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
	legacySn *wcowSnapshotter
	// cimDir is the path to the directory which holds all of the layer cim files.
	// CimFS needs all the layer cim files to be present in the same directory hence
	// cim files of all the snapshots (even if they are of different images) will be
	// kept in the same directory.
	cimDir string
	// mount manager to manage cimfs mounts
	cmm *cimfsMountManager
}

const (
	// These labels shouldn't be inherited, they are specific to a
	// particular snapshot and they include the information about the mounted cim
	// represented by that snapshot so we don't want the child snapshot of this
	// snapshot to inherit these labels. Hence, don't prefix them with
	// `containerd.io/snapshot`.

	// This label is used to store the current ref count of the mounted cim of this snapshot.
	mountedRefCountLabel = "containerd.io/snapshot/cimfs/refcount"

	// This label is used to store the volume at which the cim of this snapshot is mounted.
	mountedCimVolumeLabel = "containerd.io/snapshot/cimfs/mountedvolume"
)

var (
	ErrCimNotMounted = errors.New("cim is not mounted")
)

// NewSnapshotter returns a new windows snapshotter
func NewCimfsSnapshotter(ic *plugin.InitContext) (snapshots.Snapshotter, error) {
	if !hcsshim.IsCimfsSupported() {
		return nil, errors.Errorf("cimfs not supported on this version of windows")
	}

	baseSn, err := newWindowsSnapshotter(ic.Root, ic.Config.(*WindowsSnapshotterConfig))
	if err != nil {
		return nil, err
	}

	ls := &wcowSnapshotter{
		info: hcsshim.DriverInfo{
			HomeDir: filepath.Join(ic.Root, "snapshots"),
		},
		windowsSnapshotterBase: baseSn,
	}

	// Abort if takes more than 1 second as this will hang at containerd startup.
	ctx, _ := context.WithTimeout(context.TODO(), 1*time.Second)
	ctx, t, err := ls.windowsSnapshotterBase.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	mountManagerMap, err := loadMountedCimInfo(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "cimfs snapshot initialization failed while reading mounted cim info")
	}

	return &cimfsSnapshotter{
		legacySn: ls,
		cimDir:   filepath.Join(ls.info.HomeDir, "cim-layers"),
		cmm:      newCimfsMountManager(filepath.Join(ls.info.HomeDir, "cim-layers"), ls.ms, mountManagerMap),
	}, nil
}

// checkIfVolumeExists checks if the provided `volumePath` (in the form "\\?\Volume{<GUID>}\") actually represents a volume on the system. Returns true if such a volume is found, false otherwise.
func checkIfVolumeExists(volumePath string) (bool, error) {
	volumeStrUtf16, err := windows.UTF16FromString(volumePath)
	if err != nil {
		return false, errors.Wrap(err, "failed to convert volume path to UTF16")
	}
	if err := windows.GetVolumeInformation(&volumeStrUtf16[0], nil, 0, nil, nil, nil, nil, 0); err != nil {
		return false, errors.Wrap(err, "failed to get volume information")
	}
	return true, nil
}

// loadMountedCimInfo goes over all the snapshots in the metadata stores and reads the cimfs mount labels
// from them. It returns a map which contains the information of these mounted cims.
// Expects a storage transaction context.
func loadMountedCimInfo(ctx context.Context) (map[string]*mountedCimInfo, error) {
	mountManagerMap := make(map[string]*mountedCimInfo)
	err := storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
		if info.Labels == nil {
			return nil
		}

		refCountStr, hasRefCount := info.Labels[mountedRefCountLabel]
		mountedVolume, hasMountedVolume := info.Labels[mountedCimVolumeLabel]

		if hasRefCount && hasMountedVolume {
			refCount, err := strconv.ParseUint(refCountStr, 10, 32)
			if err != nil {
				return errors.Wrapf(err, "fetchCimMountInfo failed to parse refCount value %s", refCountStr)
			}

			// verify if this volume is still mounted.
			if exists, err := checkIfVolumeExists(mountedVolume); !exists {
				log.G(ctx).WithFields(logrus.Fields{
					"snapshot key":       info.Name,
					"ref count":          refCount,
					"mounted volume":     mountedVolume,
					"volume check error": err,
				}).Trace("skipping mounted cim info as volume check failed.")
				return nil
			}

			mountManagerMap[info.Name] = &mountedCimInfo{
				snapshotKey: info.Name,
				refCount:    uint32(refCount),
				mountedPath: mountedVolume,
			}

			log.G(ctx).WithFields(logrus.Fields{
				"snapshot key":   info.Name,
				"ref count":      refCount,
				"mounted volume": mountedVolume,
			}).Trace("loaded mounted cim info")
		}
		return nil
	})

	// on fresh start Walk function will throw `bucket does not exist` error because
	// there are no buckets in the DB yet. Ignore that error.
	if err != nil && !strings.Contains(err.Error(), "bucket does not exist") {
		return nil, errors.Wrap(err, "cimfs snapshotter init failed")
	}
	return mountManagerMap, nil
}

// isScratchLayer returns true if this snapshot will be a read-only parent layer
// returns false otherwise.
// In case of image layer snapshots this will determined by looking at the UnpackKeyPrefix
// option present in the snapshot opts.
func isScratchLayer(key string) bool {
	return !strings.Contains(key, snapshots.UnpackKeyPrefix)
}

// getCimLayerPath returns the path of the cim file for the given snapshot. Note that this function
// doesn't actually check if the cim layer exists it simply does string manipulation to generate the path
// isCimLayer can be used to verify if it is actually a cim layer.
func getCimLayerPath(cimDir, snID string) string {
	return filepath.Join(cimDir, (snID + ".cim"))
}

// isCimLayer checks if the snapshot referred by the given key is actually a cim layer.
// With cimfs snapshotter all the read-only (i.e image) layers are stored in the cim format
// while the scratch layers (or writable layers) are same as standard windows snapshotter
// scratch layers.
func (s *cimfsSnapshotter) isCimLayer(ctx context.Context, key string) (bool, error) {
	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "failed to get snapshot info")
	}
	snCimPath := getCimLayerPath(s.cimDir, id)
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

	ctx, t, err := s.legacySn.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}
	defer t.Rollback()

	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Usage{}, errors.Wrap(err, "failed to get snapshot info")
	}

	if ok, err := s.isCimLayer(ctx, key); err != nil {
		return snapshots.Usage{}, err
	} else if ok {
		cimUsage, err := hcsshim.GetCimUsage(getCimLayerPath(s.cimDir, id))
		if err != nil {
			return snapshots.Usage{}, err
		}
		legacyUsage.Size += int64(cimUsage)
	}
	return legacyUsage, nil
}

func (s *cimfsSnapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	m, err := s.legacySn.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
	if err != nil {
		return m, err
	}
	return s.createCimfsMounts(ctx, m, key, opts...)
}

func (s *cimfsSnapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	m, err := s.legacySn.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
	if err != nil {
		return m, err
	}
	return s.createCimfsMounts(ctx, m, key, opts...)
}

// createCimfsMounts creates cimfs snapshotter mounts from the legacy mounts. This
// function will also mount a parent cim if required. It will persist the information of
// mounted cims in the metadata DB so that if containerd restarts those cims can be
// properly cleaned up.
func (s *cimfsSnapshotter) createCimfsMounts(ctx context.Context, m []mount.Mount, key string, opts ...snapshots.Opt) (_ []mount.Mount, retErr error) {
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
		return m, errors.Wrap(err, "createCimfsMounts failed to get snapshot")
	}

	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return m, errors.Wrap(err, "createCimfsMounts failed to get snapshot info")
	}

	if sn.Kind == snapshots.KindView || isScratchLayer(key) {
		// mount the parent cim.
		mountedLocation, err := s.cmm.mountSnapshot(ctx, info.Parent)
		if err != nil {
			return m, errors.Wrap(err, "createCimfsMounts failed to mount snapshot")
		}

		defer func() {
			if retErr != nil {
				if err := s.cmm.unmountSnapshot(ctx, mountedLocation); err != nil {
					log.G(ctx).WithError(retErr).Warnf("cimfs cleanup on failure during create cimfs mounts failed: %s", err)
				}
			}
		}()

		m[0].Options = append(m[0].Options, mount.MountedCimFlag+mountedLocation)
		if sn.Kind == snapshots.KindView {
			m[0].Source = ""
		}
	}

	if err = t.Commit(); err != nil {
		return m, err
	}

	return m, nil

}

// Mounts returns the cimfs mounts for the snapshot identified by key.
//
// This can be used to recover mounts after calling View or Prepare.
func (s *cimfsSnapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	ctx, t, err := s.legacySn.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	sn, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "mounts failed to get snapshot")
	}

	_, snInfo, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "mounts failed to get snapshot info")
	}

	mounts, err := s.legacySn.mounts(sn), nil
	if err != nil {
		return nil, err
	}

	if len(mounts) != 1 {
		return mounts, errors.Errorf("expected exactly 1 mount from legacy windows snapshotter, found %d", len(mounts))
	}

	mounts[0].Type = "cimfs"

	if sn.Kind == snapshots.KindView || isScratchLayer(key) {
		mountedLocation, err := s.cmm.getCimMountPath(snInfo.Parent)
		if err != nil {
			return mounts, errors.Wrap(err, "failed to get parent cim mount location")
		}
		mounts[0].Options = append(mounts[0].Options, mount.MountedCimFlag+mountedLocation)
		if sn.Kind == snapshots.KindView {
			mounts[0].Source = ""
		}
	}
	return mounts, nil
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
	if ok, err := s.isCimLayer(ctx, key); err != nil {
		return err
	} else if ok {
		cimUsage, err := hcsshim.GetCimUsage(getCimLayerPath(s.cimDir, id))
		if err != nil {
			return err
		}
		usage.Size += int64(cimUsage)
	}

	// TODO(ambarve): When committing a scratch layer we must write it to cimfs. Add
	// that logic.

	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}
	return t.Commit()
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (s *cimfsSnapshotter) Remove(ctx context.Context, key string) error {
	ctx, t, err := s.legacySn.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer t.Rollback()

	// Get info before calling legacySn.Remove
	id, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to get snapshot info")
	}

	if info.Kind == snapshots.KindActive {
		// unmount the parent cim
		if err := s.cmm.unmountSnapshot(ctx, info.Parent); err != nil {
			if !errors.Is(err, ErrCimNotMounted) {
				return errors.Wrap(err, "failed to unmount cim")
			}
		}
	}

	if isCimLayer, err := s.isCimLayer(ctx, key); err != nil {
		return errors.Wrap(err, "failed to detect if this is a cim layer")
	} else if isCimLayer {
		if s.cmm.inUse(key) {
			return errors.Errorf("can't remove snapshot when it is being used")
		}

		// unmount this cim first
		if err := s.cmm.unmountSnapshot(ctx, key); err != nil {
			if !errors.Is(err, ErrCimNotMounted) {
				return errors.Wrap(err, "failed to unmount cim")
			}
		}

		if err := hcsshim.DestroyCimLayer(s.legacySn.info, id); err != nil {
			return err
		}
	}
	// Must rollback this transaction before calling legacySn.Remove since that function opens its
	// own transaction.
	t.Rollback()

	return s.legacySn.Remove(ctx, key)

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
	// Key of the snapshot
	snapshotKey string
	// ref count for number of times this cim was mounted.
	refCount uint32
	// mounted volume i.e the volume at which this mounted cim can be accessed.
	mountedPath string
}

// A default mount manager that maintain mounts of cimfs snapshotter
// based snapshots. cimfsMountManager uses ref counting to decide
// how to mount / unmount snapshots but it can be replaced with
// some other policies.
type cimfsMountManager struct {
	hostCimMounts map[string]*mountedCimInfo
	mountMapLock  sync.Mutex
	// path to the directory inside which all cim layers are stored.
	cimDir string
	// metadata store in which information of mounted cim will be persisted.
	ms *storage.MetaStore
}

func newCimfsMountManager(cimDir string, metaStore *storage.MetaStore, initMap map[string]*mountedCimInfo) *cimfsMountManager {
	if initMap == nil {
		initMap = make(map[string]*mountedCimInfo)
	}
	return &cimfsMountManager{
		hostCimMounts: initMap,
		cimDir:        cimDir,
		ms:            metaStore,
	}
}

// mountSnapshot takes the key of a snapshot and mounts the cim associated with it.
// the context needs to be a transaction context.
func (cm *cimfsMountManager) mountSnapshot(ctx context.Context, snKey string) (_ string, retErr error) {
	cm.mountMapLock.Lock()
	defer cm.mountMapLock.Unlock()

	snID, snInfo, _, err := storage.GetInfo(ctx, snKey)
	if err != nil {
		return "", errors.Wrap(err, "mount snapshot cim failed to get snapshot info")
	}

	if _, ok := cm.hostCimMounts[snKey]; !ok {
		mountPath, err := hcsshim.MountCim(getCimLayerPath(cm.cimDir, snID))
		if err != nil {
			return "", errors.Wrap(err, "failed to mount cim")
		}
		cm.hostCimMounts[snKey] = &mountedCimInfo{snapshotKey: snKey, mountedPath: mountPath}

		log.G(ctx).WithFields(logrus.Fields{
			"snapshot key":   snKey,
			"snapshot ID":    snID,
			"mounted volume": mountPath,
		}).Trace("mounted snapshot cim")

		defer func() {
			if retErr != nil {
				if unmountErr := hcsshim.UnmountCimLayer(mountPath); unmountErr != nil {
					log.G(ctx).WithError(retErr).Errorf("unmount snapshot cim failed with: %s", unmountErr)
				}
				delete(cm.hostCimMounts, snKey)
			}
		}()
	}

	ci := cm.hostCimMounts[snKey]
	if snInfo.Labels == nil {
		snInfo.Labels = make(map[string]string)
	}
	snInfo.Labels[mountedCimVolumeLabel] = ci.mountedPath
	snInfo.Labels[mountedRefCountLabel] = fmt.Sprintf("%d", ci.refCount+1)

	if _, err := storage.UpdateInfo(ctx, snInfo); err != nil {
		return "", errors.Wrap(err, "mount snapshot info failed while writing to metastore.")
	}

	// We actually want to update the refcount here and not before the storage. UpdateInfo call so that
	// if storage.UpdateInfo call fails we don't increase the count unnecessarily.
	ci.refCount += 1

	return ci.mountedPath, nil
}

func (cm *cimfsMountManager) unmountSnapshot(ctx context.Context, snKey string) error {
	cm.mountMapLock.Lock()
	defer cm.mountMapLock.Unlock()
	ci, ok := cm.hostCimMounts[snKey]
	if !ok {
		return ErrCimNotMounted
	}

	snID, snInfo, _, err := storage.GetInfo(ctx, snKey)
	if err != nil {
		return errors.Wrap(err, "unmount snapshot cim failed to get snapshot info")
	}

	if ci.refCount == 1 {
		if err := hcsshim.UnmountCimLayer(ci.mountedPath); err != nil {
			return err
		}

		log.G(ctx).WithFields(logrus.Fields{
			"snapshot key":   snKey,
			"snapshot ID":    snID,
			"mounted volume": ci.mountedPath,
		}).Trace("unmounted snapshot cim")

		delete(cm.hostCimMounts, snKey)
		delete(snInfo.Labels, mountedCimVolumeLabel)
		delete(snInfo.Labels, mountedRefCountLabel)

	} else {
		ci.refCount -= 1
		snInfo.Labels[mountedCimVolumeLabel] = ci.mountedPath
		snInfo.Labels[mountedRefCountLabel] = fmt.Sprintf("%d", ci.refCount)
	}

	if _, err := storage.UpdateInfo(ctx, snInfo); err != nil {
		return errors.Wrap(err, "unmount snapshot info failed while writing to metastore.")
	}

	return nil
}

// checks if the cim for given snapshot is still mounted
func (cm *cimfsMountManager) inUse(snKey string) bool {
	cm.mountMapLock.Lock()
	defer cm.mountMapLock.Unlock()
	if _, ok := cm.hostCimMounts[snKey]; !ok {
		return false
	}
	return true
}

func (cm *cimfsMountManager) getCimMountPath(snKey string) (string, error) {
	cm.mountMapLock.Lock()
	defer cm.mountMapLock.Unlock()
	ci, ok := cm.hostCimMounts[snKey]
	if !ok {
		return "", ErrCimNotMounted
	}
	return ci.mountedPath, nil
}
