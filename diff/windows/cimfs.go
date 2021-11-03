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

	"github.com/Microsoft/hcsshim"
	"github.com/containerd/containerd/archive"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.DiffPlugin,
		ID:   "cimfs",
		Requires: []plugin.Type{
			plugin.MetadataPlugin,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			md, err := ic.Get(plugin.MetadataPlugin)
			if err != nil {
				return nil, err
			}
			// Verify that we are running the OS version supported for cimfs
			if !hcsshim.IsCimfsSupported() {
				return nil, errors.New("host windows version doesn't support cimfs")
			}
			ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())
			return NewCimDiff(md.(*metadata.DB).ContentStore())
		},
	})
}

// cimDiff does filesystem comparison and application
// for cimFS specific layer diffs.
type cimDiff struct {
	store content.Store
}

// NewCimDiff is the Windows CIM container layer implementation
// for comparing and applying filesystem layers
func NewCimDiff(store content.Store) (CompareApplier, error) {
	return cimDiff{
		store: store,
	}, nil
}

// Apply applies the content associated with the provided digests onto the
// provided mounts. Archive content will be extracted and decompressed if
// necessary.
func (c cimDiff) Apply(ctx context.Context, desc ocispec.Descriptor, mounts []mount.Mount, opts ...diff.ApplyOpt) (d ocispec.Descriptor, err error) {
	layer, parentLayerPaths, err := cimMountsToLayerAndParents(mounts)
	if err != nil {
		return emptyDesc, err
	}

	return applyDiffCommon(ctx, c.store, desc, layer, parentLayerPaths, archive.AsCimContainerLayer(), opts...)
}

// Compare creates a diff between the given mounts and uploads the result
// to the content store.
func (c cimDiff) Compare(ctx context.Context, lower, upper []mount.Mount, opts ...diff.Opt) (d ocispec.Descriptor, err error) {
	return emptyDesc, errdefs.ErrNotImplemented
}

func cimMountsToLayerAndParents(mounts []mount.Mount) (string, []string, error) {
	if len(mounts) != 1 {
		return "", nil, errors.Wrap(errdefs.ErrInvalidArgument, "number of mounts should always be 1 for Windows layers")
	}
	mnt := mounts[0]
	if mnt.Type != "cimfs" {
		// This is a special case error. When this is received the diff service
		// will attempt the next differ in the chain which for Windows is the
		// legacy wcow differ that we want.
		return "", nil, errdefs.ErrNotImplemented
	}
	// verify that there is no mounted cim
	if mount.GetMountedCim(&mnt) != "" {
		return "", nil, errors.New("found mounted cim when applying a diff")
	}

	parentLayerPaths, err := mnt.GetParentPaths()
	if err != nil {
		return "", nil, err
	}

	return mnt.Source, parentLayerPaths, nil
}
