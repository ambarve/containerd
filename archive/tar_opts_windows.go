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

package archive

import (
	"archive/tar"
	"context"

	"github.com/Microsoft/hcsshim/pkg/ociwclayer"
)

// AsWindowsContainerLayer indicates that the tar stream to apply is that of
// a Windows Container Layer. The caller must be holding SeBackupPrivilege and
// SeRestorePrivilege.
func AsWindowsContainerLayer() ApplyOpt {
	return func(options *ApplyOptions) error {
		options.applyFunc = applyWindowsLayer
		return nil
	}
}

func applyWindowsCimLayer(ctx context.Context, root string, tr *tar.Reader, options ApplyOptions) (size int64, err error) {
	return ociwclayer.ImportCimLayerFromTar(ctx, root, tr, options.Parents)
}

// AsCimContainerLayer indicates that the tar stream to apply is that of a Windows
// Container Layer written in the CIM format.  The caller must be holding
// SeBackupPrivilege and SeRestorePrivilege.
func AsCimContainerLayer() ApplyOpt {
	return func(options *ApplyOptions) error {
		options.applyFunc = applyWindowsCimLayer
		return nil
	}
}
