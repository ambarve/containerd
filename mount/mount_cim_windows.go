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

package mount

import (
	"path/filepath"
	"strings"

	"github.com/Microsoft/hcsshim"
	"github.com/pkg/errors"
)

// MountedCimFlag is the flag used in Options for a mount of type `cimfs` to specify
// the volume at which the mounted cim can be accessed.
const MountedCimFlag = "mountedCim="

func isWritableMount(m *Mount) bool {
	for _, option := range m.Options {
		if strings.EqualFold(option, "rw") {
			return true
		}
	}
	return false
}

func GetMountedCim(m *Mount) string {
	for _, option := range m.Options {
		if strings.Contains(option, MountedCimFlag) {
			return strings.TrimPrefix(option, MountedCimFlag)
		}
	}
	return ""
}

func cimMount(m *Mount, target string) (_ string, err error) {
	mountedCim := GetMountedCim(m)
	if m.Source == "" || mountedCim == "" {
		// Nothing to do, this is a view snapshot and cim must already be mounted
		return "", nil
	}

	// This is a scratch layer, activate and prepare that.
	home, srcLayerID := filepath.Split(m.Source)
	di := hcsshim.DriverInfo{
		HomeDir: home,
	}

	if err = hcsshim.ActivateLayer(di, srcLayerID); err != nil {
		return "", errors.Wrapf(err, "failed to activate layer %s", m.Source)
	}
	defer func() {
		if err != nil {
			hcsshim.DeactivateLayer(di, srcLayerID)
		}
	}()
	if err = hcsshim.PrepareLayer(di, srcLayerID, []string{mountedCim}); err != nil {
		return "", errors.Wrapf(err, "failed to prepare layer %s", m.Source)
	}
	return "", nil
}
