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
	"bufio"
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"

	"github.com/Microsoft/hcsshim"
	"golang.org/x/sys/windows"
)

// applyWindowsCimLayer applies a tar stream of an OCI style diff tar of a Windows
// layer using the hcsshim cim layer writer.
func applyWindowsCimLayer(ctx context.Context, root string, tr *tar.Reader, options ApplyOptions) (size int64, err error) {
	home, id := filepath.Split(root)
	info := hcsshim.DriverInfo{
		HomeDir: home,
	}

	w, err := hcsshim.NewCimLayerWriter(info, id, options.Parents)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err2 := w.Close(ctx); err2 != nil {
			// This error should not be discarded as a failure here
			// could result in an invalid layer on disk
			if err == nil {
				err = err2
			}
		}
	}()
	// TODO(ambarve): figure out why buffered io fails for multi-layer images
	buf := bufio.NewWriter(w)
	defer buf.Flush()
	hdr, nextErr := tr.Next()
	// Iterate through the files in the archive.
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		if nextErr == io.EOF {
			// end of tar archive
			break
		}
		if nextErr != nil {
			return 0, nextErr
		}

		// Note: path is used instead of filepath to prevent OS specific handling
		// of the tar path
		base := path.Base(hdr.Name)
		if strings.HasPrefix(base, whiteoutPrefix) {
			dir := path.Dir(hdr.Name)
			originalBase := base[len(whiteoutPrefix):]
			originalPath := path.Join(dir, originalBase)
			if err := w.Remove(filepath.FromSlash(originalPath)); err != nil {
				return 0, err
			}
			hdr, nextErr = tr.Next()
		} else if hdr.Typeflag == tar.TypeLink {
			err := w.AddLink(filepath.FromSlash(hdr.Name), filepath.FromSlash(hdr.Linkname))
			if err != nil {
				return 0, err
			}
			hdr, nextErr = tr.Next()
		} else {
			var sddl []byte
			var eadata []byte
			var reparse []byte
			name, fileSize, fileInfo, err := fileInfoFromHeader(hdr)
			if err != nil {
				return 0, err
			}
			sddl, err = encodeSDDLFromTarHeader(hdr)
			if err != nil {
				return 0, err
			}
			eadata, err = encodeExtendedAttributesFromTarHeader(hdr)
			if err != nil {
				return 0, err
			}
			reparse = encodeReparsePointFromTarHeader(hdr)
			// If reparse point flag is set but reparse buffer is empty remove the flag.
			if (fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT) > 0 && len(reparse) == 0 {
				fileInfo.FileAttributes &^= uint32(windows.FILE_ATTRIBUTE_REPARSE_POINT)
			}
			if err := w.Add(filepath.FromSlash(name), fileInfo, fileSize, sddl, eadata, reparse); err != nil {
				return 0, err
			}
			size += fileSize
			if hdr.Typeflag == tar.TypeReg || hdr.Typeflag == tar.TypeRegA {
				_, err = io.Copy(buf, tr)
				if err != nil {
					return 0, fmt.Errorf("error when copying file data: %s", err)
				}
			}

			// Copy all the alternate data streams and return the next non-ADS header.
			var ahdr *tar.Header
			for {
				ahdr, nextErr = tr.Next()
				if nextErr != nil {
					break
				}
				if ahdr.Typeflag != tar.TypeReg || !strings.HasPrefix(ahdr.Name, hdr.Name+":") {
					hdr = ahdr
					break
				}
				err = w.AddAlternateStream(name, uint64(ahdr.Size))
				if err != nil {
					return 0, err
				}
				_, err = io.Copy(buf, tr)
				if err != nil {
					return 0, err
				}
			}
		}
		buf.Flush()
	}
	return
}
