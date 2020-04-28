#!/bin/bash

# CDPx treats any output to stderr as a warning, so redirect output from
# "set -x" to stdout instead of stderr.
BASH_XTRACEFD=1
set -eux

export GOOS=windows

mkdir -p /source/cdpx-artifacts
cd /source/cdpx-artifacts

# Set up GOPATH with a symlink pointing to the actual source location.
mkdir -p $GOPATH/src/github.com/containerd
ln -s /source $GOPATH/src/github.com/containerd/containerd

go build github.com/containerd/containerd/cmd/containerd-shim-runhcs-v1