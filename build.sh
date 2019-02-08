#!/bin/bash

# CDPx treats any output to stderr as a warning, so redirect output from
# "set -x" to stdout instead of stderr.
BASH_XTRACEFD=1
set -eux

export GOOS=windows

# Ensures build outputs are placed in the root source directory, instead of
# in /, so they can be picked up by the artifact step.
cd /source

# Set up GOPATH with a symlink pointing to the actual source location.
mkdir -p $GOPATH/src/github.com/containerd
ln -s /source $GOPATH/src/github.com/containerd/containerd

go build github.com/containerd/containerd/cmd/containerd-shim-runhcs-v1