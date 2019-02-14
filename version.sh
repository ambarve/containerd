#!/bin/bash

set -eux

cd /source

gitVersion=$(git describe --match 'v[0-9]*' --always --long)
timeStamp=$(date --utc +%Y%m%d)
version=${gitVersion}-${timeStamp}-${CDP_DEFINITION_BUILD_COUNT}

echo '##vso[build.updatebuildnumber]'$version