#!/bin/bash

# Script to decompress the data tarball
# from our Google Drive link. We run in a docker
# container to so we can use zstd compression
# without needing it installed.
#
# Note: this may require root privileges if Docker isn't
# configured as rootless.


docker run -v "$(pwd):/workspace" ubuntu:24.04 /bin/bash -c "apt update && apt install -y zstd tar && cd /workspace; tar -xvf data.tar.zst"
