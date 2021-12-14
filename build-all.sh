#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -ex

./build.sh
./build-ssb.sh
./build-workload.sh
