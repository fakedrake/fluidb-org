#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -ex

stack build -j 4 fluidb:bench:baseline
stack build -j 4 fluidb:bench:benchmark

rm -rf ./ssb-workload/main/*.cpp
rm -rf ./ssb-workload/indiv/*.cpp

cp /run/user/1000/fluidb-data/workload-main/*.cpp ./ssb-workload/main/
cp /run/user/1000/fluidb-data/workload-indiv/*.cpp ./ssb-workload/indiv/

find ./ssb-workload/ \
     -name '*.cpp' \
     -exec sed -i '' -e 's|/run/user/1000/|/tmp/|g' \
                     {} \;

git commit -a -m "yaguasync"
git push
