#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -e

for i in /tmp/fluidb-data/workload-30000/*.cpp;
do
    c++ -std=c++2a -g -I ./bama/include/include $i -o -$i.exe
done
