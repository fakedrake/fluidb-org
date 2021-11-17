#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -xe

# for i in ssb-workload/*.cpp;
# do
#     echo "Compiling: ${i}"
#     c++ -std=c++2a -g -I ./bama/include/include $i -o -$i.exe
# done



cmake -S . -B ./cmake-build
cmake --build ./cmake-build
rm -r /tmp/fluidb-data/*
for i in {1..27}; do
    echo "Running query ${i}"
    ./cmake-build/ssb-workload/query${i}
done
