#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -xe

# for i in ssb-workload/*.cpp;
# do
#     echo "Compiling: ${i}"
#     c++ -std=c++2a -g -I ./bama/include/include $i -o -$i.exe
# done


if  [[ ! -d ./cmake-build ]]; then
    cmake -S . -B ./cmake-build
else
    echo "reusing ./cmake-build"
fi

cmake --build ./cmake-build -j 4

rm -rf /tmp/fluidb_store
mkdir /tmp/fluidb_store
for i in {1..27}; do
    echo "Running query ${i}"
    ./cmake-build/ssb-workload/query${i}
done
