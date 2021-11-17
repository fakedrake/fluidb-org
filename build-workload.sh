#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -e

# for i in ssb-workload/*.cpp;
# do
#     echo "Compiling: ${i}"
#     c++ -std=c++2a -g -I ./bama/include/include $i -o -$i.exe
# done


find ssb-workload/ -name '*.cpp' -exec sed -i -e 's|/run/user/1000|/tmp/fluidb_data/|g' -e 's|\("data[0-9]*.dat"\)|"/tmp/fluidb_data/\1"|g' {} \;
cmake -S . -B ./cmake-build
cmake --build ./cmake-build workload
for i in {1..27}; do
    echo "Running query ${i}"
    ./cmake-build/ssb-workload/query${i}
done
