#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -xe

# for i in ssb-workload/*.cpp;
# do
#     echo "Compiling: ${i}"
#     c++ -std=c++2a -g -I ./bama/include/include $i -o -$i.exe
# done


# if  [[ ! -d ./cmake-build ]]; then
#     cmake -S . -B ./cmake-build
# else
#     echo "reusing ./cmake-build"
# fi

# cd ./cmake-build/
# make VERBOSE=5 -j

rm -rf /tmp/fluidb_store
mkdir /tmp/fluidb_store

function cpp_build {
    local cpp=$1
    c++ -std=c++2a -g -I ./bama/include/include -DFMT_HEADER_ONLY $cpp -o $(basename $cpp).exe
    echo "$cpp" >> /tmp/io_perf.txt
    ./$(basename $cpp).exe
}

# Baseline measurements
echo "baseline {" >> /tmp/io_perf.txt
for cpp in ./ssb-workload/query-indiv-*; do
    cpp_build $cpp
done
echo "}" >> /tmp/io_perf.txt

# Main measurements
echo "main {" >> /tmp/io_perf.txt
for cpp in ./ssb-workload/query-main-*; do
    cpp_build $cpp
done
echo "}" >> /tmp/io_perf.txt

cp /tmp/io_perf.txt ./ssb-workload/
