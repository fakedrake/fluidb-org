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

cd ./cmake-build/
make VERBOSE=5 -j

rm -rf /tmp/fluidb_store
mkdir /tmp/fluidb_store

# Baseline measurements (should be run first because the workload
# removes initial tables)
echo "baseline {" >> /tmp/io_perf.txt
for i in {1..30}; do
    echo "query:${i}" >> /tmp/io_perf.txt
    echo "Running baseline ${i}"
    ./ssb-workload/indiv/indiv_query${i}
done
echo "}" >> /tmp/io_perf.txt

# Main measurements
echo "main {" >> /tmp/io_perf.txt
for i in {1..30}; do
    echo "query:${i}" >> /tmp/io_perf.txt
    echo "Running query ${i}"
    ./ssb-workload/main/query${i}
done
echo "}" >> /tmp/io_perf.txt
