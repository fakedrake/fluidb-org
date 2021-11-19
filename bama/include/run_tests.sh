#!/usr/bin/env bash
set -e

cmake -DCMAKE_CXX_COMPILER=clang++  -S . -B build/
cmake  --build build/
# ./build/test_heap_sort
./build/test_query
