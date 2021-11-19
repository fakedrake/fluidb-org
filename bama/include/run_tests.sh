#!/usr/bin/env bash
set -e

cmake -S . -B build/
cmake --build build/
# ./build/test_heap_sort
# ./build/test_join
./build/test_query
