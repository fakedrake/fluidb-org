#!/usr/bin/env bash
set -e

cmake -S . -B build/
cmake --build build/
./build/test_heap_sort
