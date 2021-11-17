#!/bin/bash
set -e

cmake -S . -B ./cmake-build
cmake --build ./cmake-build
echo "Running query 1"
./cmake-build/bama/include/test_query1
