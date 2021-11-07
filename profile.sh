#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -e

echo "Building the benchmark"
stack --work-dir .profile-stack-dir/ build -j 4 --profile fluidb:bench:benchmark
echo "Running profiterole"
profiterole benchmark.prof
