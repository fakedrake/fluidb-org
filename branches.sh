#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -e

echo "Building the benchmark"
stack build -j 4 --ghc-options -DVERBOSE_SOLVING fluidb:bench:benchmark 2> /tmp/benchmark.out || true
echo "Running readdump"
rm -r /tmp/benchmark.out.bench_branches
mkdir -p /tmp/benchmark.out.bench_branches
stack run fluidb:exe:readdump
