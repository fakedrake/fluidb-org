#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -e

stack build -j 4 --ghc-options -DVERBOSE_SOLVING fluidb:bench:benchmark 2> /tmp/benchmark.out
mkdir -p /tmp/benchmark.out.bench_branches
stack run fluidb:exe:readdump
