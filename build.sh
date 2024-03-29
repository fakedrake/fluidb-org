#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -ex

# stack build -j 4 fluidb:bench:baseline
stack build -j 4 fluidb:bench:benchmark

# stack build -j 4 fluidb:bench:benchmark
# stack --work-dir .benchmark-stack-dir/ --profile -j 4 run  -- +RTS -p
# profiterole benchmark.prof
# profiteur benchmark.prof

# echo "Graphical overview: $(pwd)/benchmark.prof.html"
# echo "Textual overview: $(pwd)/benchmark.profiterole.txt"
