#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -e

shake run-branches
# stack --work-dir .benchmark-stack-dir/ --profile -j 4 run  -- +RTS -p
# stack --work-dir .benchmark-stack-dir/ --profile -j 4 run  -- +RTS -p
# profiterole benchmark.prof
# profiteur benchmark.prof

# echo "Graphical overview: $(pwd)/benchmark.prof.html"
# echo "Textual overview: $(pwd)/benchmark.profiterole.txt"
