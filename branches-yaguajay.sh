#!/bin/bash
set -ex
# Push to git and build haskell on yaguajay then build c++ locally.

git commit -a -m "sync" || true
git push
# ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && ./branches.sh'"
rm -r /tmp/benchmark.out.bench_branches
scp -r christosp@yaguajay:/tmp/benchmark.out.bench_branches /tmp/
