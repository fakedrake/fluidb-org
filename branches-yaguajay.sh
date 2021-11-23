#!/bin/bash
set -ex
# Push to git and build haskell on yaguajay then build c++ locally.

git commit -a -m "sync" || true
git push
ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && rm /tmp/branches.zip && ./branches.sh && zip -r /tmp/branches.zip /tmp/benchmark.out.bench_branches'"
rm -rf /tmp/benchmark.out.bench_branches /tmp/branches.zip
scp -r christosp@yaguajay:/tmp/branches.zip /tmp/
cd /tmp/
unzip branches.zip
