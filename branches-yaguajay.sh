#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -xe

git commit -a -m "sync" || true
git push
ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && rm -f /tmp/branches.zip && ./branches.sh && cd /tmp && zip -r branches.zip benchmark.out.bench_branches'"
rm -rf /tmp/benchmark.out.bench_branches /tmp/branches.zip
scp -r christosp@yaguajay:/tmp/branches.zip /tmp/
cd /tmp/
unzip branches.zip
