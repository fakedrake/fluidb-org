#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -xe

git commit -a -m "sync" || true
git push
ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && ./build-workload.sh'"
