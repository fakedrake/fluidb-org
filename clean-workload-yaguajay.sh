#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -xe

git push
ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && ./clean-workload.sh'"
