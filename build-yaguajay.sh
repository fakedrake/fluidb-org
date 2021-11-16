#!/bin/bash
set -e
# Push to git and build haskell on yaguajay then build c++ locally.

git commit -a -m "sync"
git push
# ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && ./build.sh"
# Copy the c++ files here
scp -r christosp@yaguajay:/run/user/1000/fluidb-data/workload ./ssb-workload/
