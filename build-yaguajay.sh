#!/bin/bash
set -ex
# Push to git and build haskell on yaguajay then build c++ locally.

git commit -a -m "sync" || true
git push
ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && ./build.sh'"

mkdir -p ./ssb-workload/main/
mkdir -p ./ssb-workload/indiv/
rm -r ./ssb-workload/main/query*
rm -r ./ssb-workload/indiv/query*
scp -r christosp@yaguajay:/run/user/1000/fluidb-data/workload-main/* ./ssb-workload/main/
scp -r christosp@yaguajay:/run/user/1000/fluidb-data/workload-indiv/* ./ssb-workload/idiv/
mkdir -p /tmp/fluidb-data/
scp -r christosp@yaguajay:/run/user/1000/fluidb-data/*.dat /tmp/fluidb-data/

find ssb-workload/ \
     -name '*.cpp' \
     -exec sed -i '' -e 's|/run/user/1000/|/tmp/|g' \
                     {} \;
