#!/bin/bash
set -ex
# Push to git and build haskell on yaguajay then build c++ locally.

git commit -a -m "sync"
git push
ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && ./build-branches.sh'"
Copy the c++ files here

rm -r ./ssb-workload/query*
scp -r christosp@yaguajay:/run/user/1000/fluidb-data/workload/* ./ssb-workload/

find ssb-workload/ \
     -name '*.cpp' \
     -exec sed -i '' -e 's|/run/user/1000/|/tmp/fluidb_data/|g' \
                     -e 's|"\(data[0-9]*.dat\)"|"/tmp/fluidb_data/\1"|g' \
                     {} \;
