#!/bin/bash
set -ex

git push
ssh christosp@yaguajay bash -c "'cd /home/christosp/Projects/fluidb-org && git pull && ./clean-workload.sh'"
