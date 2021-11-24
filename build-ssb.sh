#!/usr/bin/env nix-shell
#! nix-shell -i bash
set -xe

root=$(pwd)

function new_dir {
    rm -rf $1
    mkdir -p $1
    cd $1
}
cd $root
new_dir ./ssb-data
new_dir ./tables

echo "Generating tables in $(pwd)"
for i in l d s c p; do
    echo "Generating table: ${i}"
    dbgen -s 1 -T ${i}
done

new_dir /tmp/fluidb-data
new_dir /tmp/fluidb-data/bamas
bama_dir=/tmp/fluidb-data/bamas
dat_dir=/tmp/fluidb-data
echo "Generating bama files in ${bama_dir}  and ${dat_dir}"
stack build 'fluidb:exe:bamify'
stack exec -- bamify /home/christosp/Projects/fluidb-org/ssb-data/tables /tmp/fluidb-data/bamas /tmp/fluidb-data
