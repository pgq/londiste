#! /bin/bash

set -e
set -x

tests="qtable"
tests="
    simple
    qtable
    obfuscate
    shard
    register
    takeover-merge
"
#tests="shard"
#tests="takeover-merge"

PAGER=cat
export PAGER

cd $(dirname $0)

for tst in $tests; do
    (cd $tst; ../zstop.sh; ./init.sh; ./regen.sh; ../zstop.sh)
done

python3 --version
psql --version
initdb --version

