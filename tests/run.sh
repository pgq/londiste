#! /bin/bash

set -e
set -x

tests="qtable"
tests="simple"

cd $(dirname $0)

for tst in $tests; do
    (cd $tst; ../zstop.sh; ./init.sh; ./regen.sh; )
done
