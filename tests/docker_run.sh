#! /bin/bash

set -e
set -x

pg_ctl -D data -l log/pg.log start || { cat log/pg.log ; exit 1; }

cd tests

exec "$@"
