#! /bin/sh

set -e

db_list="takeover_source1 takeover_source2 takeover_target1 takeover_target2"

for db in $db_list; do
  echo dropdb $db
  dropdb $db || true
done


for db in $db_list; do
  echo createdb $db
  createdb $db
done

