#! /bin/sh

set -e

db_list="shard_src shard_dst"

for db in $db_list; do
  echo dropdb $db
  dropdb $db || true
done


for db in $db_list; do
  echo createdb $db
  createdb $db
done

