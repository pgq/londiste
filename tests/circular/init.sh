#! /bin/sh

db_list="a b c d"

for db in $db_list; do
  echo dropdb $db
  dropdb $db
done


for db in $db_list; do
  echo createdb $db
  createdb $db
done
