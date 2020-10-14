#! /bin/sh

lst="simpledb1 simpledb2"
lst="db1 db2 db3 db4 db5"

for db in $lst; do
  echo dropdb $db
  dropdb --if-exists $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
