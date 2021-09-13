#! /bin/sh

lst="regdb1 regdb2"

for db in $lst; do
  echo dropdb $db
  dropdb --if-exists $db
done
for db in $lst; do
  echo createdb $db
  createdb $db
done
