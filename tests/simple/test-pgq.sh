#! /bin/bash

. ../testlib.sh

v='-q'
v=''
nocheck=1

dropdb testdb
createdb testdb

run_sql testdb "create extension pgq"
run_sql testdb "create extension pgq_node"
run_sql testdb "select pgq.create_queue('testq')"
