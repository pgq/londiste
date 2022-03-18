#! /bin/bash

. ../testlib.sh

v='-q'
v=''
nocheck=1

db_list="a b c d"

kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

do_check() {
  test $nocheck = 1 || ../zcheck.sh
}

title Circular test

# create ticker conf
cat > conf/pgqd.ini <<EOF
[pgqd]
database_list = $kdb_list
ticker_period = 0.2
check_period = 1
syslog = 0
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
#threaded_copy_tables = public.mytable2
#threaded_copy_pool_size = 1
EOF

for db in $db_list; do
  cleardb $db
done

clearlogs

set -e

msg "Initialize nodes"
# root
run londiste $v cf/aq_a.ini create-root a
run londiste $v cf/bq_b.ini create-root b
run londiste $v cf/cq_c.ini create-root c
run londiste $v cf/dq_d.ini create-root d

# merge-leaf
run londiste $v cf/aq_c.ini create-leaf aq_c --provider='dbname=a' --merge='cq'
run londiste $v cf/bq_a.ini create-leaf bq_a --provider='dbname=b' --merge='aq'
run londiste $v cf/bq_d.ini create-leaf bq_d --provider='dbname=b' --merge='dq'

# leaf
run londiste $v cf/cq_b.ini create-leaf cq_b --provider='dbname=c'
run londiste $v cf/cq_d.ini create-leaf cq_d --provider='dbname=c'


msg "Run ticker"
run pgqd $v -d conf/pgqd.ini
run sleep 2

msg "See topology"
run londiste $v cf/aq_a.ini status
run londiste $v cf/bq_b.ini status
run londiste $v cf/cq_c.ini status
run londiste $v cf/dq_d.ini status

msg "Run londiste daemon for each node"
run londiste $v -d cf/aq_a.ini worker
run londiste $v -d cf/bq_b.ini worker
run londiste $v -d cf/cq_c.ini worker
run londiste $v -d cf/dq_d.ini worker
run londiste $v -d cf/aq_c.ini worker
run londiste $v -d cf/cq_b.ini worker
run londiste $v -d cf/cq_d.ini worker
run londiste $v -d cf/bq_a.ini worker
run londiste $v -d cf/bq_d.ini worker


msg "Create tables"
run_sql a "create table a_to_cbd (data text primary key)"
run_sql a "create table b_to_acd (data text primary key)"

run_sql b "create table a_to_cbd (data text primary key)"
run_sql b "create table b_to_acd (data text primary key)"
run_sql b "create table b_to_d (data text primary key)"
run_sql b "create table c_to_bd (data text primary key)"

run_sql c "create table a_to_cbd (data text primary key)"
run_sql c "create table b_to_acd (data text primary key)"
run_sql c "create table c_to_bd (data text primary key)"

run_sql d "create table a_to_cbd (data text primary key)"
run_sql d "create table b_to_acd (data text primary key)"
run_sql d "create table b_to_d (data text primary key)"
run_sql d "create table c_to_bd (data text primary key)"


msg "Register tables"
# root
run londiste $v cf/aq_a.ini add-table a_to_cbd
run londiste $v cf/bq_b.ini add-table b_to_acd b_to_d
run londiste $v cf/cq_c.ini add-table c_to_bd

# merge-leaf
run londiste $v cf/bq_a.ini add-table b_to_acd --merge-all
run londiste $v cf/aq_c.ini add-table a_to_cbd b_to_acd --merge-all
run londiste $v cf/bq_d.ini add-table b_to_d --merge-all

# leaf
run londiste $v cf/cq_d.ini add-table a_to_cbd c_to_bd b_to_acd --no-merge
run londiste $v cf/cq_b.ini add-table a_to_cbd c_to_bd --no-merge


msg "Wait until everything is in sync"
run londiste $v cf/aq_c.ini wait-sync
run londiste $v cf/bq_a.ini wait-sync
run londiste $v cf/bq_d.ini wait-sync
run londiste $v cf/cq_b.ini wait-sync
run londiste $v cf/cq_d.ini wait-sync


msg "Add some data"
run_sql a "insert into a_to_cbd values ('test')"
run_sql b "insert into b_to_acd values ('test')"
run_sql b "insert into b_to_d values ('test')"
run_sql c "insert into c_to_bd values ('test')"


msg "Wait for tables to replicate"
run sleep 10


msg "Check data"
run_sql a "select * from a_to_cbd"
run_sql a "select * from b_to_acd"

run_sql b "select * from a_to_cbd"
run_sql b "select * from b_to_acd"
run_sql b "select * from b_to_d"
run_sql b "select * from c_to_bd"

run_sql c "select * from a_to_cbd"
run_sql c "select * from b_to_acd"
run_sql c "select * from c_to_bd"

run_sql d "select * from a_to_cbd"
run_sql d "select * from b_to_acd"
run_sql d "select * from b_to_d"
run_sql d "select * from c_to_bd"


msg "Check londiste status"
run londiste $v cf/aq_a.ini status
run londiste $v cf/bq_b.ini status
run londiste $v cf/cq_c.ini status
run londiste $v cf/dq_d.ini status

#run tail -f log/cq_d.log

exit 0
