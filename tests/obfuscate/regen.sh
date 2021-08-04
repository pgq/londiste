#! /bin/bash

. ../testlib.sh

v='-q'
v=''
nocheck=1

db_list="root leaf"

kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

do_check() {
  test $nocheck = 1 || ../zcheck.sh
}

title Obfuscate test

# create ticker conf
cat > conf/pgqd.ini <<EOF
[pgqd]
database_list = $kdb_list
ticker_period = 0.2
check_period = 1
syslog = 0
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
threaded_copy_tables = public.mytable2
threaded_copy_pool_size = 1
EOF

for db in $db_list; do
  cleardb $db
done

clearlogs

set -e

msg "Basic config"
run cat conf/pgqd.ini
run cat cf/rootq_root.ini
run cat cf/rootq_leaf.ini
run cat cf/leafq_leaf.ini

msg "Initialize nodes"
run londiste $v cf/rootq_root.ini create-root source
run londiste $v cf/leafq_leaf.ini create-root obfsource
run londiste $v cf/rootq_leaf.ini create-leaf obfleaf --provider='dbname=root' --merge='leafq'

msg "Run ticker"
run pgqd $v -d conf/pgqd.ini
run sleep 2

msg "See topology"
run londiste $v cf/rootq_root.ini status
run londiste $v cf/rootq_leaf.ini status
run londiste $v cf/leafq_leaf.ini status

msg "Run londiste daemon for each node"
run londiste $v -d cf/rootq_root.ini worker
run londiste $v -d cf/leafq_leaf.ini worker
run londiste $v -d cf/rootq_leaf.ini worker

msg "Create table in each node"
run_sql root "create table mytable (id int4 primary key, htext text, btext text, stext text)"
run_sql leaf "create table mytable (id int4 primary key, htext text, btext text, stext text)"

msg "Add some data in root node"
run_sql root "insert into mytable values (1, 'hdata1', 'bdata1', 'sdata1')"

msg "Register table on each node"
run londiste $v cf/rootq_root.ini add-table mytable
run londiste $v cf/rootq_leaf.ini add-table mytable --merge-all --handler=obfuscate

msg "Wait until table is in sync"
run londiste $v cf/rootq_leaf.ini wait-sync

msg "Add more data in root node"
run_sql root "insert into mytable values (2, 'hdata2', 'bdata2', 'sdata2')"

msg "Wait until table is in sync"
run sleep 3

msg "See data"
run_sql root "select * from pgq.event_template where ev_extra1 = 'public.mytable'"
run_sql root "select * from mytable"
run_sql leaf "select * from pgq.event_template where ev_extra1 = 'public.mytable'"
run_sql leaf "select * from mytable"

msg "## table 2"

msg "Create table in each node"
run_sql root "create table mytable2 (id int4 primary key, htext text, btext text, stext text)"
run_sql leaf "create table mytable2 (id int4 primary key, htext text, btext text, stext text)"

msg "Add some data in root node"
run_sql root "insert into mytable2 values (1, 'hxdata1', 'bdata1', 'sdata1')"

msg "Register table on each node"
run londiste $v cf/rootq_root.ini add-table mytable2
run londiste $v cf/rootq_leaf.ini add-table mytable2 --merge-all --handler=obfuscate

msg "Wait until table is in sync"
run londiste $v cf/rootq_leaf.ini wait-sync

msg "Add more data in root node"
run_sql root "insert into mytable2 values (2, 'hxdata2', 'bdata2', 'sdata2')"

msg "Wait until table is in sync"
run sleep 3

msg "See data"
run_sql root "select * from pgq.event_template where ev_extra1 = 'public.mytable2'"
run_sql root "select * from mytable2"
run_sql leaf "select * from pgq.event_template where ev_extra1 = 'public.mytable2'"
run_sql leaf "select * from mytable2"

msg "See londiste status"
run londiste $v cf/rootq_root.ini status
run londiste $v cf/rootq_leaf.ini status
run londiste $v cf/leafq_leaf.ini status

#run tail -n 1000 log/rootq_leaf.log

exit 0
