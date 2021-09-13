#! /bin/bash

. ../testlib.sh

../zstop.sh

v='-v'
v=''

root_db="regdb1"
branch_db="regdb2"
root_conf="conf/${root_db}.ini"
branch_conf="conf/${branch_db}.ini"
root_cstr="dbname=${root_db}"
branch_cstr="dbname=${branch_db}"

db_list="${root_db} ${branch_db}"
qname="regq"

kdb_list=`echo $db_list | sed 's/ /,/g'`

echo " * create configs * "

# create ticker conf
cat > conf/pgqd.ini <<EOF
[pgqd]
database_list = $kdb_list
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

# londiste configs
cat > conf/${root_db}.ini <<EOF
[londiste]
db = dbname=${root_db}
queue_name = ${qname}
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
EOF

cat > conf/${branch_db}.ini <<EOF
[londiste]
db = dbname=${branch_db}
queue_name = ${qname}
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
register_only_tables = public.table1, public.table2, public.table3
register_skip_tables = public.table2, public.table4
EOF

for db in $db_list; do
  cleardb $db
done

clearlogs

set -e

msg "Basic config"
run cat conf/pgqd.ini
run cat ${root_conf}

msg "Install londiste and initialize nodes"
run londiste $v "${root_conf}" create-root "${root_db}" "${root_cstr}"
run londiste $v "${branch_conf}" create-branch "${branch_db}" "${branch_cstr}" --provider="${root_cstr}"
for db in $db_list; do
  run_sql ${db} "update pgq.queue set queue_ticker_idle_period='3 secs'"
done

msg "Run ticker"
run pgqd -d conf/pgqd.ini
run sleep 5

msg "See topology"
run londiste $v "${root_conf}" status

msg "Run londiste daemon for each node"
for db in $db_list; do
  run londiste $v -d conf/$db.ini worker
done

msg "Create tables on root node"
run_sql "${root_db}" "
    create table public.table1 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table2 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table3 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table4 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table5 (id int4 primary key, data text, tstamp timestamptz default now());
"

msg "Create tables on branch node"
run_sql "${branch_db}" "
    create table public.table1 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table2 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table3 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table4 (id int4 primary key, data text, tstamp timestamptz default now());
    create table public.table5 (id int4 primary key, data text, tstamp timestamptz default now());
"

msg "Register tables on root node"
run londiste $v ${root_conf} add-table table1 table2

msg "Register table on other node with creation, shard handler"
run londiste $v ${branch_conf} add-table table1

msg "Register more tables on root node"
run londiste $v ${root_conf} add-table table3 table4 table5

msg "Wait until table is in sync"
run londiste $v ${branch_conf} wait-sync


msg "Check status"
run londiste $v "${branch_conf}" status

run sleep 5

msg "Check table info on branch"
psql ${branch_db} -qAtc "select table_name from londiste.table_info order by nr"
wrong=$(psql ${branch_db} -qAtc "select table_name from londiste.table_info where table_name not in ('public.table1', 'public.table3')")
if test -n "${wrong}"; then
    echo "wrong tables registered: ${wrong}"
    exit 1
fi

../zcheck.sh

