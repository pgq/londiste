#! /bin/bash

. ../testlib.sh

../zstop.sh

v='-v'
v=''

# bulkloader method
meth=0

db_list="shard_src shard_dst"
qname="shardq"

kdb_list=`echo $db_list | sed 's/ /,/g'`

#( cd ../..; make -s install )

echo " * create configs * "

# create ticker conf
cat > conf/pgqd.ini <<EOF
[pgqd]
database_list = $kdb_list
logfile = log/pgqd.log
pidfile = pid/pgqd.pid
EOF

# londiste configs
for db in ${db_list}; do
cat > conf/${db}.ini <<EOF
[londiste]
db = dbname=${db}
queue_name = ${qname}
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
handler_modules = londiste.handlers.shard
EOF
done

for db in $db_list; do
  cleardb $db
done

root_db="shard_src"
branch_db="shard_dst"

root_conf="conf/shard_src.ini"
branch_conf="conf/shard_dst.ini"

root_cstr="dbname=${root_db}"
branch_cstr="dbname=${branch_db}"

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

msg "Create part config"

hashfunc="
create function partconf.get_hash_raw (val uuid)
returns int4 as 'select get_byte(uuid_send(val), 15)' language sql immutable strict
"
hashfunc="
create function partconf.get_hash_raw (val int4)
returns int4 as 'select val & 15'
language sql immutable strict
"

run_sql "${root_db}" "create schema partconf"
run_sql "${root_db}" "create table partconf.conf (shard_nr int4, shard_mask int4, shard_count int4)"
run_sql "${root_db}" "insert into partconf.conf values (0, 0, 1)"
run_sql "${root_db}" "${hashfunc}"

run_sql "${branch_db}" "create schema partconf"
run_sql "${branch_db}" "create table partconf.conf (shard_nr int4, shard_mask int4, shard_count int4)"
run_sql "${branch_db}" "insert into partconf.conf values (0, 1, 2)"
run_sql "${branch_db}" "${hashfunc}"

msg "Create table on root node and fill couple of rows"
run_sql "${root_db}" "create table mytable (id int4 primary key, data text, tstamp timestamptz default now())"
for n in 1 2 3 4 5 6 7 8 9 10; do
  run_sql "${root_db}" "insert into mytable values ($n, 'row$n')"
done
run_sql "${branch_db}" "create table mytable (id int4 primary key, data text, tstamp timestamptz default now())"

msg "Register table on root node"
run londiste $v ${root_conf} add-table mytable --trigger-flags=J --handler=shard --handler-arg="key=id"

msg "Register table on other node with creation, shard handler"
run londiste $v ${branch_conf} add-table mytable --trigger-flags=J --handler=shard --handler-arg="key=id"

msg "Wait until table is in sync"
run londiste $v ${branch_conf} wait-sync

msg "Do some updates"
run_sql ${root_db} "insert into mytable values (15, 'row5')"
run_sql ${root_db} "update mytable set data = 'row5x' where id = 5"

run_sql ${root_db} "insert into mytable values (16, 'row6')"
run_sql ${root_db} "delete from mytable where id = 6"

run_sql ${root_db} "insert into mytable values (17, 'row7')"
run_sql ${root_db} "update mytable set data = 'row7x' where id = 7"
run_sql ${root_db} "delete from mytable where id = 7"

run_sql ${root_db} "delete from mytable where id = 1"
run_sql ${root_db} "update mytable set data = 'row2x' where id = 2"

run sleep 5

msg "Check status"
run londiste $v "${branch_conf}" status

run sleep 5

tbl=$(psql ${root_db} -qAtc "select * from pgq.current_event_table('${qname}');")
msg "Check queue '${qname}' from table $tbl"
run_sql ${branch_db} "select * from $tbl"

#run_sql hdst 'select * from mytable order by id'

../zcheck.sh

