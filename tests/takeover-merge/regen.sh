#! /bin/bash

. ../testlib.sh

../zstop.sh

v='-v'
v=''

sourceq1="takeover_sourceq1"
sourceq2="takeover_sourceq2"
targetq="takeover_targetq"

sourcedb1="takeover_source1"
sourcedb2="takeover_source2"
targetdb1="takeover_target1"
targetdb2="takeover_target2"

db_list="${sourcedb1} ${sourcedb2} ${targetdb1} ${targetdb2}"

conn_extra=""
if test -n "${PGHOST}"; then
    conn_extra="host=${PGHOST}"
fi

for db in $db_list; do
  cleardb $db
done
clearlogs

echo " * create configs * "

# create ticker conf
pgqd_conf=$(make_pgqd_config $db_list)

sourceq1_sourcedb1_conf=$(make_worker_config ${sourceq1} ${sourcedb1})
sourceq2_sourcedb2_conf=$(make_worker_config ${sourceq2} ${sourcedb2})
sourceq1_targetdb1_conf=$(make_worker_config ${sourceq1} ${targetdb1})
sourceq1_targetdb2_conf=$(make_worker_config ${sourceq1} ${targetdb2})
sourceq2_targetdb1_conf=$(make_worker_config ${sourceq2} ${targetdb1})
sourceq2_targetdb2_conf=$(make_worker_config ${sourceq2} ${targetdb2})
targetq_targetdb1_conf=$(make_worker_config ${targetq} ${targetdb1})
targetq_targetdb2_conf=$(make_worker_config ${targetq} ${targetdb2})

sourcedb1_cstr="dbname=${sourcedb1} ${conn_extra}"
sourcedb2_cstr="dbname=${sourcedb2} ${conn_extra}"
targetdb1_cstr="dbname=${targetdb1} ${conn_extra}"
targetdb2_cstr="dbname=${targetdb2} ${conn_extra}"

set -e
set -o pipefail

msg "Install londiste and initialize nodes"

run londiste $v "${sourceq1_sourcedb1_conf}" create-root "${sourcedb1}" "${sourcedb1_cstr}"
run londiste $v "${sourceq2_sourcedb2_conf}" create-root "${sourcedb2}" "${sourcedb2_cstr}"

run londiste $v "${targetq_targetdb1_conf}" create-root "${targetdb1}" "${targetdb1_cstr}"
run londiste $v "${targetq_targetdb2_conf}" create-branch "${targetdb2}" "${targetdb2_cstr}" \
    --provider="${targetdb1_cstr}"

run londiste $v "${sourceq1_targetdb1_conf}" create-leaf "${targetdb1}" "${targetdb1_cstr}" \
    --provider="${sourcedb1_cstr}" --merge="${targetq}"
run londiste $v "${sourceq1_targetdb2_conf}" create-leaf "${targetdb2}" "${targetdb2_cstr}" \
    --provider="${sourcedb1_cstr}" --merge="${targetq}"

run londiste $v "${sourceq2_targetdb1_conf}" create-leaf "${targetdb1}" "${targetdb1_cstr}" \
    --provider="${sourcedb2_cstr}" --merge="${targetq}"
run londiste $v "${sourceq2_targetdb2_conf}" create-leaf "${targetdb2}" "${targetdb2_cstr}" \
    --provider="${sourcedb2_cstr}" --merge="${targetq}"

ticker_config_sql="
    update pgq.queue set queue_ticker_idle_period='2 secs', queue_ticker_max_count=10, queue_ticker_max_lag='0.5 seconds'
"
for db in $db_list; do
  run_sql ${db} "${ticker_config_sql}"
done

msg "Run ticker"
run pgqd -d conf/pgqd.ini
run sleep 5

msg "See topology"
run londiste $v "${sourceq1_targetdb1_conf}" status
run londiste $v "${sourceq2_targetdb1_conf}" status
run londiste $v "${targetq_targetdb1_conf}" status

msg "Run londiste daemon for each node"

run londiste $v -d "${sourceq1_sourcedb1_conf}" worker
run londiste $v -d "${sourceq2_sourcedb2_conf}" worker

run londiste $v -d "${sourceq1_targetdb1_conf}" worker
run londiste $v -d "${sourceq1_targetdb2_conf}" worker

run londiste $v -d "${sourceq2_targetdb1_conf}" worker
run londiste $v -d "${sourceq2_targetdb2_conf}" worker

run londiste $v -d "${targetq_targetdb1_conf}" worker
run londiste $v -d "${targetq_targetdb2_conf}" worker

msg "Create table on root node and fill couple of rows"

create_sql='
create extension if not exists "uuid-ossp" with schema public;

create table mytable (
    id uuid primary key default uuid_generate_v4(),
    data text,
    tstamp timestamptz default now()
);
'

run_sql "${sourcedb1_cstr}" "${create_sql}"
run_sql "${sourcedb2_cstr}" "${create_sql}"
run_sql "${targetdb1_cstr}" "${create_sql}"
run_sql "${targetdb2_cstr}" "${create_sql}"

for n in 1 2 3 4 5 6 7 8 9 10; do
    run_sql "${sourcedb1_cstr}" "insert into mytable (data) values ('s1.row$n')"
done

for n in 1 2 3 4 5 6 7 8 9 10; do
    run_sql "${sourcedb2_cstr}" "insert into mytable (data) values ('s2.row$n')"
done

msg "Register table"

run londiste $v ${sourceq1_sourcedb1_conf} add-table mytable
run londiste $v ${sourceq2_sourcedb2_conf} add-table mytable

sleep 5

run londiste $v ${sourceq1_targetdb1_conf} add-table mytable --merge-all
run londiste $v ${sourceq1_targetdb1_conf} wait-sync

run londiste $v ${sourceq1_targetdb2_conf} add-table mytable --merge-all
run londiste $v ${targetq_targetdb2_conf} wait-sync

msg "Show status"

run londiste $v "${sourceq1_targetdb1_conf}" status
run londiste $v "${sourceq2_targetdb2_conf}" status
run londiste $v "${targetq_targetdb1_conf}" status

msg "Generate load"

for n in 1 2; do
cat > conf/gen$n.ini <<EOF
[loadgen]
job_name = gen${n}
db = ${sourcedb1_cstr}
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
loop_delay = 0.1
EOF
done

for n in 3 4; do
cat > conf/gen$n.ini <<EOF
[loadgen]
job_name = gen${n}
db = ${sourcedb2_cstr}
logfile = log/%(job_name)s.log
pidfile = pid/%(job_name)s.pid
loop_delay = 0.1
EOF
done

run ./loadgen.py -d conf/gen1.ini
run ./loadgen.py -d conf/gen2.ini
run ./loadgen.py -d conf/gen3.ini
run ./loadgen.py -d conf/gen4.ini

sleep 5

psql -d "${targetdb2_cstr}" -c "select count(*) from mytable"

for nr in $(seq 3); do

    msg "Takeover $nr to ${targetdb2}"

    run londiste $v "${targetq_targetdb2_conf}" takeover "${targetdb1}"

    msg "Show status"

    sleep 15

    run londiste $v "${sourceq1_targetdb1_conf}" status
    run londiste $v "${sourceq2_targetdb2_conf}" status
    run londiste $v "${targetq_targetdb1_conf}" status

    msg "Takeover $nr to ${targetdb1}"

    run londiste $v "${targetq_targetdb1_conf}" takeover "${targetdb2}"

    sleep 15

    msg "Show status"

    run londiste $v "${sourceq1_targetdb1_conf}" status
    run londiste $v "${sourceq2_targetdb2_conf}" status
    run londiste $v "${targetq_targetdb1_conf}" status
done

../zcheck.sh

