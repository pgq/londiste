"""Basic replication core.
"""

import os
import sys
import time
import fnmatch

from logging import Logger
from typing import List, Optional, Dict, Sequence, Mapping, Tuple, Iterator

import skytools

from skytools.basetypes import DictRow, Connection, Cursor

from pgq.baseconsumer import EventList
from pgq.event import Event
from pgq.cascade.worker import CascadedWorker

from .exec_attrs import ExecAttrs
from .handler import build_handler, BaseHandler
from .handlers import load_handler_modules

__all__ = ['Replicator', 'TableState',
           'TABLE_MISSING', 'TABLE_IN_COPY', 'TABLE_CATCHING_UP',
           'TABLE_WANNA_SYNC', 'TABLE_DO_SYNC', 'TABLE_OK']

# state                 # owner - who is allowed to change
TABLE_MISSING = 0       # main
TABLE_IN_COPY = 1       # copy
TABLE_CATCHING_UP = 2   # copy
TABLE_WANNA_SYNC = 3    # main
TABLE_DO_SYNC = 4       # copy
TABLE_OK = 5            # setup

SYNC_OK = 0    # continue with batch
SYNC_LOOP = 1  # sleep, try again
SYNC_EXIT = 2  # nothing to do, exit script

MAX_PARALLEL_COPY = 8  # default number of allowed max parallel copy processes


def is_data_event(ev: Event) -> bool:
    """Is it insert/update/delete for one table?
    """
    if ev.type in ('I', 'U', 'D'):
        return True
    elif ev.type[:2] in ('I:', 'U:', 'D:', '{"'):
        return True
    return False


class Counter:
    """Counts table statuses."""

    missing = 0
    copy = 0
    catching_up = 0
    wanna_sync = 0
    do_sync = 0
    ok = 0

    def __init__(self, tables: List["TableState"], copy_method_map: Dict[str, Optional[int]]) -> None:
        """Counts and sanity checks."""
        for t in tables:
            if t.state == TABLE_MISSING:
                self.missing += 1
            elif t.state == TABLE_IN_COPY:
                nthreads = copy_method_map[t.name]
                if nthreads is None:
                    self.copy += 1
                else:
                    self.copy += nthreads
            elif t.state == TABLE_CATCHING_UP:
                self.catching_up += 1
            elif t.state == TABLE_WANNA_SYNC:
                self.wanna_sync += 1
            elif t.state == TABLE_DO_SYNC:
                self.do_sync += 1
            elif t.state == TABLE_OK:
                self.ok += 1

    def get_copy_count(self) -> int:
        return self.copy + self.catching_up + self.wanna_sync + self.do_sync


class TableState:
    """Keeps state about one table."""

    name: str
    dest_table: str
    log: Logger
    state: int
    last_snapshot_tick: Optional[int]
    str_snapshot: Optional[str]
    from_snapshot: Optional[skytools.Snapshot]
    sync_tick_id: Optional[int]
    ok_batch_count: int
    last_tick: Optional[int]
    table_attrs: Mapping[str, Optional[str]]
    copy_role: Optional[str]
    dropped_ddl: Optional[str]
    plugin: Optional[BaseHandler]
    changed: int
    copy_pos: int
    max_parallel_copy: int

    def __init__(self, name: str, log: Logger) -> None:
        """Init TableState for one table."""
        self.name = name
        self.dest_table = name
        self.log = log
        # same as forget:
        self.state = TABLE_MISSING
        self.last_snapshot_tick = None
        self.str_snapshot = None
        self.from_snapshot = None
        self.sync_tick_id = None
        self.ok_batch_count = 0
        self.last_tick = 0
        self.table_attrs = {}
        self.copy_role = None
        self.dropped_ddl = None
        self.plugin = None
        # except this
        self.changed = 0
        # position in parallel copy work order
        self.copy_pos = 0
        # max number of parallel copy processes allowed
        self.max_parallel_copy = MAX_PARALLEL_COPY

    def forget(self) -> None:
        """Reset all info."""
        self.state = TABLE_MISSING
        self.last_snapshot_tick = None
        self.str_snapshot = None
        self.from_snapshot = None
        self.sync_tick_id = None
        self.ok_batch_count = 0
        self.last_tick = 0
        self.table_attrs = {}
        self.changed = 1
        self.plugin = None
        self.copy_pos = 0
        self.max_parallel_copy = MAX_PARALLEL_COPY

    def change_snapshot(self, str_snapshot: Optional[str], tag_changed: int = 1) -> None:
        """Set snapshot."""
        if self.str_snapshot == str_snapshot:
            return
        self.log.debug("%s: change_snapshot to %s", self.name, str_snapshot)
        self.str_snapshot = str_snapshot
        if str_snapshot:
            self.from_snapshot = skytools.Snapshot(str_snapshot)
        else:
            self.from_snapshot = None

        if tag_changed:
            self.ok_batch_count = 0
            self.last_tick = None
            self.changed = 1

    def change_state(self, state: int, tick_id: Optional[int] = None) -> None:
        """Set state."""
        if self.state == state and self.sync_tick_id == tick_id:
            return
        self.state = state
        self.sync_tick_id = tick_id
        self.changed = 1
        self.log.debug("%s: change_state to %s", self.name, self.render_state())

    def render_state(self) -> Optional[str]:
        """Make a string to be stored in db."""

        if self.state == TABLE_MISSING:
            return None
        elif self.state == TABLE_IN_COPY:
            return 'in-copy'
        elif self.state == TABLE_CATCHING_UP:
            return 'catching-up'
        elif self.state == TABLE_WANNA_SYNC:
            return 'wanna-sync:%d' % (self.sync_tick_id or 0)
        elif self.state == TABLE_DO_SYNC:
            return 'do-sync:%d' % (self.sync_tick_id or 0)
        elif self.state == TABLE_OK:
            return 'ok'
        return None

    def parse_state(self, merge_state: Optional[str]) -> int:
        """Read state from string."""

        state = -1
        if merge_state is None:
            state = TABLE_MISSING
        elif merge_state == "in-copy":
            state = TABLE_IN_COPY
        elif merge_state == "catching-up":
            state = TABLE_CATCHING_UP
        elif merge_state == "ok":
            state = TABLE_OK
        elif merge_state == "?":
            state = TABLE_OK
        else:
            tmp = merge_state.split(':')
            if len(tmp) == 2:
                self.sync_tick_id = int(tmp[1])
                if tmp[0] == 'wanna-sync':
                    state = TABLE_WANNA_SYNC
                elif tmp[0] == 'do-sync':
                    state = TABLE_DO_SYNC

        if state < 0:
            raise Exception("Bad table state: %s" % merge_state)

        return state

    def loaded_state(self, row: DictRow) -> None:
        """Update object with info from db."""

        self.log.debug("loaded_state: %s: %s / %s",
                       self.name, row['merge_state'], row['custom_snapshot'])
        self.change_snapshot(row['custom_snapshot'], 0)
        self.state = self.parse_state(row['merge_state'])
        self.changed = 0
        if row['table_attrs']:
            self.table_attrs = skytools.db_urldecode(row['table_attrs'])
        else:
            self.table_attrs = {}
        self.copy_role = row['copy_role']
        self.dropped_ddl = row['dropped_ddl']
        if row['merge_state'] == "?":
            self.changed = 1

        self.copy_pos = int(row.get('copy_pos', '0'))

        max_parallel_copy = self.table_attrs.get('max_parallel_copy')
        if max_parallel_copy:
            self.max_parallel_copy = int(max_parallel_copy)

        if 'dest_table' in row and row['dest_table']:
            self.dest_table = row['dest_table']
        else:
            self.dest_table = self.name

        hstr = self.table_attrs.get('handlers', '')  # compat
        hstr = self.table_attrs.get('handler', hstr) or ''
        self.plugin = build_handler(self.name, hstr, self.dest_table)

    def max_parallel_copies_reached(self) -> bool:
        return self.max_parallel_copy is not None and \
            self.copy_pos >= self.max_parallel_copy

    def interesting(self, ev: Event, tick_id: int, copy_thread: bool, copy_table_name: Optional[str]) -> bool:
        """Check if table wants this event."""

        if copy_thread:
            if self.name != copy_table_name:
                return False
            if self.state not in (TABLE_CATCHING_UP, TABLE_DO_SYNC):
                return False
        else:
            if self.state != TABLE_OK:
                return False

        # if no snapshot tracking, then accept always
        if not self.from_snapshot:
            return True

        # uninteresting?
        if self.from_snapshot.contains(ev.txid):
            return False

        # after couple interesting batches there no need to check snapshot
        # as there can be only one partially interesting batch
        if tick_id != self.last_tick:
            self.last_tick = tick_id
            self.ok_batch_count += 1

            # disable batch tracking
            if self.ok_batch_count > 3:
                self.change_snapshot(None)
        return True

    def gc_snapshot(self, copy_thread: bool, prev_tick: int, cur_tick: int, no_lag: bool) -> None:
        """Remove attached snapshot if possible.

        If the event processing is in current moment, the snapshot
        is not needed beyond next batch.

        The logic is needed for mostly unchanging tables,
        where the .ok_batch_count check in .interesting()
        method can take a lot of time.
        """

        # check if gc is needed
        if self.str_snapshot is None:
            return

        # check if allowed to modify
        if copy_thread:
            if self.state != TABLE_CATCHING_UP:
                return
        else:
            if self.state != TABLE_OK:
                return

        # aquire last tick
        if not self.last_snapshot_tick:
            if no_lag:
                self.last_snapshot_tick = cur_tick
            return

        # reset snapshot if not needed anymore
        if self.last_snapshot_tick < prev_tick:
            self.change_snapshot(None)

    def get_plugin(self) -> BaseHandler:
        if not self.plugin:
            raise ValueError("no handler set")
        return self.plugin


class Replicator(CascadedWorker):
    """Replication core.

    Config options::

        ## Parameters for Londiste ##

        # target database
        db = dbname=somedb host=127.0.0.1

        # extra connect string parameters to add to node public connect strings.
        # useful values: user= sslmode=
        #remote_extra_connstr =

        # how many tables can be copied in parallel
        #parallel_copies = 1

        # glob patterns for table names: archive.*, public.*
        #threaded_copy_tables =
        # number of threads in pool
        #threaded_copy_pool_size = 1

        # accept only events for locally present tables
        #local_only = false
        # do not load EXECUTE events from source queue when local_only is active
        #local_only_drop_execute = false

        ## compare/repair
        # max amount of time table can be locked
        #lock_timeout = 10
        # compare: sql to use
        #compare_sql = select count(1) as cnt, sum(hashtext(t.*::text)) as chksum from only _TABLE_ t
        # workaround for hashtext change between 8.3 and 8.4
        #compare_sql = select count(1) as cnt, sum(('x' ||
        #       substr(md5(t.*::text),1,16))::bit(64)::bigint) as chksum from only _TABLE_ t
        #compare_fmt = %(cnt)d rows, checksum=%(chksum)s

        ## Parameters for initial node creation: create-root/branch/leaf ##

        # These parameters can be given on either command-line or in config
        # command-line values override config values.  Those values are
        # used only during create time, otherwise they are loaded from database.

        # Name for local node.
        #node_name =

        # public connect string for local node, which other nodes will use
        # to connect to this one.
        #public_node_location =

        # connect string for existing node to use as provider
        #initial_provider_location =

        # filter for table/seq registration
        #register_only_tables =
        #register_only_seqs =
        #register_skip_tables = s.a, s.b, s.c
        #register_skip_seqs =
    """

    # batch info
    cur_tick: int = 0
    prev_tick: int = 0
    copy_table_name: Optional[str] = None  # filled by Copytable()
    sql_list: List[str] = []

    current_event: Optional[Event] = None

    threaded_copy_tables: Sequence[str]
    threaded_copy_pool_size: int
    copy_method_map: Dict[str, Optional[int]]

    register_only_tables: Optional[Sequence[str]] = None
    register_only_seqs: Optional[Sequence[str]] = None
    register_skip_tables: Optional[Sequence[str]] = None
    register_skip_seqs: Optional[Sequence[str]] = None

    local_only: bool = False
    local_only_drop_execute: bool = False

    table_list: List[TableState]
    table_map: Dict[str, TableState]
    used_plugins: Dict[str, BaseHandler]
    copy_thread: bool

    def __init__(self, args: Sequence[str]) -> None:
        """Replication init."""
        super().__init__('londiste', 'db', args)

        self.table_list = []
        self.table_map = {}

        self.threaded_copy_tables = self.cf.getlist('threaded_copy_tables', [])
        self.threaded_copy_pool_size = self.cf.getint('threaded_copy_pool_size', 1)
        self.copy_method_map = {}

        self.copy_thread = False
        self.set_name = self.queue_name
        self.used_plugins = {}

        self.parallel_copies = self.cf.getint('parallel_copies', 1)
        if self.parallel_copies < 1:
            raise Exception('Bad value for parallel_copies: %d' % self.parallel_copies)

        self.consumer_filter = None

        self.register_only_tables = self.cf.getlist("register_only_tables", [])
        self.register_only_seqs = self.cf.getlist("register_only_seqs", [])
        self.register_skip_tables = self.cf.getlist("register_skip_tables", [])
        self.register_skip_seqs = self.cf.getlist("register_skip_seqs", [])

        self.local_only = self.cf.getboolean('local_only', False)
        self.local_only_drop_execute = self.cf.getboolean('local_only_drop_execute', False)

    def reload(self) -> None:
        super().reload()

        load_handler_modules(self.cf)

        self.threaded_copy_tables = self.cf.getlist('threaded_copy_tables', [])
        self.threaded_copy_pool_size = self.cf.getint('threaded_copy_pool_size', 1)
        self.copy_method_map = {}

        self.register_only_tables = self.cf.getlist("register_only_tables", [])
        self.register_only_seqs = self.cf.getlist("register_only_seqs", [])
        self.register_skip_tables = self.cf.getlist("register_skip_tables", [])
        self.register_skip_seqs = self.cf.getlist("register_skip_seqs", [])

        self.local_only = self.cf.getboolean('local_only', False)
        self.local_only_drop_execute = self.cf.getboolean('local_only_drop_execute', False)

    def fill_copy_method(self) -> None:
        for table_name in self.table_map:
            if table_name not in self.copy_method_map:
                for pat in self.threaded_copy_tables:
                    if fnmatch.fnmatchcase(table_name, pat):
                        self.copy_method_map[table_name] = self.threaded_copy_pool_size
                        break
                if table_name not in self.copy_method_map:
                    self.copy_method_map[table_name] = None

    def connection_hook(self, dbname: str, db: Connection) -> None:
        if dbname == 'db':
            curs = db.cursor()
            curs.execute("select londiste.set_session_replication_role('replica', false)")
            db.commit()

    code_check_done = 0
    def check_code(self, db: Connection) -> None:
        objs = [
            skytools.DBFunction("pgq.maint_operations", 0, sql_file="londiste.maint-upgrade.sql"),
        ]
        skytools.db_install(db.cursor(), objs, self.log)
        db.commit()

    def process_remote_batch(self, src_db: Connection, tick_id: int, ev_list: EventList, dst_db: Connection) -> None:
        "All work for a batch.  Entry point from SetConsumer."

        self.current_event = None

        # this part can play freely with transactions

        if not self.code_check_done:
            self.check_code(dst_db)
            self.code_check_done = 1

        self.sync_database_encodings(src_db, dst_db)

        assert self.batch_info
        self.cur_tick = self.batch_info['tick_id']
        self.prev_tick = self.batch_info['prev_tick_id']

        dst_curs = dst_db.cursor()
        self.load_table_state(dst_curs)
        self.sync_tables(src_db, dst_db)

        self.copy_snapshot_cleanup(dst_db)

        # only main thread is allowed to restore fkeys
        assert self._worker_state
        if not self.copy_thread and self._worker_state.process_events:
            self.restore_fkeys(dst_db)

        for p in self.used_plugins.values():
            p.reset()
        self.used_plugins = {}

        # now the actual event processing happens.
        # they must be done all in one tx in dst side
        # and the transaction must be kept open so that
        # the cascade-consumer can save last tick and commit.

        self.sql_list = []
        super().process_remote_batch(src_db, tick_id, ev_list, dst_db)
        self.flush_sql(dst_curs)

        for p in self.used_plugins.values():
            p.finish_batch(self.batch_info, dst_curs)
        self.used_plugins = {}

        # finalize table changes
        self.save_table_state(dst_curs)

    def sync_tables(self, src_db: Connection, dst_db: Connection) -> None:
        """Table sync loop.

        Calls appropriate handles, which is expected to
        return one of SYNC_* constants."""

        self.log.debug('Sync tables')
        while True:
            cnt = Counter(self.table_list, self.copy_method_map)
            if self.copy_thread:
                res = self.sync_from_copy_thread(cnt, src_db, dst_db)
            else:
                res = self.sync_from_main_thread(cnt, src_db, dst_db)

            if res == SYNC_EXIT:
                self.log.debug('Sync tables: exit')
                if self.copy_thread:
                    self.unregister_consumer()
                src_db.commit()
                sys.exit(0)
            elif res == SYNC_OK:
                return
            elif res != SYNC_LOOP:
                raise Exception('Program error')

            self.log.debug('Sync tables: sleeping')
            time.sleep(3)
            dst_db.commit()
            self.load_table_state(dst_db.cursor())
            dst_db.commit()

    dsync_backup: Optional[Tuple[int, Optional[int], Optional[str]]] = None
    def sync_from_main_thread(self, cnt: Counter, src_db: Connection, dst_db: Connection) -> int:
        "Main thread sync logic."

        # This operates on all table, any amount can be in any state

        ret = SYNC_OK

        if cnt.do_sync:
            # wait for copy thread to catch up
            ret = SYNC_LOOP

        # we need to do wanna-sync->do_sync with small batches
        need_dsync = False
        dsync_ok = True
        if self.pgq_min_interval or self.pgq_min_count:
            dsync_ok = False
        elif self.dsync_backup and self.dsync_backup[0] >= self.cur_tick:
            dsync_ok = False

        # now check if do-sync is needed
        for t in self.get_tables_in_state(TABLE_WANNA_SYNC):
            # copy thread wants sync, if not behind, do it
            if t.sync_tick_id is not None and self.cur_tick >= t.sync_tick_id:
                if dsync_ok:
                    self.change_table_state(dst_db, t, TABLE_DO_SYNC, self.cur_tick)
                    ret = SYNC_LOOP
                else:
                    need_dsync = True

        # tune batch size if needed
        if need_dsync:
            if self.pgq_min_count or self.pgq_min_interval:
                bak = (self.cur_tick, self.pgq_min_count, self.pgq_min_interval)
                self.dsync_backup = bak
                self.pgq_min_count = None
                self.pgq_min_interval = None
        elif self.dsync_backup:
            self.pgq_min_count = self.dsync_backup[1]
            self.pgq_min_interval = self.dsync_backup[2]
            self.dsync_backup = None

        # now handle new copies
        npossible = self.parallel_copies - cnt.get_copy_count()
        if cnt.missing and npossible > 0:
            pmap = self.get_state_map(src_db.cursor())
            src_db.commit()
            for t in self.get_tables_in_state(TABLE_MISSING):
                if 'copy_node' in t.table_attrs:
                    # should we go and check this node?
                    pass
                else:
                    # regular provider is used
                    if t.name not in pmap:
                        self.log.warning("Table %s not available on provider", t.name)
                        continue
                    pt = pmap[t.name]
                    if pt.state != TABLE_OK:  # or pt.custom_snapshot: # FIXME: does snapsnot matter?
                        self.log.info("Table %s not OK on provider, waiting", t.name)
                        continue

                # don't allow more copies than configured
                if npossible == 0:
                    break
                npossible -= 1

                # drop all foreign keys to and from this table
                self.drop_fkeys(dst_db, t.dest_table)

                # change state after fkeys are dropped thus allowing
                # failure inbetween
                self.change_table_state(dst_db, t, TABLE_IN_COPY)

                # the copy _may_ happen immediately
                self.launch_copy(t)

                # there cannot be interesting events in current batch
                # but maybe there's several tables, lets do them in one go
                ret = SYNC_LOOP

        return ret

    def sync_from_copy_thread(self, cnt: Counter, src_db: Connection, dst_db: Connection) -> int:
        "Copy thread sync logic."

        # somebody may have done remove-table in the meantime
        if self.copy_table_name not in self.table_map:
            self.log.error("copy_sync: lost table: %s", self.copy_table_name)
            return SYNC_EXIT

        # This operates on single table
        t = self.table_map[self.copy_table_name]

        if t.state == TABLE_DO_SYNC:
            # these settings may cause copy to miss right tick
            self.pgq_min_count = None
            self.pgq_min_interval = None

            assert t.sync_tick_id

            # main thread is waiting, catch up, then handle over
            if self.cur_tick == t.sync_tick_id:
                self.change_table_state(dst_db, t, TABLE_OK)
                return SYNC_EXIT
            elif self.cur_tick < t.sync_tick_id:
                return SYNC_OK
            else:
                self.log.error("copy_sync: cur_tick=%d sync_tick=%d",
                               self.cur_tick, t.sync_tick_id)
                raise Exception('Invalid table state')
        elif t.state == TABLE_WANNA_SYNC:
            # wait for main thread to react
            return SYNC_LOOP
        elif t.state == TABLE_CATCHING_UP:

            # partition merging
            if t.copy_role in ('wait-replay', 'lead'):
                return SYNC_LOOP

            # copy just finished
            if t.dropped_ddl:
                self.restore_copy_ddl(t, dst_db)
                return SYNC_OK

            # is there more work?
            if self.work_state:
                return SYNC_OK

            # seems we have catched up
            self.change_table_state(dst_db, t, TABLE_WANNA_SYNC, self.cur_tick)
            return SYNC_LOOP
        elif t.state == TABLE_IN_COPY:
            # table is not copied yet, do it
            self.do_copy(t, src_db, dst_db)

            # forget previous value
            self.work_state = 1

            return SYNC_LOOP
        else:
            # nothing to do
            return SYNC_EXIT

    def restore_copy_ddl(self, ts: TableState, dst_db: Connection) -> None:
        self.log.info("%s: restoring DDL", ts.name)
        dst_curs = dst_db.cursor()
        if ts.dropped_ddl:
            for ddl in skytools.parse_statements(ts.dropped_ddl):
                self.log.info(ddl)
                dst_curs.execute(ddl)
        q = "select * from londiste.local_set_table_struct(%s, %s, NULL)"
        self.exec_cmd(dst_curs, q, [self.queue_name, ts.name])
        ts.dropped_ddl = None
        dst_db.commit()

        # analyze
        self.log.info("%s: analyze", ts.name)
        dst_curs.execute("analyze " + skytools.quote_fqident(ts.name))
        dst_db.commit()

    def do_copy(self, tbl: TableState, src_db: Connection, dst_db: Connection) -> None:
        """Callback for actual copy implementation."""
        raise Exception('do_copy not implemented')

    def process_remote_event(self, src_curs: Cursor, dst_curs: Cursor, ev: Event) -> None:
        """handle one event"""

        self.log.debug(
            "New event: id=%s / type=%s / data=%s / extra1=%s / extra2=%r / extra3=%r",
            ev.id, ev.type, ev.data, ev.extra1, ev.extra2, ev.extra3
        )

        # set current_event only if processing them one-by-one
        if self.work_state < 0:
            self.current_event = ev

        if is_data_event(ev):
            self.handle_data_event(ev, dst_curs)
        elif ev.type == "R":
            self.flush_sql(dst_curs)
            self.handle_truncate_event(ev, dst_curs)
        elif ev.type == 'EXECUTE':
            self.flush_sql(dst_curs)
            self.handle_execute_event(ev, dst_curs)
        elif ev.type == 'londiste.add-table':
            self.flush_sql(dst_curs)
            self.add_set_table(dst_curs, ev.data)
        elif ev.type == 'londiste.remove-table':
            self.flush_sql(dst_curs)
            self.remove_set_table(dst_curs, ev.data)
        elif ev.type == 'londiste.remove-seq':
            self.flush_sql(dst_curs)
            self.remove_set_seq(dst_curs, ev.data)
        elif ev.type == 'londiste.update-seq':
            self.flush_sql(dst_curs)
            self.update_seq(dst_curs, ev)
        else:
            super().process_remote_event(src_curs, dst_curs, ev)

        # no point keeping it around longer
        self.current_event = None

    def handle_data_event(self, ev: Event, dst_curs: Cursor) -> None:
        """handle one data event"""
        t = self.get_table_by_name(ev.extra1)
        if not t or not t.interesting(ev, self.cur_tick, self.copy_thread, self.copy_table_name):
            self.stat_increase('ignored_events')
            return

        try:
            p = self.used_plugins[ev.extra1]
        except KeyError:
            p = t.get_plugin()
            self.used_plugins[ev.extra1] = p
            assert self.batch_info
            p.prepare_batch(self.batch_info, dst_curs)

        p.process_event(ev, self.apply_sql, dst_curs)

    def handle_truncate_event(self, ev: Event, dst_curs: Cursor) -> None:
        """handle one truncate event"""
        t = self.get_table_by_name(ev.extra1)
        if not t or not t.interesting(ev, self.cur_tick, self.copy_thread, self.copy_table_name):
            self.stat_increase('ignored_events')
            return

        fqname = skytools.quote_fqident(t.dest_table)

        try:
            p = self.used_plugins[ev.extra1]
        except KeyError:
            p = t.get_plugin()
            self.used_plugins[ev.extra1] = p
            assert self.batch_info
            p.prepare_batch(self.batch_info, dst_curs)

        if p.conf.get('ignore_truncate'):
            self.log.info("ignoring truncate for %s", fqname)
            return

        #
        # Always use CASCADE, because without it the
        # operation cannot work with FKeys, on both
        # slave and master.
        #
        sql = "TRUNCATE %s CASCADE;" % fqname

        self.flush_sql(dst_curs)
        dst_curs.execute(sql)

    def handle_execute_event(self, ev: Event, dst_curs: Cursor) -> None:
        """handle one EXECUTE event"""

        if self.copy_thread:
            return

        # parse event
        fname = ev.extra1
        s_attrs = ev.extra2
        exec_attrs = ExecAttrs(urlenc=s_attrs)
        sql = ev.data

        # fixme: curs?
        dst_curs.execute("select londiste.set_session_replication_role('local', true)")

        seq_map = {}
        q = "select seq_name, local from londiste.get_seq_list(%s) where local"
        dst_curs.execute(q, [self.queue_name])
        for row in dst_curs.fetchall():
            seq_map[row['seq_name']] = row['seq_name']

        tbl_map = {}
        for t in self.table_map.values():
            tbl_map[t.name] = t.dest_table

        q = "select * from londiste.execute_start(%s, %s, %s, false, %s)"
        res = self.exec_cmd(dst_curs, q, [self.queue_name, fname, sql, s_attrs], commit=False)
        ret = res[0]['ret_code']
        if ret > 200:
            self.log.warning("Skipping execution of '%s'", fname)
            dst_curs.execute("select londiste.set_session_replication_role('replica', true)")
            return

        if exec_attrs.need_execute(dst_curs, tbl_map, seq_map):
            self.log.info("%s: executing sql")
            xsql = exec_attrs.process_sql(sql, tbl_map, seq_map)
            for stmt in skytools.parse_statements(xsql):
                dst_curs.execute(stmt)
        else:
            self.log.info("%s: execution not needed on this node")

        q = "select * from londiste.execute_finish(%s, %s)"
        self.exec_cmd(dst_curs, q, [self.queue_name, fname], commit=False)
        dst_curs.execute("select londiste.set_session_replication_role('replica', true)")

    def apply_sql(self, sql: str, dst_curs: Cursor) -> None:

        # how many queries to batch together, drop batching on error
        limit = 200
        if self.work_state == -1:
            limit = 0

        self.sql_list.append(sql)
        if len(self.sql_list) >= limit:
            self.flush_sql(dst_curs)

    def flush_sql(self, dst_curs: Cursor) -> None:
        """Send all buffered statements to DB."""

        if len(self.sql_list) == 0:
            return

        buf = "\n".join(self.sql_list)
        self.sql_list = []

        dst_curs.execute(buf)

    def add_set_table(self, dst_curs: Cursor, tbl: str) -> None:
        """There was new table added to root, remember it."""

        if self.register_only_tables and tbl not in self.register_only_tables:
            return
        if self.register_skip_tables and tbl in self.register_skip_tables:
            return
        q = "select londiste.global_add_table(%s, %s)"
        dst_curs.execute(q, [self.set_name, tbl])

    def remove_set_table(self, dst_curs: Cursor, tbl: str) -> None:
        """There was table dropped from root, remember it."""
        if tbl in self.table_map:
            t = self.table_map[tbl]
            del self.table_map[tbl]
            self.table_list.remove(t)
        q = "select londiste.global_remove_table(%s, %s)"
        dst_curs.execute(q, [self.set_name, tbl])

    def remove_set_seq(self, dst_curs: Cursor, seq: str) -> None:
        """There was seq dropped from root, remember it."""

        q = "select londiste.global_remove_seq(%s, %s)"
        dst_curs.execute(q, [self.set_name, seq])

    def setup_local_only_filter(self) -> None:
        # store event filter
        if self.local_only:
            # create list of tables
            if self.copy_thread:
                _filterlist = skytools.quote_literal(self.copy_table_name)
            else:
                _filterlist = ','.join(map(skytools.quote_literal, self.table_map.keys()))

            # build filter
            cond_list = [
                "ev_type like 'pgq.%'",
                "ev_type like 'londiste.%'",
            ]
            if not self.local_only_drop_execute:
                cond_list.append("ev_type = 'EXECUTE'")
            if _filterlist:
                cond_list.append(f"ev_extra1 in ({_filterlist})")
            expr = " or ".join(cond_list)
            self.consumer_filter = f"({expr})"
        else:
            # no filter
            self.consumer_filter = None

    def load_table_state(self, curs: Cursor) -> None:
        """Load table state from database.

        Todo: if all tables are OK, there is no need
        to load state on every batch.
        """

        q = "select * from londiste.get_table_list(%s)"
        curs.execute(q, [self.set_name])

        new_list = []
        new_map = {}
        for row in curs.fetchall():
            if not row['local']:
                continue
            t = self.get_table_by_name(row['table_name'])
            if not t:
                t = TableState(row['table_name'], self.log)
            t.loaded_state(row)
            new_list.append(t)
            new_map[t.name] = t

        self.table_list = new_list
        self.table_map = new_map

        self.fill_copy_method()
        self.setup_local_only_filter()

    def refresh_state(self, dst_db: Connection, full_logic: bool = True) -> DictRow:
        res = super().refresh_state(dst_db, full_logic=full_logic)

        # make sure local_only filter is loaded on boot
        if self.local_only and self.consumer_filter is None:
            self.load_table_state(dst_db.cursor())
            dst_db.commit()

        return res

    def get_state_map(self, curs: Cursor) -> Dict[str, TableState]:
        """Get dict of table states."""

        q = "select * from londiste.get_table_list(%s)"
        curs.execute(q, [self.set_name])

        new_map = {}
        for row in curs.fetchall():
            if not row['local']:
                continue
            t = TableState(row['table_name'], self.log)
            t.loaded_state(row)
            new_map[t.name] = t
        return new_map

    def save_table_state(self, curs: Cursor) -> None:
        """Store changed table state in database."""

        for t in self.table_list:
            # backwards compat: move plugin-only dest_table to table_info
            if t.plugin and t.dest_table != t.plugin.dest_table:
                self.log.info("Overwriting .dest_table from plugin: tbl=%s  dst=%s",
                              t.name, t.plugin.dest_table)
                q = "update londiste.table_info set dest_table = %s"\
                    " where queue_name = %s and table_name = %s"
                curs.execute(q, [t.plugin.dest_table, self.set_name, t.name])

            if not t.changed:
                continue
            merge_state = t.render_state()
            self.log.info("storing state of %s: copy:%d new_state:%s",
                          t.name, self.copy_thread, merge_state)
            q = "select londiste.local_set_table_state(%s, %s, %s, %s)"
            curs.execute(q, [self.set_name,
                             t.name, t.str_snapshot, merge_state])
            t.changed = 0

    def change_table_state(self, dst_db: Connection, tbl: TableState, state: int, tick_id: Optional[int] = None) -> None:
        """Chage state for table."""

        tbl.change_state(state, tick_id)
        self.save_table_state(dst_db.cursor())
        dst_db.commit()

        self.log.info("Table %s status changed to '%s'", tbl.name, tbl.render_state())

    def get_tables_in_state(self, state: int) -> Iterator[TableState]:
        "get all tables with specific state"

        for t in self.table_list:
            if t.state == state:
                yield t

    def get_table_by_name(self, name: str) -> Optional[TableState]:
        """Returns cached state object."""
        if name.find('.') < 0:
            name = "public.%s" % name
        if name in self.table_map:
            return self.table_map[name]
        return None

    def launch_copy(self, tbl_stat: TableState) -> None:
        """Run parallel worker for copy."""
        self.log.info("Launching copy process")
        main_exe = sys.argv[0]
        conf = self.cf.filename or 'undefined'
        cmd: List[str] = [main_exe, conf, 'copy', tbl_stat.name, '-d']

        # pass same verbosity options as main script got
        if self.options.quiet:
            cmd.append('-q')
        if self.options.verbose:
            cmd += ['-v'] * self.options.verbose

        # let existing copy finish and clean its pidfile,
        # otherwise new copy will exit immediately.
        # FIXME: should not happen on per-table pidfile ???
        copy_pidfile = "%s.copy.%s" % (self.pidfile, tbl_stat.name)
        while skytools.signal_pidfile(copy_pidfile, 0):
            self.log.warning("Waiting for existing copy to exit")
            time.sleep(2)

        # launch and wait for daemonization result
        self.log.debug("Launch args: %r", cmd)
        res = os.spawnvp(os.P_WAIT, main_exe, cmd)
        self.log.debug("Launch result: %r", res)
        if res != 0:
            self.log.error("Failed to launch copy process, result=%d", res)

    def sync_database_encodings(self, src_db: Connection, dst_db: Connection) -> None:
        """Make sure client_encoding is same on both side."""

        try:
            # psycopg2
            if src_db.encoding != dst_db.encoding:
                dst_db.set_client_encoding(src_db.encoding)
        except AttributeError:
            # psycopg1
            src_curs = src_db.cursor()
            dst_curs = dst_db.cursor()
            src_curs.execute("show client_encoding")
            src_enc = src_curs.fetchone()[0]
            dst_curs.execute("show client_encoding")
            dst_enc = dst_curs.fetchone()[0]
            if src_enc != dst_enc:
                dst_curs.execute("set client_encoding = %s", [src_enc])

    def copy_snapshot_cleanup(self, dst_db: Connection) -> None:
        """Remove unnecessary snapshot info from tables."""
        no_lag = not self.work_state
        changes = False
        for t in self.table_list:
            t.gc_snapshot(self.copy_thread, self.prev_tick, self.cur_tick, no_lag)
            if t.changed:
                changes = True

        if changes:
            self.save_table_state(dst_db.cursor())
            dst_db.commit()

    def restore_fkeys(self, dst_db: Connection) -> None:
        """Restore fkeys that have both tables on sync."""
        dst_curs = dst_db.cursor()

        # NOT VALID appreared in 9.1
        q = "select londiste.version() as ext_version, current_setting('server_version_num')::int < 90100 as compat"
        dst_curs.execute(q)
        info = dst_curs.fetchone()
        if info[0]:
            ext_version = [int(v) for v in info[0].split('.')]
            do_compat_restore = ext_version < [3, 7] or info[1]
        else:
            do_compat_restore = True

        # restore fkeys -- one at a time
        q = "select * from londiste.get_valid_pending_fkeys(%s)"
        dst_curs.execute(q, [self.set_name])
        fkey_list = dst_curs.fetchall()
        dst_db.commit()

        for row in fkey_list:
            self.log.info('Creating fkey: %s (%s --> %s)', row['fkey_name'], row['from_table'], row['to_table'])
            if do_compat_restore:
                q2 = "select londiste.restore_table_fkey(%s, %s)"
                dst_curs.execute(q2, [row['from_table'], row['fkey_name']])
                dst_db.commit()
            else:
                q3 = "select londiste.restore_table_fkey(%s, %s, true)"
                done = False
                while not done:
                    dst_curs.execute(q3, [row['from_table'], row['fkey_name']])
                    sql = dst_curs.fetchone()[0]
                    if sql:
                        dst_curs.execute(sql)
                    else:
                        done = True
                    dst_db.commit()

    def drop_fkeys(self, dst_db: Connection, table_name: str) -> None:
        """Drop all foreign keys to and from this table.

        They need to be dropped one at a time to avoid deadlocks with user code.
        """

        dst_curs = dst_db.cursor()
        q = "select * from londiste.find_table_fkeys(%s)"
        dst_curs.execute(q, [table_name])
        fkey_list = dst_curs.fetchall()
        for row in fkey_list:
            self.log.info('Dropping fkey: %s', row['fkey_name'])
            q2 = "select londiste.drop_table_fkey(%s, %s)"
            dst_curs.execute(q2, [row['from_table'], row['fkey_name']])
            dst_db.commit()

    def process_root_node(self, dst_db: Connection) -> None:
        """On root node send seq changes to queue."""

        super().process_root_node(dst_db)

        q = "select * from londiste.root_check_seqs(%s)"
        self.exec_cmd(dst_db, q, [self.queue_name])

    def update_seq(self, dst_curs: Cursor, ev: Event) -> None:
        if self.copy_thread:
            return

        val = int(ev.data)
        seq = ev.extra1
        q = "select * from londiste.global_update_seq(%s, %s, %s)"
        self.exec_cmd(dst_curs, q, [self.queue_name, seq, val])

    def copy_event(self, dst_curs: Cursor, ev: Event, filtered_copy: int) -> None:
        # filtered_copy means merge-leaf
        # send only data events down (skipping seqs also)
        if filtered_copy:
            if ev.type[:9] in ('londiste.',):
                return

        if is_data_event(ev):
            t = self.get_table_by_name(ev.extra1)
            if t:
                try:
                    p = self.used_plugins[ev.extra1]
                except KeyError:
                    p = t.get_plugin()
                    self.used_plugins[ev.extra1] = p
                    p.prepare_batch(None, dst_curs)

                # handler may rewrite or drop the event
                ev2 = p.get_copy_event(ev, self.queue_name)
                if ev2 is None:
                    return
                ev = ev2

        super().copy_event(dst_curs, ev, filtered_copy)

    def exception_hook(self, det: Exception, emsg: str) -> None:
        # add event info to error message
        if self.current_event:
            ev = self.current_event
            info = "[ev_id=%d,ev_txid=%d] " % (ev.ev_id, ev.ev_txid)
            emsg = info + emsg
        super().exception_hook(det, emsg)


if __name__ == '__main__':
    script = Replicator(sys.argv[1:])
    script.start()

