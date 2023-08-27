"""
== HANDLERS ==

* dispatch - "vanilla" dispatch handler with default args (see below)
* hourly_event
* hourly_batch
* hourly_field
* hourly_time
* daily_event
* daily_batch
* daily_field
* daily_time
* monthly_event
* monthly_batch
* monthly_field
* monthly_time
* yearly_event
* yearly_batch
* yearly_field
* yearly_time
* bulk_hourly_event
* bulk_hourly_batch
* bulk_hourly_field
* bulk_hourly_time
* bulk_daily_event
* bulk_daily_batch
* bulk_daily_field
* bulk_daily_time
* bulk_monthly_event
* bulk_monthly_batch
* bulk_monthly_field
* bulk_monthly_time
* bulk_yearly_event
* bulk_yearly_batch
* bulk_yearly_field
* bulk_yearly_time
* bulk_direct - functionally identical to bulk

== HANDLER ARGUMENTS ==

table_mode:
    * part - partitioned table (default)
    * direct - non-partitioned table
    * ignore - all events are ignored

part_func:
    database function to use for creating partition table.
    default is {londiste|public}.create_partition

part_mode:
    * batch_time - partitioned by batch creation time (default)
    * event_time - partitioned by event creation time
    * date_field - partitioned by date_field value. part_field required
    * current_time - partitioned by current time

part_field:
    date_field to use for partition. Required when part_mode=date_field

period:
    partition period, used for automatic part_name and part_template building
    * hour
    * day - default
    * month
    * year

part_name:
    custom name template for partition table. default is None as it is built
    automatically.
    example for daily partition: %(parent)s_%(year)s_%(month)s_%(day)s
    template variables:
    * parent - parent table name
    * year
    * month
    * day
    * hour

part_template:
    custom sql template for creating partition table. if omitted then partition
    function is used.
    template variables:
    * dest - destination table name. result on part_name evaluation
    * part - same as dest
    * parent - parent table name
    * pkey - parent table primary keys
    * schema_table - table name with replace: '.' -> '__'. for using in
        pk names etc.
    * part_field - date field name if table is partitioned by field
    * part_time - time of partition

row_mode:
    how rows are applied to target table
    * plain - each event creates SQL statement to run (default)
    * keep_latest - change updates to DELETE + INSERT
    * keep_all - change updates to inserts, ignore deletes

event_types:
    event types to process, separated by comma. Other events are ignored.
    default is all event types
    * I - inserts
    * U - updates
    * D - deletes

load_mode:
    how data is loaded to dst database. default direct
    * direct - using direct sql statements (default)
    * bulk - using copy to temp table and then sql.

method:
    loading method for load_mode bulk. defaults to 0
    * 0 (correct) - inserts as COPY into table,
                    update as COPY into temp table and single UPDATE from there
                    delete as COPY into temp table and single DELETE from there
    * 1 (delete)  - as 'correct', but do update as DELETE + COPY
    * 2 (merged)  - as 'delete', but merge insert rows with update rows
    * 3 (insert)  - COPY inserts into table, error when other events

fields:
    field name map for using just part of the fields and rename them
    * '*' - all fields. default
    * <field>[,<field>..] - list of source fields to include in target
    * <field>:<new_name> - renaming fields
    list and rename syntax can be mixed: field1,field2:new_field2,field3

skip_fields:
    list of field names to skip

table:
    new name of destination table. default is same as source

pre_part:
    sql statement(s) to execute before creating partition table. Usable
    variables are the same as in part_template

post_part:
    sql statement(s) to execute after creating partition table. Usable
    variables are the same as in part_template

retention_period:
    how long to keep partitions around. examples: '3 months', '1 year'

ignore_old_events:
    * 0 - handle all events in the same way (default)
    * 1 - ignore events coming for obsolete partitions

ignore_truncate:
    * 0 - process truncate event (default)
    * 1 - ignore truncate event

encoding:
    name of destination encoding. handler replaces all invalid encoding symbols
    and logs them as warnings

analyze:
    * 0 - do not run analyze on temp tables (default)
    * 1 - run analyze on temp tables

== NOTES ==

NB! londiste does not currently support table renaming and field mapping when
creating or coping initial data to destination table.  --expect-sync and
--skip-truncate should be used and --create switch is to be avoided.
"""

import datetime
import re
import logging
from typing import Sequence, List, Tuple, Optional, Dict, Any, Callable, Mapping, Type, Set

import skytools
from skytools import UsageError, quote_fqident, quote_ident, dbdict
#from skytools.basetypes import DictRow
from skytools.sqltools import DictRows
from skytools.dbstruct import T_ALL, TableStruct

from londiste.handler import BatchInfo, Cursor, Event, ApplyFunc, BaseHandler
from londiste.handlers import handler_args, update
from londiste.handlers.shard import ShardHandler
import londiste.util

__all__ = ['Dispatcher']

# BulkLoader load method
METH_CORRECT = 0
METH_DELETE = 1
METH_MERGED = 2
METH_INSERT = 3

# BulkLoader hacks
AVOID_BIZGRES_BUG = 0
USE_LONGLIVED_TEMP_TABLES = True
USE_REAL_TABLE = False

# mode variables (first in list is default value)
TABLE_MODES = ['part', 'direct', 'ignore']
PART_MODES = ['batch_time', 'event_time', 'date_field', 'current_time']
ROW_MODES = ['plain', 'keep_all', 'keep_latest']
LOAD_MODES = ['direct', 'bulk']
PERIODS = ['day', 'month', 'year', 'hour']
METHODS = [METH_CORRECT, METH_DELETE, METH_MERGED, METH_INSERT]

EVENT_TYPES = ['I', 'U', 'D']

PART_FUNC_OLD = 'public.create_partition'
PART_FUNC_NEW = 'londiste.create_partition'
PART_FUNC_ARGS = ['parent', 'part', 'pkeys', 'part_field', 'part_time', 'period']

RETENTION_FUNC = "londiste.drop_obsolete_partitions"


#------------------------------------------------------------------------------
# LOADERS
#------------------------------------------------------------------------------


class BaseLoader:
    table: str
    pkeys: Sequence[str]
    log: logging.Logger
    conf: skytools.dbdict

    def __init__(self, table: str, pkeys: Sequence[str], log: logging.Logger, conf: skytools.dbdict) -> None:
        self.table = table
        self.pkeys = pkeys
        self.log = log
        self.conf = conf or skytools.dbdict()

    def process(self, op: str, row: Dict[str, Any]) -> None:
        raise NotImplementedError()

    def flush(self, curs: Cursor) -> None:
        raise NotImplementedError()


class DirectLoader(BaseLoader):
    data: List[Tuple[str, Dict[str, Any]]]
    def __init__(self, table: str, pkeys: Sequence[str], log: logging.Logger, conf: skytools.dbdict) -> None:
        super().__init__(table, pkeys, log, conf)
        self.data = []

    def process(self, op: str, row: Dict[str, Any]) -> None:
        self.data.append((op, row))

    def flush(self, curs: Cursor) -> None:
        mk_sql: Dict[str, Callable[
            [Mapping[str, Any], str, Sequence[str], Optional[Mapping[str, str]]], str
        ]] = {
            'I': skytools.mk_insert_sql,
            'U': skytools.mk_update_sql,
            'D': skytools.mk_delete_sql
        }
        if self.data:
            curs.execute("\n".join(mk_sql[op](row, self.table, self.pkeys, None)
                                   for op, row in self.data))


class BaseBulkCollectingLoader(BaseLoader):
    """ Collect events into I,U,D lists by pk and keep only last event
    with most suitable operation. For example when event has operations I,U,U
    keep only last U, when I,U,D, keep nothing etc

    If after processing the op is not in I,U or D, then ignore that event for
    rest
    """
    OP_GRAPH = {'-': {'U': 'U', 'I': 'I', 'D': 'D'},
                'I': {'D': '.'},
                'U': {'D': 'D'},
                'D': {'I': 'U'},
                '.': {'I': 'I'},
                }

    pkey_ev_map: Dict[Tuple[str, ...], Tuple[str, Dict[str, Any]]]

    def __init__(self, table: str, pkeys: Sequence[str], log: logging.Logger, conf: skytools.dbdict) -> None:
        super().__init__(table, pkeys, log, conf)
        if not self.pkeys:
            raise Exception('non-pk tables not supported: %s' % self.table)
        self.pkey_ev_map = {}

    def process(self, op: str, row: Dict[str, Any]) -> None:
        """Collect rows into pk dict, keeping only last row with most
        suitable op"""
        pk_data: Tuple[str, ...] = tuple(row[k] for k in self.pkeys)
        # get current op state, None if first event
        _op = self.pkey_ev_map.get(pk_data, ('-', {}))[0]
        # find new state and store together with row data
        try:
            # get new op state using op graph
            # when no edge defined for old -> new op, keep old
            _op = self.OP_GRAPH[_op].get(op, _op)
            self.pkey_ev_map[pk_data] = (_op, row)

            # skip update to pk-only table
            if len(pk_data) == len(row) and _op == 'U':
                del self.pkey_ev_map[pk_data]
        except KeyError:
            raise Exception('unknown event type: %s' % op) from None

    def collect_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Collects list of rows into operation hashed dict
        """
        op_map: Dict[str, List[Dict[str, Any]]] = {'I': [], 'U': [], 'D': []}
        for op, row in self.pkey_ev_map.values():
            # ignore None op events
            if op in op_map:
                op_map[op].append(row)
        return op_map

    def flush(self, curs: Cursor) -> None:
        op_map = self.collect_data()
        self.bulk_flush(curs, op_map)

    def bulk_flush(self, curs: Cursor, op_map: Dict[str, List[Dict[str, Any]]]) -> None:
        pass


class BaseBulkTempLoader(BaseBulkCollectingLoader):
    """ Provide methods for operating bulk collected events with temp table
    """
    keys: List[str]
    fields: Optional[List[str]]
    temp: str
    qtemp: str
    qtable: str

    def __init__(self, table: str, pkeys: Sequence[str], log: logging.Logger, conf: skytools.dbdict) -> None:
        super().__init__(table, pkeys, log, conf)
        # temp table name
        if USE_REAL_TABLE:
            self.temp = self.table + "_loadertmpx"
            self.qtemp = quote_fqident(self.temp)
        else:
            self.temp = self.table.replace('.', '_') + "_loadertmp"
            self.qtemp = quote_ident(self.temp)
        # quoted table name
        self.qtable = quote_fqident(self.table)
        # all fields
        self.fields = None
        # key fields used in where part, possible to add non pk fields
        # (like dist keys in gp)
        self.keys = list(self.pkeys)

    def nonkeys(self) -> List[str]:
        """returns fields not in keys"""
        if not self.fields:
            return []
        return [f for f in self.fields if f not in self.keys]

    def logexec(self, curs: Cursor, sql: str) -> None:
        """Logs and executes sql statement"""
        self.log.debug('exec: %s', sql)
        curs.execute(sql)
        self.log.debug('msg: %s, rows: %s', curs.statusmessage, curs.rowcount)

    # create sql parts

    def _where(self) -> str:
        tmpl = "%(tbl)s.%(col)s = t.%(col)s"
        stmt = (tmpl % {'col': quote_ident(f), 'tbl': self.qtable}
                for f in self.keys)
        return ' and '.join(stmt)

    def _cols(self) -> str:
        if not self.fields:
            return ''
        return ','.join(quote_ident(f) for f in self.fields)

    def insert(self, curs: Cursor) -> None:
        sql = "insert into %s (%s) select %s from %s" % (self.qtable, self._cols(), self._cols(), self.qtemp)
        self.logexec(curs, sql)

    def update(self, curs: Cursor) -> None:
        qcols = [quote_ident(c) for c in self.nonkeys()]

        # no point to update pk-only table
        if not qcols:
            return

        tmpl = "%s = t.%s"
        eqlist = [tmpl % (c, c) for c in qcols]
        _set = ", ".join(eqlist)

        sql = "update only %s set %s from %s as t where %s" % (self.qtable, _set, self.qtemp, self._where())
        self.logexec(curs, sql)

    def delete(self, curs: Cursor) -> None:
        sql = "delete from only %s using %s as t where %s" % (self.qtable, self.qtemp, self._where())
        self.logexec(curs, sql)

    def truncate(self, curs: Cursor) -> None:
        self.logexec(curs, "truncate %s" % self.qtemp)

    def drop(self, curs: Cursor) -> None:
        self.logexec(curs, "drop table %s" % self.qtemp)

    def create(self, curs: Cursor) -> None:
        if USE_REAL_TABLE:
            tmpl = "create table %s (like %s)"
        else:
            tmpl = "create temp table %s (like %s) on commit preserve rows"
        self.logexec(curs, tmpl % (self.qtemp, self.qtable))

    def analyze(self, curs: Cursor) -> None:
        self.logexec(curs, "analyze %s" % self.qtemp)

    def process(self, op: str, row: Dict[str, Any]) -> None:
        super().process(op, row)
        # TODO: maybe one assignment is enough?
        self.fields = list(row.keys())


class BulkLoader(BaseBulkTempLoader):
    """ Collects events to and loads bulk data using copy and temp tables
    """
    dist_fields: Optional[List[str]]
    run_analyze: int
    method: int
    temp_present: bool

    def __init__(self, table: str, pkeys: Sequence[str], log: logging.Logger, conf: skytools.dbdict) -> None:
        super().__init__(table, pkeys, log, conf)
        self.method = self.conf['method']
        self.run_analyze = self.conf['analyze']
        self.dist_fields = None
        # is temp table created
        self.temp_present = False

    def process(self, op: str, row: Dict[str, Any]) -> None:
        if self.method == METH_INSERT and op != 'I':
            raise Exception('%s not supported by method insert' % op)
        super().process(op, row)

    def process_delete(self, curs: Cursor, op_map: Dict[str, List[Dict[str, Any]]]) -> None:
        """Process delete list"""
        data = op_map['D']
        cnt = len(data)
        if cnt == 0:
            return
        self.log.debug("bulk: Deleting %d rows from %s", cnt, self.table)
        # copy rows to temp
        self.bulk_insert(curs, data)
        # delete rows using temp
        self.delete(curs)
        # check if right amount of rows deleted (only in direct mode)
        if self.conf.table_mode == 'direct' and cnt != curs.rowcount:
            self.log.warning("%s: Delete mismatch: expected=%s deleted=%d",
                             self.table, cnt, curs.rowcount)

    def process_update(self, curs: Cursor, op_map: Dict[str, List[Dict[str, Any]]]) -> None:
        """Process update list"""
        data = op_map['U']
        # original update list count
        real_cnt = len(data)
        # merged method loads inserts together with updates
        if self.method == METH_MERGED:
            data += op_map['I']
        cnt = len(data)
        if cnt == 0:
            return
        self.log.debug("bulk: Updating %d rows in %s", cnt, self.table)
        # copy rows to temp
        self.bulk_insert(curs, data)
        if self.method == METH_CORRECT:
            # update main table from temp
            self.update(curs)
            # check count (only in direct mode)
            if self.conf.table_mode == 'direct' and cnt != curs.rowcount:
                self.log.warning("%s: Update mismatch: expected=%s updated=%d",
                                 self.table, cnt, curs.rowcount)
        else:
            # delete from main table using temp
            self.delete(curs)
            # check count (only in direct mode)
            if self.conf.table_mode == 'direct' and real_cnt != curs.rowcount:
                self.log.warning("%s: Update mismatch: expected=%s deleted=%d",
                                 self.table, real_cnt, curs.rowcount)
            # insert into main table
            if AVOID_BIZGRES_BUG:
                # copy again, into main table
                self.bulk_insert(curs, data, table=self.qtable)
            else:
                # insert from temp - better way, but does not work
                # due bizgres bug
                self.insert(curs)

    def process_insert(self, curs: Cursor, op_map: Dict[str, List[Dict[str, Any]]]) -> None:
        """Process insert list"""
        data = op_map['I']
        cnt = len(data)
        # merged method loads inserts together with updates
        if (cnt == 0) or (self.method == METH_MERGED):
            return
        self.log.debug("bulk: Inserting %d rows into %s", cnt, self.table)
        # copy into target table (no temp used)
        self.bulk_insert(curs, data, table=self.qtable)

    def bulk_flush(self, curs: Cursor, op_map: Dict[str, List[Dict[str, Any]]]) -> None:
        self.log.debug("bulk_flush: %s  (I/U/D = %d/%d/%d)", self.table,
                       len(op_map['I']), len(op_map['U']), len(op_map['D']))

        # fetch distribution fields
        if self.dist_fields is None:
            self.dist_fields = self.find_dist_fields(curs)
            assert self.dist_fields
            self.log.debug("Key fields: %s  Dist fields: %s",
                           ",".join(self.pkeys or []),
                           ",".join(self.dist_fields or []))
            # add them to key
            for key in self.dist_fields:
                if key not in self.keys:
                    self.keys.append(key)

        # check if temp table present
        self.check_temp(curs)
        # process I,U,D
        self.process_delete(curs, op_map)
        self.process_update(curs, op_map)
        self.process_insert(curs, op_map)
        # truncate or drop temp table
        self.clean_temp(curs)

    def check_temp(self, curs: Cursor) -> None:
        if USE_REAL_TABLE:
            self.temp_present = skytools.exists_table(curs, self.temp)
        else:
            self.temp_present = skytools.exists_temp_table(curs, self.temp)

    def clean_temp(self, curs: Cursor) -> None:
        # delete remaining rows
        if self.temp_present:
            if USE_LONGLIVED_TEMP_TABLES or USE_REAL_TABLE:
                self.truncate(curs)
            else:
                # fscking problems with long-lived temp tables
                self.drop(curs)

    def create_temp(self, curs: Cursor) -> bool:
        """ check if temp table exists. Returns False if using existing temp
        table and True if creating new
        """
        if USE_LONGLIVED_TEMP_TABLES or USE_REAL_TABLE:
            if self.temp_present:
                self.log.debug("bulk: Using existing temp table %s", self.temp)
                return False
        self.create(curs)
        self.temp_present = True
        return True

    def bulk_insert(self, curs: Cursor, data: DictRows, table: Optional[str] = None) -> None:
        """Copy data to table. If table not provided, use temp table.
        When re-using existing temp table, it is always truncated first and
        analyzed after copy.
        """
        if not data:
            return
        _use_temp = table is None
        xtable = self.temp if table is None else table
        # if table not specified use temp
        if _use_temp:
            # truncate when re-using existing table
            if not self.create_temp(curs):
                self.truncate(curs)
        self.log.debug("bulk: COPY %d rows into %s", len(data), xtable)
        skytools.magic_insert(curs, xtable, data, self.fields,
                              quoted_table=True)
        if _use_temp and self.run_analyze:
            self.analyze(curs)

    def find_dist_fields(self, curs: Cursor) -> List[str]:
        """Find GP distribution keys"""
        if not skytools.exists_table(curs, "pg_catalog.gp_distribution_policy"):
            return []
        schema, name = skytools.fq_name_parts(self.table)
        qry = "select a.attname"\
            "  from pg_class t, pg_namespace n, pg_attribute a,"\
            "       gp_distribution_policy p"\
            " where n.oid = t.relnamespace"\
            "   and p.localoid = t.oid"\
            "   and a.attrelid = t.oid"\
            "   and a.attnum = any(p.attrnums)"\
            "   and n.nspname = %s and t.relname = %s"
        curs.execute(qry, [schema, name])
        res = []
        for row in curs.fetchall():
            res.append(row[0])
        return res


LOADERS = {'direct': DirectLoader, 'bulk': BulkLoader}


#------------------------------------------------------------------------------
# ROW HANDLERS
#------------------------------------------------------------------------------

class RowHandler:
    log: logging.Logger
    table_map: Dict[str, BaseLoader]

    def __init__(self, log: logging.Logger) -> None:
        self.log = log
        self.table_map = {}

    def add_table(self, table: str, ldr_cls: Type[BaseLoader], pkeys: List[str], args: dbdict) -> None:
        self.table_map[table] = ldr_cls(table, pkeys, self.log, args)

    def process(self, table: str, op: str, row: Dict[str, Any]) -> None:
        try:
            self.table_map[table].process(op, row)
        except KeyError:
            raise Exception("No loader for table %s" % table) from None

    def flush(self, curs: Cursor) -> None:
        for ldr in self.table_map.values():
            ldr.flush(curs)


class KeepAllRowHandler(RowHandler):
    def process(self, table: str, op: str, row: Dict[str, Any]) -> None:
        """Keep all row versions.

        Updates are changed to inserts, deletes are ignored.
        Makes sense only for partitioned tables.
        """
        if op == 'U':
            op = 'I'
        elif op == 'D':
            return
        super().process(table, op, row)


class KeepLatestRowHandler(RowHandler):
    def process(self, table: str, op: str, row: Dict[str, Any]) -> None:
        """Keep latest row version.

        Updates are changed to delete + insert
        Makes sense only for partitioned tables.
        """
        if op == 'U':
            super().process(table, 'D', row)
            super().process(table, 'I', row)
        elif op == 'I':
            super().process(table, 'I', row)
        elif op == 'D':
            super().process(table, 'D', row)


ROW_HANDLERS = {'plain': RowHandler,
                'keep_all': KeepAllRowHandler,
                'keep_latest': KeepLatestRowHandler}


#------------------------------------------------------------------------------
# DISPATCHER
#------------------------------------------------------------------------------

class Dispatcher(ShardHandler):
    _doc_ = """Partitioned loader.
    Splits events into partitions, if requested.
    Then applies them without further processing.
    """
    handler_name = 'dispatch'

    dst_curs: Optional[Cursor]
    ignored_tables: Set[str]
    batch_info: Optional[BatchInfo]
    pkeys: Optional[List[str]]

    @property
    def __doc__(self) -> Optional[str]:
        return self._doc_
    @__doc__.setter
    def __doc__(self, value: Optional[str]) -> None:
        pass

    def __init__(self, table_name: str, args: Dict[str, str], dest_table: str) -> None:

        # compat for dest-table
        dest_table = args.get('table', dest_table)

        super().__init__(table_name, args, dest_table)

        # show args
        self.log.debug("dispatch.init: table_name=%r, args=%r", table_name, args)
        self.ignored_tables = set()
        self.batch_info = None
        self.dst_curs = None
        self.pkeys = None
        # config
        hdlr_cls = ROW_HANDLERS[self.conf.row_mode]
        self.row_handler = hdlr_cls(self.log)

    def _parse_args_from_doc(self) -> List[Tuple[str, str, str]]:
        doc = __doc__
        params_descr: List[Tuple[str, str, str]] = []
        params_found = False
        for line in doc.splitlines():
            ln = line.strip()
            if params_found:
                if ln.startswith("=="):
                    break
                m = re.match(r"^(\w+):$", ln)
                if m:
                    name = m.group(1)
                    expr = text = ""
                elif not params_descr:
                    continue
                else:
                    name, expr, text = params_descr.pop()
                    text += ln + "\n"
                params_descr.append((name, expr, text))
            elif ln == "== HANDLER ARGUMENTS ==":
                params_found = True
        return params_descr

    def get_config(self) -> dbdict:
        """Processes args dict"""
        conf = super().get_config()
        # set table mode
        conf.table_mode = self.get_arg('table_mode', TABLE_MODES)
        conf.analyze = self.get_arg('analyze', [0, 1])
        if conf.table_mode == 'part':
            conf.part_mode = self.get_arg('part_mode', PART_MODES)
            conf.part_field = self.args.get('part_field')
            if conf.part_mode == 'date_field' and not conf.part_field:
                raise Exception('part_mode date_field requires part_field!')
            conf.period = self.get_arg('period', PERIODS)
            conf.part_name = self.args.get('part_name')
            conf.part_template = self.args.get('part_template')
            conf.pre_part = self.args.get('pre_part')
            conf.post_part = self.args.get('post_part')
            conf.part_func = self.args.get('part_func', PART_FUNC_NEW)
            conf.retention_period = self.args.get('retention_period')
            conf.ignore_old_events = self.get_arg('ignore_old_events', [0, 1], 0)
        # set row mode and event types to process
        conf.row_mode = self.get_arg('row_mode', ROW_MODES)
        cf_event_types = self.args.get('event_types', '*')
        if cf_event_types == '*':
            event_types = EVENT_TYPES
        else:
            event_types = [evt.upper() for evt in cf_event_types.split(',')]  # noqa
            for evt in event_types:
                if evt not in EVENT_TYPES:
                    raise Exception('Unsupported operation: %s' % evt)
        conf.event_types = event_types
        # set load handler
        conf.load_mode = self.get_arg('load_mode', LOAD_MODES)
        conf.method = self.get_arg('method', METHODS)
        # fields to skip
        conf.skip_fields = [f.strip().lower()
                            for f in self.args.get('skip_fields', '').split(',')]
        # get fields map (obsolete, for compatibility reasons)
        fields = self.args.get('fields', '*')
        if fields == "*":
            conf.field_map = None
        else:
            conf.field_map = {}
            for fval in fields.split(','):
                tmp = fval.split(':')
                if len(tmp) == 1:
                    conf.field_map[tmp[0]] = tmp[0]
                else:
                    conf.field_map[tmp[0]] = tmp[1]
        return conf

    def _validate_hash_key(self) -> None:
        pass  # no need for hash key when not sharding

    def prepare_batch(self, batch_info: Optional[BatchInfo], dst_curs: Cursor) -> None:
        """Called on first event for this table in current batch."""
        if batch_info is not None and self.conf.table_mode != 'ignore':
            self.batch_info = batch_info
            self.dst_curs = dst_curs
        super().prepare_batch(batch_info, dst_curs)

    def filter_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process with fields skip and map"""
        fskip = self.conf.skip_fields
        fmap = self.conf.field_map
        if fskip:
            data = dict((k, v) for k, v in data.items()
                        if k not in fskip)
        if fmap:
            # when field name not present in source is used then  None (NULL)
            # value is inserted. is it ok?
            data = dict((v, data.get(k)) for k, v in fmap.items())
        return data

    def filter_pkeys(self, pkeys: List[str]) -> List[str]:
        """Process with fields skip and map"""
        fskip = self.conf.skip_fields
        fmap = self.conf.field_map
        if fskip:
            pkeys = [f for f in pkeys if f not in fskip]
        if fmap:
            pkeys = [fmap[p] for p in pkeys if p in fmap]
        return pkeys

    def _process_event(self, ev: Event, sql_queue_func: ApplyFunc, arg: Cursor) -> None:
        """Process a event.
        Event should be added to sql_queue or executed directly.
        """
        if self.conf.table_mode == 'ignore':
            return
        # get data
        data = skytools.db_urldecode(ev.data)
        if len(ev.ev_type) < 2 or ev.ev_type[1] != ':':
            raise Exception('Unsupported event type: %s/extra1=%s/data=%s' % (
                            ev.ev_type, ev.ev_extra1, ev.ev_data))
        op, pkeys = ev.type.split(':', 1)
        if op not in 'IUD':
            raise Exception('Unknown event type: %s' % ev.ev_type)
        # process only operations specified
        if op not in self.conf.event_types:
            #self.log.debug('dispatch.process_event: ignored event type')
            return
        if self.pkeys is None:
            self.pkeys = self.filter_pkeys(pkeys.split(','))
        data = self.filter_data(data)
        # prepare split table when needed
        if self.conf.table_mode == 'part':
            dst, part_time = self.split_format(ev, data)
            if dst in self.ignored_tables:
                return
            if dst not in self.row_handler.table_map:
                self.check_part(dst, part_time)
                if dst in self.ignored_tables:
                    return
        else:
            dst = self.dest_table

        if dst not in self.row_handler.table_map:
            self.row_handler.add_table(dst, LOADERS[self.conf.load_mode],
                                       self.pkeys, self.conf)
        self.row_handler.process(dst, op, data)

    def finish_batch(self, batch_info: BatchInfo, dst_curs: Cursor) -> None:
        """Called when batch finishes."""
        if self.conf.table_mode != 'ignore':
            self.row_handler.flush(dst_curs)
        #super().finish_batch(batch_info, dst_curs)

    def get_part_name(self) -> str:
        # if custom part name template given, use it
        if self.conf.part_name:
            return self.conf.part_name
        parts = ['year', 'month', 'day', 'hour']
        name_parts = ['parent'] + parts[:parts.index(self.conf.period) + 1]
        return '_'.join('%%(%s)s' % part for part in name_parts)

    def split_format(self, ev: Event, data: Dict[str, Any]) -> Tuple[str, datetime.datetime]:
        """Generates part table name from template"""
        assert self.batch_info
        if self.conf.part_mode == 'batch_time':
            dtm = self.batch_info['batch_end']
        elif self.conf.part_mode == 'event_time':
            dtm = ev.ev_time
        elif self.conf.part_mode == 'current_time':
            dtm = datetime.datetime.now()
        elif self.conf.part_mode == 'date_field':
            dt_str = data[self.conf.part_field]
            if dt_str is None:
                raise Exception('part_field(%s) is NULL: %s' % (self.conf.part_field, ev))
            dtm = datetime.datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        else:
            raise UsageError('Bad value for part_mode: %s' % self.conf.part_mode)
        vals = {
            'parent': self.dest_table,
            'year': "%04d" % dtm.year,
            'month': "%02d" % dtm.month,
            'day': "%02d" % dtm.day,
            'hour': "%02d" % dtm.hour,
        }
        return (self.get_part_name() % vals, dtm)

    def check_part(self, dst: str, part_time: datetime.datetime) -> None:
        """Create part table if not exists.

        It part_template present, execute it
        else if part function present in db, call it
        else clone master table"""
        curs = self.dst_curs
        assert curs

        if (self.conf.ignore_old_events and self.conf.retention_period and
                self.is_obsolete_partition(dst, self.conf.retention_period, self.conf.period)):
            self.ignored_tables.add(dst)
            return
        if skytools.exists_table(curs, dst):
            return

        dst = quote_fqident(dst)
        vals = {'dest': dst,
                'part': dst,
                'parent': self.fq_dest_table,
                'pkeys': ",".join(self.pkeys or []),  # quoting?
                # we do this to make sure that constraints for
                # tables who contain a schema will still work
                'schema_table': dst.replace(".", "__"),
                'part_field': self.conf.part_field,
                'part_time': part_time,
                'period': self.conf.period,
                }

        def exec_with_vals(tmpl: str) -> bool:
            if tmpl:
                sql = tmpl % vals
                curs.execute(sql)
                return True
            return False

        exec_with_vals(self.conf.pre_part)

        if not exec_with_vals(self.conf.part_template):
            self.log.debug('part_template not provided, using part func')
            # if part func exists call it with val arguments
            pfargs = ', '.join('%%(%s)s' % arg for arg in PART_FUNC_ARGS)

            # set up configured function
            pfcall = 'select %s(%s)' % (self.conf.part_func, pfargs)
            have_func = skytools.exists_function(curs, self.conf.part_func, len(PART_FUNC_ARGS))

            # backwards compat
            if not have_func and self.conf.part_func == PART_FUNC_NEW:
                pfcall = 'select %s(%s)' % (PART_FUNC_OLD, pfargs)
                have_func = skytools.exists_function(curs, PART_FUNC_OLD, len(PART_FUNC_ARGS))

            if have_func:
                self.log.debug('check_part.exec: func: %s, args: %s', pfcall, vals)
                curs.execute(pfcall, vals)
            else:
                #
                # Otherwise create simple clone.
                #
                # FixMe: differences from create_partitions():
                # - check constraints
                # - inheritance
                #
                self.log.debug('part func %s not found, cloning table', self.conf.part_func)
                struct = TableStruct(curs, self.dest_table)
                struct.create(curs, T_ALL, dst)

        exec_with_vals(self.conf.post_part)
        self.log.info("Created table: %s", dst)

        if self.conf.retention_period:
            dropped = self.drop_obsolete_partitions(self.dest_table, self.conf.retention_period, self.conf.period)
            if self.conf.ignore_old_events and dropped:
                for tbl in dropped:
                    self.ignored_tables.add(tbl)
                    if tbl in self.row_handler.table_map:
                        del self.row_handler.table_map[tbl]

    def drop_obsolete_partitions(self, parent_table: str, retention_period: str, partition_period: str) -> List[str]:
        """ Drop obsolete partitions of partition-by-date parent table.
        """
        curs = self.dst_curs
        assert curs

        func = RETENTION_FUNC
        args = [parent_table, retention_period, partition_period]
        sql = "select " + func + "(%s, %s, %s)"
        self.log.debug("func: %s, args: %s", func, args)
        curs.execute(sql, args)
        res = [row[0] for row in curs.fetchall()]
        if res:
            self.log.info("Dropped tables: %s", ", ".join(res))
        return res

    def is_obsolete_partition(self, partition_table: str, retention_period: str, partition_period: str) -> bool:
        """ Test partition name of partition-by-date parent table.
        """
        curs = self.dst_curs
        assert curs

        func = "londiste.is_obsolete_partition"
        args = [partition_table, retention_period, partition_period]
        sql = "select " + func + "(%s, %s, %s)"
        self.log.debug("func: %s, args: %s", func, args)
        curs.execute(sql, args)
        res = curs.fetchone()[0]
        if res:
            self.log.info("Ignored table: %s", partition_table)
        return res

    def real_copy(self, tablename: str, src_curs: Cursor, dst_curs: Cursor, column_list: Sequence[str]) -> Tuple[int, int]:
        """do actual table copy and return tuple with number of bytes and rows
        copied
        """
        _src_cols = _dst_cols = column_list
        condition = self.get_copy_condition(src_curs, dst_curs)

        if self.conf.skip_fields:
            _src_cols = [col for col in column_list
                         if col not in self.conf.skip_fields]
            _dst_cols = _src_cols

        if self.conf.field_map:
            _src_cols = [col for col in _src_cols if col in self.conf.field_map]
            _dst_cols = [self.conf.field_map[col] for col in _src_cols]

        return skytools.full_copy(tablename, src_curs, dst_curs,
                                  _src_cols, condition,
                                  dst_tablename=self.dest_table,
                                  dst_column_list=_dst_cols)

    def real_copy_threaded(
        self,
        src_real_table: str,
        src_curs: Cursor,
        dst_db_connstr: str,
        column_list: Sequence[str],
        config_file: str,
        config_section: str,
        parallel: int = 1,
    ) -> Tuple[int, int]:
        with skytools.connect_database(dst_db_connstr) as dst_db:
            with dst_db.cursor() as dst_curs:
                condition = self.get_copy_condition(src_curs, dst_curs)
            dst_db.commit()

        _src_cols = _dst_cols = column_list
        if self.conf.skip_fields:
            _src_cols = [col for col in column_list
                         if col not in self.conf.skip_fields]
            _dst_cols = _src_cols

        if self.conf.field_map:
            _src_cols = [col for col in _src_cols if col in self.conf.field_map]
            _dst_cols = [self.conf.field_map[col] for col in _src_cols]

        return londiste.util.full_copy_parallel(
            src_real_table, src_curs,
            dst_db_connstr=dst_db_connstr,
            dst_tablename=self.dest_table,
            condition=condition,
            column_list=_src_cols,
            dst_column_list=_dst_cols,
            parallel=parallel,
        )


# add arguments' description to handler's docstring
def _install_handler_docstrings(dst_cls: Type[BaseHandler]) -> None:
    found = False
    for line in __doc__.splitlines():
        if line.startswith("== HANDLER ARGUMENTS =="):
            found = True
        if found:
            dst_cls._doc_ += "\n" + line


_install_handler_docstrings(Dispatcher)

#------------------------------------------------------------------------------
# register handler class
#------------------------------------------------------------------------------


__londiste_handlers__ = [Dispatcher]


#------------------------------------------------------------------------------
# build set of handlers with different default values for easier use
#------------------------------------------------------------------------------


LOAD = {
    '': {'load_mode': 'direct'},
    'bulk': {'load_mode': 'bulk'}
}
PERIOD = {
    'hourly': {'period': 'hour'},
    'daily': {'period': 'day'},
    'monthly': {'period': 'month'},
    'yearly': {'period': 'year'},
}
MODE = {
    'event': {'part_mode': 'event_time'},
    'batch': {'part_mode': 'batch_time'},
    'field': {'part_mode': 'date_field'},
    'time': {'part_mode': 'current_time'},
}
BASE = {
    'table_mode': 'part',
    'row_mode': 'keep_latest',
}


def set_handler_doc(cls: Type[BaseHandler], handler_defs: Dict[str, str]) -> None:
    """ generate handler docstring """
    cls._doc_ = "Custom dispatch handler with default args.\n\n" \
                "Parameters:\n"
    for k, v in handler_defs.items():
        cls._doc_ += "  %s = %s\n" % (k, v)


def _generate_handlers() -> None:
    for load, load_dict in LOAD.items():
        for period, period_dict in PERIOD.items():
            for mode, mode_dict in MODE.items():
                handler_name = '_'.join(p for p in (load, period, mode) if p)

                # define creator func to keep default dicts in separate context
                def create_handler(_handler_name: str, _load_dict: Dict[str, str], _period_dict: Dict[str, str],
                                   _mode_dict: Dict[str, str]) -> None:
                    default = update(_mode_dict, _period_dict, _load_dict, BASE)

                    @handler_args(_handler_name, Dispatcher)
                    def handler_func(args: Dict[str, str]) -> Dict[str, str]:
                        return update(args, default)

                    #assert handler_func   # avoid 'unused' warning, decorator registers it

                create_handler(handler_name, load_dict, period_dict, mode_dict)
                hcls = __londiste_handlers__[-1]   # it was just added
                defs = update(mode_dict, period_dict, load_dict, BASE)
                set_handler_doc(hcls, defs)


_generate_handlers()


@handler_args('bulk_direct', Dispatcher)
def bulk_direct_handler(args: Dict[str, str]) -> Dict[str, str]:
    return update(args, {'load_mode': 'bulk', 'table_mode': 'direct'})


set_handler_doc(__londiste_handlers__[-1], {'load_mode': 'bulk', 'table_mode': 'direct'})


@handler_args('direct', Dispatcher)
def direct_handler(args: Dict[str, str]) -> Dict[str, str]:
    return update(args, {'load_mode': 'direct', 'table_mode': 'direct'})


set_handler_doc(__londiste_handlers__[-1], {'load_mode': 'direct', 'table_mode': 'direct'})

