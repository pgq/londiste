
"""Table handler.

Per-table decision how to create trigger, copy data and apply events.
"""

from typing import List, Dict, Any, Sequence, Tuple, Optional, Union, Callable, Type

import json
import logging
import sys

import skytools
from skytools.basetypes import Cursor, Connection
from skytools import dbdict

from pgq import Event
from pgq.baseconsumer import BatchInfo

import londiste.util

ApplyFunc = Callable[[str, Cursor], None]


_ = """

-- redirect & create table
partition by batch_time
partition by date field

-- sql handling:
cube1 - I/U/D -> partition, insert
cube2 - I/U/D -> partition, del/insert
field remap
name remap

bublin filter
- replay: filter events
- copy: additional where
- add: add trigger args

multimaster
- replay: conflict handling, add fncall to sql queue?
- add: add 'backup' arg to trigger

plain londiste:
- replay: add to sql queue

"""

__all__ = ['RowCache', 'BaseHandler', 'build_handler',
           'create_handler_string', 'BatchInfo',
           'Event', 'Cursor', 'Connection']


class RowCache:

    table_name: str
    keys: Dict[str, int]
    rows: List[Tuple[Any, ...]]

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.keys = {}
        self.rows = []

    def add_row(self, d: Dict[str, Any]) -> None:
        row = [None] * len(self.keys)
        for k, v in d.items():
            try:
                row[self.keys[k]] = v
            except KeyError:
                i = len(row)
                self.keys[k] = i
                row.append(v)
        self.rows.append(tuple(row))

    def get_fields(self) -> Sequence[str]:
        row: List[str] = [""] * len(self.keys)
        for k, i in self.keys.items():
            row[i] = k
        return tuple(row)

    def apply_rows(self, curs: Cursor) -> None:
        fields = self.get_fields()
        skytools.magic_insert(curs, self.table_name, self.rows, fields)


class BaseHandler:
    """Defines base API, does nothing.
    """
    handler_name = 'nop'
    log = logging.getLogger('basehandler')

    table_name: str
    dest_table: str
    fq_table_name: str
    fq_dest_table: str
    args: Dict[str, str]
    conf: skytools.dbdict
    _doc_: str = ''

    def __init__(self, table_name: str, args: Optional[Dict[str,str]], dest_table: Optional[str]) -> None:
        self.table_name = table_name
        self.dest_table = dest_table or table_name
        self.fq_table_name = skytools.quote_fqident(self.table_name)
        self.fq_dest_table = skytools.quote_fqident(self.dest_table)
        self.args = args if args else {}
        self._check_args(self.args)
        self.conf = self.get_config()

    def _parse_args_from_doc(self) -> List[Tuple[str, str, str]]:
        doc = self.__doc__ or ""
        params_descr: List[Tuple[str, str, str]] = []
        params_found = False
        for line in doc.splitlines():
            ln = line.strip()
            if params_found:
                if ln == "":
                    break
                descr = ln.split(None, 1)
                name, sep, ___rest = descr[0].partition('=')
                if sep:
                    expr = descr[0].rstrip(":")
                    text = descr[1].lstrip(":- \t")
                else:
                    name, expr, text = params_descr.pop()
                    text += "\n" + ln
                params_descr.append((name, expr, text))
            elif ln == "Parameters:":
                params_found = True
        return params_descr

    def _check_args(self, args: Dict[str, str]) -> None:
        self.valid_arg_names = []
        passed_arg_names = args.keys() if args else []
        args_from_doc = self._parse_args_from_doc()
        if args_from_doc:
            self.valid_arg_names = [arg[0] for arg in args_from_doc]
        invalid = set(passed_arg_names) - set(self.valid_arg_names)
        if invalid:
            raise ValueError("Invalid handler argument: %s" % list(invalid))

    def get_arg(self, name: str, value_list: Union[List[str], List[int]], default: Optional[Union[str, int]]=None) -> Union[str, int]:
        """ Return arg value or default; also check if value allowed. """
        default = default or value_list[0]
        val = type(default)(self.args.get(name, default))
        if val not in value_list:
            raise Exception('Bad argument %s value %r' % (name, val))
        return val

    def get_config(self) -> dbdict:
        """ Process args dict (into handler config). """
        conf = skytools.dbdict()
        return conf

    def add(self, trigger_arg_list: List[str]) -> None:
        """Called when table is added.

        Can modify trigger args.
        """
        pass

    def reset(self) -> None:
        """Called before starting to process a batch.
        Should clean any pending data.
        """
        pass

    def prepare_batch(self, batch_info: Optional[BatchInfo], dst_curs: Cursor) -> None:
        """Called on first event for this table in current batch."""
        pass

    def process_event(self, ev: Event, sql_queue_func: ApplyFunc, dst_curs: Cursor) -> None:
        """Process a event.

        Event should be added to sql_queue or executed directly.
        """
        pass

    def finish_batch(self, batch_info: BatchInfo, dst_curs: Cursor) -> None:
        """Called when batch finishes."""
        pass

    def get_copy_condition(self, src_curs: Cursor, dst_curs: Cursor) -> str:
        """ Use if you want to filter data """
        return ''

    def real_copy(self, src_tablename: str, src_curs: Cursor, dst_curs: Cursor, column_list: List[str]) -> Tuple[int, int]:
        """do actual table copy and return tuple with number of bytes and rows
        copied
        """
        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename=self.dest_table)

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

        return londiste.util.full_copy_parallel(
            src_real_table, src_curs,
            dst_db_connstr=dst_db_connstr,
            dst_tablename=self.dest_table,
            column_list=column_list,
            condition=condition,
            parallel=parallel,
        )

    def needs_table(self) -> bool:
        """Does the handler need the table to exist on destination."""
        return True

    @classmethod
    def load_conf(cls, cf: skytools.Config) -> None:
        """Load conf."""
        pass

    def get_copy_event(self, ev: Event, queue_name: str) -> Optional[Event]:
        """Get event copy for destination queue."""
        return ev


class TableHandler(BaseHandler):
    """Default Londiste handler, inserts events into tables with plain SQL.

    Parameters:
      encoding=ENC - Validate and fix incoming data from encoding.
                     Only 'utf8' is supported at the moment.
      ignore_truncate=BOOL - Ignore truncate event. Default: 0; Values: 0,1.
    """
    handler_name = 'londiste'

    sql_command = {
        'I': "insert into %s %s;",
        'U': "update only %s set %s;",
        'D': "delete from only %s where %s;",
    }

    allow_sql_event = 1

    def __init__(self, table_name: str, args: Dict[str, str], dest_table: Optional[str]) -> None:
        super().__init__(table_name, args, dest_table)

        enc = args.get('encoding')
        if enc:
            raise ValueError("encoding validator not supported")

    def get_config(self) -> dbdict:
        conf = super().get_config()
        conf.ignore_truncate = self.get_arg('ignore_truncate', [0, 1], 0)
        return conf

    def process_event(self, ev: Event, sql_queue_func: ApplyFunc, dst_curs: Cursor) -> None:
        row = self.parse_row_data(ev)
        if len(ev.type) == 1:
            # sql event
            fqname = self.fq_dest_table
            fmt = self.sql_command[ev.type]
            sql = fmt % (fqname, row)
        else:
            if ev.type[0] == '{':
                jtype = json.loads(ev.type)
                pklist = jtype['pkey']
                op = jtype['op'][0]
            else:
                # urlenc event
                pklist = ev.type[2:].split(',')
                op = ev.type[0]
            tbl = self.dest_table
            if op == 'I':
                sql = skytools.mk_insert_sql(row, tbl, pklist)
            elif op == 'U':
                sql = skytools.mk_update_sql(row, tbl, pklist)
            elif op == 'D':
                sql = skytools.mk_delete_sql(row, tbl, pklist)

        sql_queue_func(sql, dst_curs)

    def parse_row_data(self, ev: Event) -> Dict[str, Any]:
        """Extract row data from event, with optional encoding fixes.

        Returns either string (sql event) or dict (urlenc event).
        """

        if len(ev.type) == 1:
            if not self.allow_sql_event:
                raise Exception('SQL events not supported by this handler')
            return ev.data
        elif ev.data[0] == '{':
            row = json.loads(ev.data)
            return row
        else:
            row = skytools.db_urldecode(ev.data)
            return row

    def real_copy(self, src_tablename: str, src_curs: Cursor, dst_curs: Cursor, column_list: List[str]) -> Tuple[int, int]:
        """do actual table copy and return tuple with number of bytes and rows
        copied
        """

        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename=self.dest_table)

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

        return londiste.util.full_copy_parallel(
            src_real_table, src_curs,
            dst_db_connstr=dst_db_connstr,
            column_list=column_list,
            condition=condition,
            dst_tablename=self.dest_table,
            parallel=parallel,
        )



#
# handler management
#

_handler_map: Dict[str, Type[BaseHandler]] = {
    'londiste': TableHandler,
}


def register_handler_module(modname: str, cf: skytools.Config) -> None:
    """Import and module and register handlers."""
    try:
        __import__(modname)
    except ImportError:
        print("Failed to load handler module: %s" % (modname,))
        return
    m = sys.modules[modname]
    for h in getattr(m, "__londiste_handlers__"):
        h.load_conf(cf)
        _handler_map[h.handler_name] = h


def _parse_arglist(arglist: Sequence[str]) -> Dict[str, str]:
    args = {}
    for arg in arglist or []:
        key, _, val = arg.partition('=')
        key = key.strip()
        if key in args:
            raise Exception('multiple handler arguments: %s' % key)
        args[key] = val.strip()
    return args


def create_handler_string(name: str, arglist: Sequence[str]) -> str:
    handler = name
    if name.find('(') >= 0:
        raise Exception('invalid handler name: %s' % name)
    if arglist:
        args = _parse_arglist(arglist)
        astr = skytools.db_urlencode(args)
        handler = '%s(%s)' % (handler, astr)
    return handler


def _parse_handler(hstr: str) -> Tuple[str, Dict[str, str]]:
    """Parse result of create_handler_string()."""
    args = {}
    name = hstr
    pos = hstr.find('(')
    if pos > 0:
        name = hstr[: pos]
        if hstr[-1] != ')':
            raise Exception('invalid handler format: %s' % hstr)
        astr = hstr[pos + 1: -1]
        if astr:
            astr = astr.replace(',', '&')
            args = {
                k: v
                for k, v in skytools.db_urldecode(astr).items()
                if v is not None
            }
    return (name, args)


def build_handler(tblname: str, hstr: str, dest_table: Optional[str] = None) -> BaseHandler:
    """Parse and initialize handler.

    hstr is result of create_handler_string()."""
    hname, args = _parse_handler(hstr)
    # when no handler specified, use londiste
    hname = hname or 'londiste'
    klass = _handler_map[hname]
    if not dest_table:
        dest_table = tblname
    return klass(tblname, args, dest_table)


#def load_handler_modules(cf: skytools.Config) -> None:
#    """Load and register modules from config."""
#    from londiste.handlers import DEFAULT_HANDLERS
#    for m in DEFAULT_HANDLERS:
#        register_handler_module(m, cf)
#
#    for m in cf.getlist('handler_modules', []):
#        register_handler_module(m, cf)


def show(mods: Sequence[str]) -> None:
    if not mods:
        for n, kls in _handler_map.items():
            desc = kls.__doc__ or ''
            if desc:
                desc = desc.strip().split('\n', 1)[0]
            print("%s - %s" % (n, desc))
    else:
        for n in mods:
            kls = _handler_map[n]
            desc = kls.__doc__ or ''
            if desc:
                desc = desc.strip()
            print("%s - %s" % (n, desc))

