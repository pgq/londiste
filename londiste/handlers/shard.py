"""Event filtering by hash, for partitioned databases.

Parameters:
  key=COLUMN: column name to use for hashing
  hash_key=COLUMN: column name to use for hashing (overrides 'key' parameter)
  encoding=ENC: validate and fix incoming data (only utf8 supported atm)
  ignore_truncate=BOOL: ignore truncate event, default: 0, values: 0,1
  disable_replay=BOOL: no replay to table, just copy events.  default: 0, values: 0 1

On root node:
* Hash of key field will be added to ev_extra3.
  This is implemented by adding additional trigger argument:
        ev_extra3='hash='||hashfunc(key_column)

On branch/leaf node:
* On COPY time, the SELECT on provider side gets filtered by hash.
* On replay time, the events gets filtered by looking at hash in ev_extra3.

Local config:
* Local hash value and mask are loaded from partconf.conf table.

Custom parameters from config file
* shard_hash_func: function to use for hashing
* shard_info_sql: SQL query to get (shard_nr, shard_mask, shard_count) values.

"""

from typing import Dict, List, Sequence, Tuple, Optional, Type

import skytools
from skytools.basetypes import Cursor
from pgq.baseconsumer import BatchInfo
from pgq.event import Event

from londiste.handler import TableHandler, BaseHandler, ApplyFunc

__all__ = ['ShardHandler', 'PartHandler']

_SHARD_HASH_FUNC = 'partconf.get_hash_raw'
_SHARD_INFO_SQL = "select shard_nr, shard_mask, shard_count from partconf.conf"
_SHARD_NR = None    # part number of local node
_SHARD_MASK = None  # max part nr (atm)


class ShardHandler(TableHandler):
    __doc__: Optional[str] = __doc__
    handler_name = 'shard'

    DEFAULT_HASH_EXPR = "%s(%s)"

    hash_key: str
    hash_expr: str
    disable_replay: bool

    def __init__(self, table_name: str, args: Dict[str, str], dest_table: str) -> None:
        super().__init__(table_name, args, dest_table)

        # primary key columns
        hash_key = args.get('hash_key', args.get('key'))
        if hash_key is None:
            raise Exception('Specify hash key field as hash_key argument')
        self.hash_key = hash_key

        # hash function & full expression
        self.hash_expr = self.DEFAULT_HASH_EXPR % (
            skytools.quote_fqident(_SHARD_HASH_FUNC),
            skytools.quote_ident(self.hash_key or ''))
        self.hash_expr = args.get('hash_expr', self.hash_expr)

        disable_replay = args.get('disable_replay', 'false')
        self.disable_replay = disable_replay in ('true', '1')

    @classmethod
    def load_conf(cls, cf: skytools.Config) -> None:
        global _SHARD_HASH_FUNC, _SHARD_INFO_SQL

        _SHARD_HASH_FUNC = cf.get("shard_hash_func", _SHARD_HASH_FUNC)
        _SHARD_INFO_SQL = cf.get("shard_info_sql", _SHARD_INFO_SQL)

    def add(self, trigger_arg_list: List[str]) -> None:
        """Let trigger put hash into extra3"""
        arg = "ev_extra3='hash='||%s" % self.hash_expr
        trigger_arg_list.append(arg)
        super().add(trigger_arg_list)

    def is_local_shard_event(self, ev: Event) -> bool:
        assert _SHARD_MASK is not None
        if ev.extra3 is None:
            raise ValueError("handlers.shard: extra3 not filled on %s" % (self.table_name,))
        meta = skytools.db_urldecode(ev.extra3)
        meta_hash = meta.get('hash')
        if meta_hash is None:
            raise ValueError("handlers.shard: extra3 does not have 'hash' key")
        is_local = (int(meta_hash) & _SHARD_MASK) == _SHARD_NR
        self.log.debug('shard.process_event: meta=%r, shard_nr=%i, mask=%i, is_local=%r',
                       meta, _SHARD_NR, _SHARD_MASK, is_local)
        return is_local

    def prepare_batch(self, batch_info: Optional[BatchInfo], dst_curs: Cursor) -> None:
        """Called on first event for this table in current batch."""
        if _SHARD_MASK is None:
            self.load_shard_info(dst_curs)
        super().prepare_batch(batch_info, dst_curs)

    def process_event(self, ev: Event, sql_queue_func: ApplyFunc, dst_curs: Cursor) -> None:
        """Filter event by hash in extra3, apply only if for local shard."""
        if self.disable_replay:
            return
        if self.is_local_shard_event(ev):
            super().process_event(ev, sql_queue_func, dst_curs)

    def get_copy_condition(self, src_curs: Cursor, dst_curs: Cursor) -> str:
        """Prepare the where condition for copy and replay filtering"""
        self.load_shard_info(dst_curs)
        assert _SHARD_MASK is not None
        assert _SHARD_NR is not None
        expr = "(%s & %d) = %d" % (self.hash_expr, _SHARD_MASK, _SHARD_NR)
        self.log.debug('shard: copy_condition=%r', expr)
        return expr

    def load_shard_info(self, curs: Cursor) -> None:
        """Load part/slot info from database."""
        global _SHARD_NR, _SHARD_MASK

        curs.execute(_SHARD_INFO_SQL)
        row = curs.fetchone()
        shard_nr: Optional[int] = row[0]
        shard_mask: Optional[int] = row[1]
        shard_count: Optional[int] = row[2]

        if shard_nr is None or shard_mask is None or shard_count is None:
            raise Exception('Error loading shard info')
        if shard_count & shard_mask != 0 or shard_mask + 1 != shard_count:
            raise Exception('Invalid shard info')
        if shard_nr < 0 or shard_nr >= shard_count:
            raise Exception('Invalid shard nr')

        _SHARD_NR = shard_nr
        _SHARD_MASK = shard_mask

    def get_copy_event(self, ev: Event, queue_name: str) -> Optional[Event]:
        if self.is_local_shard_event(ev):
            return ev
        return None

    def real_copy(self, tablename: str, src_curs: Cursor, dst_curs: Cursor, column_list: List[str]) -> Tuple[int, int]:
        """Force copy not to start"""
        if self.disable_replay:
            return (0, 0)
        return super().real_copy(tablename, src_curs, dst_curs, column_list)

    def real_copy_threaded(
        self,
        src_real_table: str,
        src_curs: Cursor,
        dst_db_connstr: str,
        common_cols: Sequence[str],
        config_file: str,
        config_section: str,
        parallel: int = 1,
    ) -> Tuple[int, int]:
        if self.disable_replay:
            return (0, 0)
        return super().real_copy_threaded(
            src_real_table, src_curs, dst_db_connstr, common_cols,
            config_file, config_section, parallel
        )

    def needs_table(self) -> bool:
        if self.disable_replay:
            return False
        return True


class PartHandler(ShardHandler):
    __doc__ = "Deprecated compat name for shard handler.\n" + __doc__.split('\n', 1)[1]
    handler_name = 'part'


# register handler class
__londiste_handlers__: List[Type[BaseHandler]] = [ShardHandler, PartHandler]

