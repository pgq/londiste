"""Set up table that sends inserts to queue.

Handlers:

qtable     - dummy handler to setup queue tables. All events are ignored. Use in
             root node.
fake_local - dummy handler to setup queue tables. All events are ignored. Table
             structure is not required. Use in branch/leaf.
qsplitter  - dummy handler to setup queue tables. All events are ignored. Table
             structure is not required. All table events are inserted to
             destination queue, specified with handler arg 'queue'.

"""

from typing import Sequence, Tuple, List, Dict, Optional, Type, Any

from skytools.basetypes import Cursor

import pgq
from pgq.baseconsumer import BatchInfo
from pgq.event import Event
from londiste.handler import BaseHandler, ApplyFunc

__all__ = ['QueueTableHandler', 'QueueSplitterHandler']


class QueueTableHandler(BaseHandler):
    """Queue table handler. Do nothing.

    Trigger: before-insert, skip trigger.
    Event-processing: do nothing.
    """
    handler_name = 'qtable'

    def add(self, trigger_arg_list: List[str]) -> None:
        """Create SKIP and BEFORE INSERT trigger"""
        trigger_arg_list.append('tgflags=BI')
        trigger_arg_list.append('SKIP')
        trigger_arg_list.append('expect_sync')

    def real_copy(self, tablename: str, src_curs: Cursor, dst_curs: Cursor, column_list: List[str]) -> Tuple[int, int]:
        """Force copy not to start"""
        return (0, 0)

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
        return (0, 0)

    def needs_table(self) -> bool:
        return False


class QueueSplitterHandler(BaseHandler):
    """Send events for one table to another queue.

    Parameters:
      queue=QUEUE - Queue name.
    """
    handler_name = 'qsplitter'
    rows: List[Sequence[Any]]

    def __init__(self, table_name: str, args: Dict[str, str], dest_table: Optional[str]) -> None:
        """Init per-batch table data cache."""
        super().__init__(table_name, args, dest_table)
        try:
            self.dst_queue_name = args['queue']
        except KeyError:
            raise Exception('specify queue with handler-arg') from None
        self.rows = []

    def add(self, trigger_arg_list: List[str]) -> None:
        trigger_arg_list.append('virtual_table')

    def prepare_batch(self, batch_info: Optional[BatchInfo], dst_curs: Cursor) -> None:
        """Called on first event for this table in current batch."""
        self.rows = []

    def process_event(self, ev: Event, sql_queue_func: ApplyFunc, dst_curs: Cursor) -> None:
        """Process a event.

        Event should be added to sql_queue or executed directly.
        """
        if self.dst_queue_name is None:
            return

        data = [ev.type, ev.data,
                ev.extra1, ev.extra2, ev.extra3, ev.extra4, ev.time]
        self.rows.append(data)

    def finish_batch(self, batch_info: BatchInfo, dst_curs: Cursor) -> None:
        """Called when batch finishes."""
        if self.dst_queue_name is None:
            return

        fields = ['type', 'data',
                  'extra1', 'extra2', 'extra3', 'extra4', 'time']
        pgq.bulk_insert_events(dst_curs, self.rows, fields, self.dst_queue_name)

    def needs_table(self) -> bool:
        return False


__londiste_handlers__: List[Type[BaseHandler]] = [QueueTableHandler, QueueSplitterHandler]

