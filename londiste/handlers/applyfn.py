"""Send all events to a DB function.
"""

from typing import Optional, List, Type

import skytools

from londiste.handler import BaseHandler, BatchInfo, Cursor, Event, ApplyFunc

__all__ = ['ApplyFuncHandler']


class ApplyFuncHandler(BaseHandler):
    """Call DB function to apply event.

    Parameters:
      func_name=NAME - database function name
      func_conf=CONF - database function conf
    """
    handler_name: str = 'applyfn'
    cur_tick: Optional[int] = None

    def prepare_batch(self, batch_info: Optional[BatchInfo], dst_curs: Cursor) -> None:
        if batch_info is not None:
            self.cur_tick = batch_info['tick_id']

    def process_event(self, ev: Event, sql_queue_func: ApplyFunc, qfunc_arg: Cursor) -> None:
        """Ignore events for this table"""
        fn = self.args.get('func_name') or 'undefined'
        fnconf = self.args.get('func_conf', '')

        args = [fnconf, self.cur_tick,
                ev.ev_id, ev.ev_time,
                ev.ev_txid, ev.ev_retry,
                ev.ev_type, ev.ev_data,
                ev.ev_extra1, ev.ev_extra2,
                ev.ev_extra3, ev.ev_extra4]

        qfn = skytools.quote_fqident(fn)
        qargs = [skytools.quote_literal(a) for a in args]
        sql = "select %s(%s);" % (qfn, ', '.join(qargs))
        self.log.debug('applyfn.sql: %s', sql)
        sql_queue_func(sql, qfunc_arg)

#------------------------------------------------------------------------------
# register handler class
#------------------------------------------------------------------------------


__londiste_handlers__: List[Type[BaseHandler]] = [ApplyFuncHandler]

