"""Virtual Table handler.

Hack to get local=t for a table, but without processing any events.
"""

from typing import List, Type

from londiste.handler import BaseHandler

__all__ = ['VirtualTableHandler', 'FakeLocalHandler']


class VirtualTableHandler(BaseHandler):
    __doc__ = __doc__
    handler_name = 'vtable'

    def add(self, trigger_arg_list: List[str]) -> None:
        trigger_arg_list.append('virtual_table')

    def needs_table(self) -> bool:
        return False


class FakeLocalHandler(VirtualTableHandler):
    """Deprecated compat name for vtable."""
    handler_name = 'fake_local'


__londiste_handlers__: List[Type[BaseHandler]] = [VirtualTableHandler, FakeLocalHandler]

