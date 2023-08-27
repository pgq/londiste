"""Handlers module
"""

import functools
import sys

from typing import List, Callable, Dict, Type, Optional

import skytools
from londiste.handler import BaseHandler, register_handler_module

DEFAULT_HANDLERS: List[str] = []

ArgHandler = Callable[[Dict[str, str]], Dict[str,str]]
ArgWrapper = Callable[[ArgHandler], ArgHandler]

def handler_args(name: str, cls: Type[BaseHandler]) -> ArgWrapper:
    """Handler arguments initialization decorator

    Define successor for handler class cls with func as argument generator
    """
    def wrapper(func: ArgHandler) -> ArgHandler:
        # pylint: disable=unnecessary-dunder-call
        def _init_override(self: BaseHandler, table_name: str, args: Dict[str, str], dest_table: Optional[str]) -> None:
            cls.__init__(self, table_name, func(args.copy()), dest_table)
        dct = {'__init__': _init_override, 'handler_name': name}
        module = sys.modules[cls.__module__]
        newname = '%s_%s' % (cls.__name__, name.replace('.', '_'))
        newcls = type(newname, (cls,), dct)
        setattr(module, newname, newcls)
        getattr(module, "__londiste_handlers__").append(newcls)
        getattr(module, "__all__").append(newname)
        return func
    return wrapper


def update(*p: Dict[str, str]) -> Dict[str, str]:
    """ Update dicts given in params with its predecessor param dict
    in reverse order """
    return functools.reduce(lambda x, y: x.update(y) or x,
                            (p[i] for i in range(len(p) - 1, -1, -1)),
                            {})


def load_handler_modules(cf: skytools.Config) -> None:
    """Load and register modules from config."""
    for m in DEFAULT_HANDLERS:
        register_handler_module(m, cf)

    for m in cf.getlist('handler_modules', []):
        register_handler_module(m, cf)

