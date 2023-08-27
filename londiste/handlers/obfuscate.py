"""Handler that uses keyed-hash to obfuscate data.

To use set in londiste.ini:

    handler_modules = londiste.handlers.obfuscate
    obfuscator_map = rules.yaml
    obfuscator_key = seedForHash

then add table with:
  londiste add-table xx --handler="obfuscate"

"""

import json
import uuid
from hashlib import blake2s

from typing import Dict, Any, Sequence, Tuple, Optional, List, cast

from skytools.basetypes import Cursor, DictRow
import skytools
import yaml

from pgq.event import Event
from londiste.handler import TableHandler
import londiste.util


__all__ = ['Obfuscator']

_KEY = b''

BOOL = 'bool'
KEEP = 'keep'
JSON = 'json'
HASH32 = 'hash32'
HASH64 = 'hash64'
HASH128 = 'hash'
SKIP = 'skip'

RuleDict = Dict[str, Any]


def as_bytes(data: Any) -> bytes:
    """Convert input string or json value into bytes.
    """
    if isinstance(data, str):
        return data.encode('utf8')
    if isinstance(data, int):
        return b'%d' % data
    if isinstance(data, float):
        # does not work - pgsql repr may differ
        return b'%r' % data
    if isinstance(data, bool):
        # may work but needs to be in sync with copy and event
        # only 2 output hashes..
        return data and b't' or b'f'
    # no point hashing str() of list or dict
    raise ValueError('Invalid input type for hashing: %s' % type(data))


def hash32(data: Any) -> Optional[int]:
    """Returns hash as 32-bit signed int.
    """
    if data is None:
        return None
    hash_bytes = blake2s(as_bytes(data), digest_size=4, key=_KEY).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)


def hash64(data: Any) -> Optional[int]:
    """Returns hash as 64-bit signed int.
    """
    if data is None:
        return None
    hash_bytes = blake2s(as_bytes(data), digest_size=8, key=_KEY).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)


def hash128(data: Any) -> Optional[str]:
    """Returns hash as 128-bit variant 0 uuid.
    """
    if data is None:
        return None
    hash_bytes = blake2s(as_bytes(data), digest_size=16, key=_KEY).digest()
    hash_int = int.from_bytes(hash_bytes, byteorder='big')

    # rfc4122 variant bit:
    # normal uuids are variant==1 (X >= 8), make this variant==0 (X <= 7)
    # uuid: ........-....-....-X...-............
    hash_int &= ~(0x8000 << 48)

    return str(uuid.UUID(int=hash_int))


def data_to_dict(data: str, column_list: Sequence[str]) -> Dict[str, Any]:
    """Convert data received from copy to dict
    """
    if data[-1] == '\n':
        data = data[:-1]

    vals = [skytools.unescape_copy(value) for value in data.split('\t')]
    row = dict(zip(column_list, vals))
    return row


def obf_vals_to_data(obf_vals: Sequence[Optional[str]]) -> str:
    """Converts obfuscated values back to copy data
    """
    vals = [skytools.quote_copy(value) for value in obf_vals]
    obf_data = '\t'.join(vals) + '\n'
    return obf_data


def obf_json(json_data: Any, rule_data: RuleDict) -> Any:
    """JSON cleanup.

    >>> obf_json({'a': 1, 'b': 2, 'c': 3}, {'a': 'keep', 'b': 'hash'})
    {'a': 1, 'b': 'da0f3012-9a91-a079-484b-883a64e535df'}
    >>> obf_json({'a': {'b': {'c': 3}}}, {'a': {}})
    >>> obf_json({'a': {'b': {'c': 3}}}, {'a': {'b': {'c': 'hash'}}})
    {'a': {'b': {'c': 'ad8f95d3-1e86-689a-24aa-54dbb60d022e'}}}
    >>> obf_json({'a': {'b': {'c': 3}}, 'd': []}, {'a': {'b': {'c': 'skip'}}, 'd': 'keep'})
    {'d': []}
    """
    if isinstance(rule_data, dict):
        if not isinstance(json_data, dict):
            return None
        result = {}
        for rule_key, rule_value in rule_data.items():
            val = obf_json(json_data.get(rule_key), rule_value)
            if val is not None:
                result[rule_key] = val
        if not result:
            return None
        return result
    if rule_data == KEEP:
        return json_data
    if rule_data == SKIP:
        return None
    if isinstance(json_data, (dict, list)):
        return None
    if rule_data == BOOL:
        if json_data is None:
            return None
        return bool(json_data) and 't' or 'f'
    if rule_data == HASH32:
        return hash32(json_data)
    if rule_data == HASH64:
        return hash64(json_data)
    if rule_data == HASH128:
        return hash128(json_data)
    raise ValueError('Invalid rule value: %r' % rule_data)


class Obfuscator(TableHandler):
    """Default Londiste handler, inserts events into tables with plain SQL.
    """
    handler_name = 'obfuscate'
    obf_map: Dict[str, RuleDict] = {}

    @classmethod
    def load_conf(cls, cf: skytools.Config) -> None:
        global _KEY

        _KEY = as_bytes(cf.get('obfuscator_key', ''))
        with open(cf.getfile('obfuscator_map'), 'r', encoding="utf8") as f:
            cls.obf_map = yaml.safe_load(f)

    def _get_map(self, src_tablename: str, row: Optional[Dict[str, Any]] = None) -> RuleDict:
        """Can be over ridden in inherited classes to implemnt data driven maps
        """
        if src_tablename not in self.obf_map:
            raise KeyError('Source table not in obf_map: %s' % src_tablename)
        return self.obf_map[src_tablename]

    def parse_row_data(self, ev: Event) -> Dict[str, Any]:
        """Extract row data from event, with optional encoding fixes.

        Returns either string (sql event) or dict (urlenc event).
        """
        row = super().parse_row_data(ev)

        rule_data = self._get_map(self.table_name, row)
        dst: Dict[str, Any] = {}
        for field, value in row.items():
            action = rule_data.get(field, SKIP)
            if isinstance(action, dict):
                dst[field] = self.obf_json(value, action)
            elif action == KEEP:
                dst[field] = value
            elif action == SKIP:
                continue
            elif action == BOOL:
                if value is None:
                    dst[field] = value
                else:
                    dst[field] = bool(value) and 't' or 'f'
            elif action == HASH32:
                dst[field] = hash32(value)
            elif action == HASH64:
                dst[field] = hash64(value)
            elif action == HASH128:
                dst[field] = hash128(value)
            else:
                raise ValueError('Invalid value for action: %r' % action)
        return dst

    def obf_json(self, value: Any, rule_data: RuleDict) -> Optional[str]:
        """Recursive obfuscate for json
        """
        if value is None:
            return None
        json_data = json.loads(value)
        obf_data = obf_json(json_data, rule_data)
        if obf_data is None:
            obf_data = {}
        return json.dumps(obf_data)

    def obf_copy_row(self, data: str, column_list: Sequence[str], src_tablename: str) -> str:
        """Apply obfuscation to one row
        """
        row = data_to_dict(data, column_list)
        obf_col_map = self._get_map(src_tablename, row)

        obf_vals: List[Optional[str]] = []
        for field, value in row.items():
            action = obf_col_map.get(field, SKIP)

            if isinstance(action, dict):
                obf_val = self.obf_json(value, action)
                obf_vals.append(obf_val)
                continue
            elif action == KEEP:
                obf_vals.append(value)
                continue
            elif action == SKIP:
                continue

            if value is None:
                obf_vals.append(value)
            elif action == BOOL:
                obf_val = str(bool(value) and 't' or 'f')
                obf_vals.append(obf_val)
            elif action == HASH32:
                obf_val = str(hash32(value))
                obf_vals.append(obf_val)
            elif action == HASH64:
                obf_val = str(hash64(value))
                obf_vals.append(obf_val)
            elif action == HASH128:
                obf_val = hash128(value)
                obf_vals.append(obf_val)
            else:
                raise ValueError('Invalid value for action: %s' % action)

        obf_data = obf_vals_to_data(obf_vals)
        return obf_data

    def real_copy(self, src_tablename: str, src_curs: Cursor, dst_curs: Cursor, column_list: Sequence[str]) -> Tuple[int, int]:
        """Initial copy
        """
        obf_col_map = self._get_map(src_tablename)

        new_list = []
        for col in column_list:
            action = obf_col_map.get(col, SKIP)
            if action != SKIP:
                new_list.append(col)
        column_list = new_list

        def _write_hook(pipe: Any, data: str) -> str:
            return self.obf_copy_row(data, column_list, src_tablename)

        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename=self.dest_table,
                                  write_hook=_write_hook)

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

        obf_col_map = self._get_map(src_real_table)

        new_list = []
        for col in column_list:
            action = obf_col_map.get(col, SKIP)
            if action != SKIP:
                new_list.append(col)
        column_list = new_list

        def _write_hook(pipe: Any, data: str) -> str:
            return self.obf_copy_row(data, column_list, src_real_table)

        return londiste.util.full_copy_parallel(
            src_real_table, src_curs,
            dst_db_connstr=dst_db_connstr,
            dst_tablename=self.dest_table,
            condition=condition,
            column_list=column_list,
            write_hook=_write_hook,
            parallel=parallel,
        )

    def get_copy_event(self, ev: Event, queue_name: str) -> Optional[Event]:
        row = self.parse_row_data(ev)

        ev_data: str
        if len(ev.type) == 1:
            raise ValueError("sql trigger not supported")
        elif ev.data[0] == '{':
            ev_data = skytools.json_encode(row)
        else:
            ev_data = skytools.db_urlencode(row)

        ev_row = dict(ev._event_row.items())
        ev_row['ev_data'] = ev_data
        return Event(queue_name, cast(DictRow, ev_row))


__londiste_handlers__ = [Obfuscator]

if __name__ == '__main__':
    import doctest
    doctest.testmod()

