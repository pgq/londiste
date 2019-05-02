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

import yaml
import skytools
from londiste.handler import TableHandler

__all__ = ['Obfuscator']

_KEY = b''

KEEP = 'keep'
JSON = 'json'
HASH32 = 'hash32'
HASH64 = 'hash64'
HASH128 = 'hash'
SKIP = 'skip'

def as_bytes(data):
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

def hash32(data):
    """Returns hash as 32-bit signed int.
    """
    if data is None:
        return None
    hash_bytes = blake2s(as_bytes(data), digest_size=4, key=_KEY).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)

def hash64(data):
    """Returns hash as 64-bit signed int.
    """
    if data is None:
        return None
    hash_bytes = blake2s(as_bytes(data), digest_size=8, key=_KEY).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)

def hash128(data):
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

def obf_json(json_data, rule_data):
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
    elif rule_data == KEEP:
        return json_data
    elif rule_data == SKIP:
        return None
    elif isinstance(json_data, (dict, list)):
        return None
    elif rule_data == HASH32:
        return hash32(json_data)
    elif rule_data == HASH64:
        return hash64(json_data)
    elif rule_data == HASH128:
        return hash128(json_data)
    raise ValueError('Invalid rule value: %r' % rule_data)

class Obfuscator(TableHandler):
    """Default Londiste handler, inserts events into tables with plain SQL.
    """
    handler_name = 'obfuscate'
    obf_map = {}

    @classmethod
    def load_conf(cls, cf):
        global _KEY

        _KEY = as_bytes(cf.get('obfuscator_key', ''))
        with open(cf.getfile('obfuscator_map'), 'r') as f:
            cls.obf_map = yaml.safe_load(f)

    def _validate(self, src_tablename, column_list):
        """Warn if column names in keep list are not in column list
        """
        if src_tablename not in self.obf_map:
            raise KeyError('Source tabel not in obf_map: %s' % src_tablename)

    def parse_row_data(self, ev):
        """Extract row data from event, with optional encoding fixes.

        Returns either string (sql event) or dict (urlenc event).
        """
        row = super(Obfuscator, self).parse_row_data(ev)
        self._validate(self.table_name, row.keys())

        rule_data = self.obf_map[self.table_name]
        dst = {}
        for field, value in row.items():
            action = rule_data.get(field, SKIP)
            if isinstance(action, dict):
                dst[field] = self.obf_json(value, action)
            elif action == KEEP:
                dst[field] = value
            elif action == SKIP:
                continue
            elif action == HASH32:
                dst[field] = hash32(value)
            elif action == HASH64:
                dst[field] = hash64(value)
            elif action == HASH128:
                dst[field] = hash128(value)
            else:
                raise ValueError('Invalid value for action: %r' % action)
        return dst

    def obf_json(self, value, obf_col):
        if value is None:
            return None
        json_data = json.loads(value)
        rule_data = obf_col['rules']
        obf_data = obf_json(json_data, rule_data)
        if obf_data is None:
            obf_data = {}
        return json.dumps(obf_data)

    def obf_copy_row(self, data, column_list, obf_col_map):
        if data[-1] == '\n':
            data = data[:-1]
        else:
            self.log.warning('Unexpected line from copy without end of line.')

        vals = data.split('\t')
        obf_vals = []
        for field, value in zip(column_list, vals):
            action = obf_col_map.get(field, SKIP)

            if isinstance(action, dict):
                str_val = skytools.unescape_copy(value)
                obf_val = self.obf_json(str_val, action)
                obf_vals.append(skytools.quote_copy(obf_val))
                continue
            elif action == KEEP:
                obf_vals.append(value)
                continue
            elif action == SKIP:
                continue

            str_val = skytools.unescape_copy(value)
            if str_val is None:
                obf_vals.append(value)
            elif action == HASH32:
                obf_val = str(hash32(str_val))
                obf_vals.append(obf_val)
            elif action == HASH64:
                obf_val = str(hash64(str_val))
                obf_vals.append(obf_val)
            elif action == HASH128:
                obf_val = hash128(str_val)
                obf_vals.append(obf_val)
            else:
                raise ValueError('Invalid value for action: %s' % action)
        obf_data = '\t'.join(obf_vals) + '\n'
        return obf_data

    def real_copy(self, src_tablename, src_curs, dst_curs, column_list):
        """Initial copy
        """
        self._validate(src_tablename, column_list)
        obf_col_map = self.obf_map[src_tablename]

        new_list = []
        for col in column_list:
            action = obf_col_map.get(col, SKIP)
            if action != SKIP:
                new_list.append(col)
        column_list = new_list

        def _write_hook(_, data):
            return self.obf_copy_row(data, column_list, obf_col_map)

        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename=self.dest_table,
                                  write_hook=_write_hook)

__londiste_handlers__ = [Obfuscator]

if __name__ == '__main__':
    import doctest
    doctest.testmod()

