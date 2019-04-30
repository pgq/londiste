"""
Bulk loading into OLAP database.

To use set in londiste.ini:

    handler_modules = londiste.handlers.bulk

then add table with:
  londiste3 add-table xx --handler="obfuscate"

or:
  londiste3 add-table xx --handler="obfuscate" --handler-arg="keep=field1, field2, ..."
    list of fields whose values are not to be obfuscated

Default is 0.

"""
import json
import uuid
from hashlib import blake2s

import yaml
import skytools
from londiste.handler import TableHandler

__all__ = ['Obfuscator']

_KEY = b''

class actions:
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
    """Calculate hash for given data
    """
    if data is None:
        return None
    hash_bytes = blake2s(as_bytes(data), digest_size=4, key=_KEY).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)

def hash64(data):
    """Calculate hash for given data
    """
    if data is None:
        return None
    hash_bytes = blake2s(as_bytes(data), digest_size=8, key=_KEY).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)

def hash128(data):
    """Calculate hash for given data
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

def obf_json(json_data, rule_data, data=None, last_node=None, last_key=None):
    if data is None:
        data = {}
    for rule_key, rule_value in rule_data.items():
        if isinstance(rule_value, dict):
            node = data.setdefault(rule_key, {})
            if not isinstance(json_data, dict):
                json_data = {}
            obf_json(json_data.get(rule_key, {}), rule_value, node, data, rule_key)
        else:
            if rule_key == "action":
                if isinstance(json_data, dict) and not json_data:
                    json_data = None
                elif rule_value == actions.KEEP or json_data is None:
                    pass
                elif rule_value == actions.HASH32:
                    json_data = hash32(json_data)
                elif rule_value == actions.HASH64:
                    json_data = hash64(json_data)
                elif rule_value == actions.HASH128:
                    json_data = hash128(json_data)
                else:
                    raise ValueError('Invalid rule value: %s' % rule_value)
                last_node[last_key] = json_data
            else:
                raise ValueError('Invalid rule key: %s' % rule_key)
    return data

class Obfuscator(TableHandler):
    """Default Londiste handler, inserts events into tables with plain SQL.
    """
    handler_name = 'obfuscate'
    obf_map = {}

    @classmethod
    def load_conf(cls, cf):
        global _KEY
        with open(cf.getfile('obfuscator_map'), 'r') as f:
            cls.obf_map = yaml.safe_load(f)
        _KEY = as_bytes(cf.get('obfuscator_key', ''))

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

        obf_col_map = self.obf_map[self.table_name]
        dst = {}
        for field, value in row.items():
            obf_col = obf_col_map.get(field, {})
            action = obf_col.get('action', actions.SKIP)

            if action == actions.KEEP:
                dst[field] = value
            elif action == actions.SKIP:
                continue
            elif action == actions.HASH32:
                dst[field] = hash32(value)
            elif action == actions.HASH64:
                dst[field] = hash64(value)
            elif action == actions.HASH128:
                dst[field] = hash128(value)
            elif action == actions.JSON:
                dst[field] = self.obf_json(value, obf_col)
            else:
                raise ValueError('Invalid value for action: %s' % action)
        return dst

    def obf_json(self, value, obf_col):
        json_data = json.loads(value)
        rule_data = obf_col['rules']
        obf_data = obf_json(json_data, rule_data)
        return json.dumps(obf_data)

    def obf_copy_row(self, data, column_list, obf_col_map):
        if data[-1] == '\n':
            data = data[:-1]
        else:
            self.log.warning('Unexpected line from copy without end of line.')

        vals = data.split('\t')
        obf_vals = []
        for field, value in zip(column_list, vals):
            obf_col = obf_col_map.get(field, {})
            action = obf_col.get('action', actions.SKIP)

            if action == actions.KEEP:
                obf_vals.append(value)
                continue
            if action == actions.SKIP:
                continue
            str_val = skytools.unescape_copy(value)
            if str_val is None:
                obf_vals.append(value)
            elif action == actions.HASH32:
                obf_val = str(hash32(str_val))
                obf_vals.append(obf_val)
            elif action == actions.HASH64:
                obf_val = str(hash64(str_val))
                obf_vals.append(obf_val)
            elif action == actions.HASH128:
                obf_val = hash128(str_val)
                obf_vals.append(obf_val)
            elif action == actions.JSON:
                obf_val = self.obf_json(str_val, obf_col)
                obf_vals.append(skytools.quote_copy(obf_val))
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
            action = obf_col_map.get(col, {}).get('action', actions.SKIP)
            if action != actions.SKIP:
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
