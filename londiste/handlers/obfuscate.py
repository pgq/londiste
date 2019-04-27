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
import yaml
from hashlib import blake2s

import skytools
from londiste.handler import TableHandler

__all__ = ['Obfuscator']

class actions:
    KEEP = 'keep'
    HASH = 'hash'
    JSON = 'json'

def sanihash_bytes(data, salt=b'', key=b''):
    """Calculate hash for given data
    """
    hash_bytes = blake2s(data, digest_size=8, key=key, salt=salt).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)

def hash_function(value):
    return sanihash_bytes(str(value).encode('utf8'))

def obf_json(json_data, rule_data, data=None, last_node=None, last_key=None,
             hash_function=hash_function):
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
                elif rule_value == actions.KEEP:
                    pass
                elif rule_value == actions.HASH:
                    json_data = hash_function(json_data)
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
        with open(cf.getfile('obfuscator_map'), 'r') as f:
            cls.obf_map = yaml.safe_load(f)

    def _validate(self, src_tablename, column_list):
        """Warn if column names in keep list are not in column list
        """
        if src_tablename not in self.obf_map:
            raise KeyError('Source tabel not in obf_map: %s' % src_tablename)

        obf_column_map = self.obf_map[src_tablename]
        for column in column_list:
            if column not in obf_column_map:
                self.log.warning(
                    'Column (%s) of table (%s) not in obf_map', column, src_tablename)

    def parse_row_data(self, ev):
        """Extract row data from event, with optional encoding fixes.

        Returns either string (sql event) or dict (urlenc event).
        """
        row = super(Obfuscator, self).parse_row_data(ev)
        self._validate(self.table_name, row.keys())

        obf_col_map = self.obf_map[self.table_name]
        for field, value in row.items():
            if value is None:
                continue

            obf_col = obf_col_map.get(field, {})
            action = obf_col.get('action', actions.HASH)

            if action == actions.KEEP:
                continue
            elif action == actions.HASH:
                hash_val = hash_function(value)
                row[field] = hash_val
            elif action == actions.JSON:
                row[field] = self.obf_json(value, obf_col)
            else:
                raise ValueError('Invalid value for action: %s' % action)
        return row

    def obf_json(self, value, obf_col):
        json_data = json.loads(value)
        rule_data = obf_col['rules']
        obf_data = obf_json(json_data, rule_data)
        return json.dumps(obf_data)

    def real_copy(self, src_tablename, src_curs, dst_curs, column_list):
        """Initial copy
        """
        self._validate(src_tablename, column_list)
        obf_col_map = self.obf_map[src_tablename]
        def _write_hook(_, data):
            if data[-1] == '\n':
                data = data[:-1]
            else:
                self.log.warning('Unexpected line from copy without end of line.')

            vals = data.split('\t')
            obf_vals = []
            for field, value in zip(column_list, vals):
                obf_col = obf_col_map.get(field, {})
                action = obf_col.get('action', actions.HASH)

                if action == actions.KEEP:
                    obf_vals.append(value)
                    continue
                str_val = skytools.unescape_copy(value)
                if str_val is None:
                    obf_vals.append(value)
                    continue
                if action == actions.HASH:
                    obf_val = hash_function(str_val)
                    obf_vals.append('%d' % obf_val)
                elif action == actions.JSON:
                    obf_val = self.obf_json(str_val, obf_col)
                    obf_vals.append(skytools.quote_copy(obf_val))
                else:
                    raise ValueError('Invalid value for action: %s' % action)
            obf_data = '\t'.join(obf_vals) + '\n'
            return obf_data

        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename=self.dest_table,
                                  write_hook=_write_hook)

__londiste_handlers__ = [Obfuscator]
