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

from hashlib import blake2s

import skytools
from londiste.handler import TableHandler

__all__ = ['Obfuscator']

def sanihash_bytes(data, salt=b'', key=b''):
    """Calculate hash for given data
    """
    hash_bytes = blake2s(data, digest_size=8, key=key, salt=salt).digest()
    return int.from_bytes(hash_bytes, byteorder='big', signed=True)

class Obfuscator(TableHandler):
    """Default Londiste handler, inserts events into tables with plain SQL.

    Parameters:
      keep=field_list - "field1", "field2", ...
    """
    handler_name = 'obfuscate'

    def __init__(self, table_name, args, dest_table):
        super(Obfuscator, self).__init__(table_name, args, dest_table)

        keep = args.get('keep')
        if keep:
            keep_fields = keep.split(',')
            self.keep = {field.strip() for field in keep_fields}
        else:
            self.keep = set()

    def _validate_keep(self, column_list):
        """Warn if column names in keep list are not in column list
        """
        cols = set(column_list)
        for keep_field in self.keep:
            if keep_field not in cols:
                self.log.warning(
                    'Field name in keep list (%s) missing from column liost.', keep_field)

    def parse_row_data(self, ev):
        """Extract row data from event, with optional encoding fixes.

        Returns either string (sql event) or dict (urlenc event).
        """
        row = super(Obfuscator, self).parse_row_data(ev)
        self._validate_keep(row.keys())
        for field, value in row.items():
            if field in self.keep:
                continue
            if value is None:
                continue
            hash_val = sanihash_bytes(str(value).encode('utf8'))
            row[field] = hash_val
        return row

    def real_copy(self, src_tablename, src_curs, dst_curs, column_list):
        """Initial copy
        """
        self._validate_keep(column_list)

        keep_flags = [(field_name in self.keep) for field_name in column_list]

        def _write_hook(_, data):
            if data[-1] == '\n':
                data = data[:-1]
            else:
                self.log.warning('Unexpected line from copy without end of line.')

            vals = data.split('\t')
            obvals = []
            for i, value in enumerate(vals):
                if keep_flags[i]:
                    obvals.append(value)
                    continue
                str_val = skytools.unescape_copy(value)
                if str_val is None:
                    obvals.append(value)
                else:
                    hash_val = sanihash_bytes(str_val.encode('utf8'))
                    obvals.append('%d' % hash_val)
            obdata = '\t'.join(obvals) + '\n'
            return obdata

        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename=self.dest_table,
                                  write_hook=_write_hook)

__londiste_handlers__ = [Obfuscator]
