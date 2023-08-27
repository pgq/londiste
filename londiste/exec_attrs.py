"""Custom parser for EXECUTE attributes.

The values are parsed from SQL file given to EXECUTE.

Format rules:
    * Only lines starting with meta-comment prefix will be parsed: --*--
    * Empty or regular SQL comment lines are ignored.
    * Parsing stops on first SQL statement.
    * Meta-line format: "--*-- Key: value1, value2"
    * If line ends with ',' then next line is taken as continuation.

Supported keys:
    * Local-Table:
    * Local-Sequence:
    * Local-Destination:

    * Need-Table
    * Need-Sequence
    * Need-Function
    * Need-Schema
    * Need-View

Sample file::
  --*-- Local-Sequence: myseq
  --*--
  --*-- Local-Table: table1,
  --*--     table2, table3
  --*--

Tests:

>>> a = ExecAttrs()
>>> a.add_value("Local-Table", "mytable")
>>> a.add_value("Local-Sequence", "seq1")
>>> a.add_value("Local-Sequence", "seq2")
>>> a.to_urlenc() in (
...     'local-table=mytable&local-sequence=seq1%2cseq2',
...     'local-sequence=seq1%2cseq2&local-table=mytable')
True
>>> a.add_value("Local-Destination", "mytable-longname-more1")
>>> a.add_value("Local-Destination", "mytable-longname-more2")
>>> a.add_value("Local-Destination", "mytable-longname-more3")
>>> a.add_value("Local-Destination", "mytable-longname-more4")
>>> a.add_value("Local-Destination", "mytable-longname-more5")
>>> a.add_value("Local-Destination", "mytable-longname-more6")
>>> a.add_value("Local-Destination", "mytable-longname-more7")
>>> print(a.to_sql())
--*-- Local-Table: mytable
--*-- Local-Sequence: seq1, seq2
--*-- Local-Destination: mytable-longname-more1, mytable-longname-more2,
--*--     mytable-longname-more3, mytable-longname-more4, mytable-longname-more5,
--*--     mytable-longname-more6, mytable-longname-more7
>>> a = ExecAttrs(sql = '''
...
...  --
...
... --*-- Local-Table: foo ,
... --
... --*-- bar ,
... --*--
... --*-- zoo
... --*--
... --*-- Local-Sequence: goo
... --*--
... --
...
... create fooza;
... ''')
>>> print(a.to_sql())
--*-- Local-Table: foo, bar, zoo
--*-- Local-Sequence: goo
>>> seqs = {'public.goo': 'public.goo'}
>>> tables = {}
>>> tables['public.foo'] = 'public.foo'
>>> tables['public.bar'] = 'other.Bar'
>>> tables['public.zoo'] = 'Other.Foo'
>>> a.need_execute(None, tables, seqs)
True
>>> a.need_execute(None, [], [])
False
>>> sql = '''alter table @foo@;
... alter table @bar@;
... alter table @zoo@;'''
>>> print(a.process_sql(sql, tables, seqs))
alter table public.foo;
alter table other."Bar";
alter table "Other"."Foo";
"""

from typing import Dict, List, Optional

import skytools
from skytools.basetypes import Cursor

META_PREFIX = "--*--"


class Matcher:
    nice_name: str = ''
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        return False
    def get_key(self) -> str:
        return self.nice_name.lower()
    def local_rename(self) -> bool:
        return False


class LocalTable(Matcher):
    nice_name: str = "Local-Table"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        return objname in tables
    def local_rename(self) -> bool:
        return True


class LocalSequence(Matcher):
    nice_name = "Local-Sequence"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        return objname in seqs
    def local_rename(self) -> bool:
        return True


class LocalDestination(Matcher):
    nice_name = "Local-Destination"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        if objname not in tables:
            return False
        dest_name = tables[objname]
        return skytools.exists_table(curs, dest_name)
    def local_rename(self) -> bool:
        return True


class NeedTable(Matcher):
    nice_name = "Need-Table"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        return skytools.exists_table(curs, objname)


class NeedSequence(Matcher):
    nice_name = "Need-Sequence"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        return skytools.exists_sequence(curs, objname)


class NeedSchema(Matcher):
    nice_name = "Need-Schema"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        return skytools.exists_schema(curs, objname)


class NeedFunction(Matcher):
    nice_name = "Need-Function"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        nargs = 0
        pos1 = objname.find('(')
        if pos1 > 0:
            pos2 = objname.find(')')
            if pos2 > 0:
                s = objname[pos1 + 1: pos2]
                objname = objname[:pos1]
                nargs = int(s)
        return skytools.exists_function(curs, objname, nargs)


class NeedView(Matcher):
    nice_name = "Need-View"
    def match(self, objname: str, curs: Cursor,  tables: Dict[str, str],  seqs: Dict[str, str]) -> bool:
        return skytools.exists_view(curs, objname)


META_SPLITLINE = 70

# list of matches, in order they need to be probed
META_MATCHERS = [
    LocalTable(), LocalSequence(), LocalDestination(),
    NeedTable(), NeedSequence(), NeedFunction(),
    NeedSchema(), NeedView()
]

# key to nice key
META_KEYS = {m.nice_name.lower(): m for m in META_MATCHERS}


class ExecAttrsException(skytools.UsageError):
    """Some parsing problem."""


class ExecAttrs:
    """Container and parser for EXECUTE attributes."""
    attrs: Dict[str, List[str]]

    def __init__(self, sql: Optional[str] = None, urlenc: Optional[str] = None) -> None:
        """Create container and parse either sql or urlenc string."""

        self.attrs = {}
        if sql and urlenc:
            raise Exception("Both sql and urlenc set.")
        if urlenc:
            self.parse_urlenc(urlenc)
        elif sql:
            self.parse_sql(sql)

    def add_value(self, k: str, v: str) -> None:
        """Add single value to key."""

        xk = k.lower().strip()
        if xk not in META_KEYS:
            raise ExecAttrsException("Invalid key: %s" % k)
        if xk not in self.attrs:
            self.attrs[xk] = []

        xv = v.strip()
        self.attrs[xk].append(xv)

    def to_urlenc(self) -> str:
        """Convert container to urlencoded string."""
        sdict = {}
        for k, v in self.attrs.items():
            sdict[k] = ','.join(v)
        return skytools.db_urlencode(sdict)

    def parse_urlenc(self, ustr: str) -> None:
        """Parse urlencoded string adding values to current container."""
        sdict = skytools.db_urldecode(ustr)
        for k, v in sdict.items():
            if v:
                for v1 in v.split(','):
                    self.add_value(k, v1)

    def to_sql(self) -> str:
        """Convert container to SQL meta-comments."""
        lines = []
        for m in META_MATCHERS:
            k = m.get_key()
            if k not in self.attrs:
                continue
            vlist = self.attrs[k]
            ln = "%s %s: " % (META_PREFIX, m.nice_name)
            start = 0
            for nr, v in enumerate(vlist):
                if nr > start:
                    ln = ln + ", " + v
                else:
                    ln = ln + v

                if len(ln) >= META_SPLITLINE and nr < len(vlist) - 1:
                    ln += ','
                    lines.append(ln)
                    ln = META_PREFIX + "     "
                    start = nr + 1
            lines.append(ln)
        return '\n'.join(lines)

    def parse_sql(self, sql: str) -> None:
        """Parse SQL meta-comments."""

        cur_key: Optional[str] = None
        cur_continued = False
        for ln in sql.splitlines():

            # skip empty lines
            ln = ln.strip()
            if not ln:
                continue

            # stop at non-comment
            if ln[:2] != '--':
                break

            # parse only meta-comments
            if ln[:len(META_PREFIX)] != META_PREFIX:
                continue

            # cut prefix, skip empty comments
            ln = ln[len(META_PREFIX):].strip()
            if not ln:
                continue

            # continuation of previous key
            if cur_continued:
                # collect values
                for v in ln.split(','):
                    v = v.strip()
                    if v and cur_key:
                        self.add_value(cur_key, v)

                # does this key continue?
                if ln[-1] != ',':
                    cur_key = None
                    cur_continued = False

                # go to next line
                continue

            # parse key
            pos = ln.find(':')
            if pos < 0:
                continue
            k = ln[:pos].strip()

            # collect values
            for v in ln[pos + 1:].split(','):
                v = v.strip()
                if not v:
                    continue
                self.add_value(k, v)

            # check if current key values will continue
            if ln[-1] == ',':
                cur_key = k
                cur_continued = True
            else:
                cur_key = None
                cur_continued = False

    def need_execute(self, curs: Cursor, local_tables: Dict[str, str], local_seqs: Dict[str, str]) -> bool:
        # if no attrs, always execute
        if not self.attrs:
            return True

        matched = 0
        missed = 0
        good_list = []
        miss_list = []
        for m in META_MATCHERS:
            k = m.get_key()
            if k not in self.attrs:
                continue
            for v in self.attrs[k]:
                fqname = skytools.fq_name(v)
                if m.match(fqname, curs, local_tables, local_seqs):
                    matched += 1
                    good_list.append(v)
                else:
                    missed += 1
                    miss_list.append(v)
                    # should be drop out early?
        if matched > 0 and missed == 0:
            return True
        elif missed > 0 and matched == 0:
            return False
        elif missed == 0 and matched == 0:
            # should not happen, but lets restore old behaviour?
            return True
        else:
            raise Exception("SQL only partially matches local setup: matches=%r misses=%r" % (good_list, miss_list))

    def get_attr(self, k: str) -> List[str]:
        k = k.lower().strip()
        if k not in META_KEYS:
            raise Exception("Bug: invalid key requested: " + k)
        if k not in self.attrs:
            return []
        return self.attrs[k]

    def process_sql(self, sql: str, local_tables: Dict[str, str], local_seqs: Dict[str, str]) -> str:
        """Replace replacement tags in sql with actual local names."""
        for k, vlist in self.attrs.items():
            m = META_KEYS[k]
            if not m.local_rename():
                continue
            for v in vlist:
                repname = '@%s@' % v
                fqname = skytools.fq_name(v)
                if fqname in local_tables:
                    localname = local_tables[fqname]
                elif fqname in local_seqs:
                    localname = local_seqs[fqname]
                else:
                    # should not happen
                    raise Exception("bug: lost table: " + v)
                qdest = skytools.quote_fqident(localname)
                sql = sql.replace(repname, qdest)
        return sql

