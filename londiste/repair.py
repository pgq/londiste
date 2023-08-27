"""Repair data on subscriber.

Walks tables by primary key and searches for missing inserts/updates/deletes.
"""

import os
import optparse
import subprocess
import sys

from typing import Optional, Dict, List, Sequence, Any, IO

import skytools
from skytools.basetypes import Cursor, Connection

from londiste.syncer import Syncer, ATable

__all__ = ['Repairer']


def unescape(s: str) -> Optional[str]:
    """Remove copy escapes."""
    return skytools.unescape_copy(s)


class Repairer(Syncer):
    """Walks tables in primary key order and checks if data matches."""

    cnt_insert: int = 0
    cnt_update: int = 0
    cnt_delete: int = 0
    total_src: int = 0
    total_dst: int = 0
    pkey_list: List[str] = []
    common_fields: List[str] = []
    apply_curs: Optional[Cursor] = None
    fq_common_fields: Sequence[str] = ()

    def init_optparse(self, p: Optional[optparse.OptionParser] = None) -> optparse.OptionParser:
        """Initialize cmdline switches."""
        p = super().init_optparse(p)
        p.add_option("--apply", action="store_true", help="apply fixes")
        p.add_option("--sort-bufsize", help="buffer for coreutils sort")
        p.add_option("--repair-where", help="where condition for selecting data")
        return p

    def process_sync(self, t1: ATable, t2: ATable, src_db: Connection, dst_db: Connection) -> int:
        """Actual comparison."""

        apply_db: Optional[Connection] = None

        if self.options.apply:
            apply_db = self.get_database('db', cache='applydb', autocommit=1)
            self.apply_curs = apply_db.cursor()
            self.apply_curs.execute("select londiste.set_session_replication_role('replica', false)")

        src_tbl = t1.dest_table
        dst_tbl = t2.dest_table

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info('Checking %s', dst_tbl)

        self.common_fields = []
        self.fq_common_fields = []
        self.pkey_list = []
        self.load_common_columns(src_tbl, dst_tbl, src_curs, dst_curs)

        dump_src = dst_tbl + ".src"
        dump_dst = dst_tbl + ".dst"
        dump_src_sorted = dump_src + ".sorted"
        dump_dst_sorted = dump_dst + ".sorted"

        dst_where = t2.plugin.get_copy_condition(src_curs, dst_curs)
        src_where = dst_where

        if self.options.repair_where and src_where:
            dst_where = src_where = src_where + ' and ' + self.options.repair_where
        elif self.options.repair_where:
            dst_where = src_where = self.options.repair_where

        self.log.info("Dumping src table: %s %s", src_tbl, src_where)
        self.dump_table(src_tbl, src_curs, dump_src, src_where)
        src_db.commit()
        self.log.info("Dumping dst table: %s %s", dst_tbl, dst_where)
        self.dump_table(dst_tbl, dst_curs, dump_dst, dst_where)
        dst_db.commit()

        self.log.info("Sorting src table: %s", dump_src)
        self.do_sort(dump_src, dump_src_sorted)
        self.log.info("Sorting dst table: %s", dump_dst)
        self.do_sort(dump_dst, dump_dst_sorted)

        self.dump_compare(dst_tbl, dump_src_sorted, dump_dst_sorted)

        os.unlink(dump_src)
        os.unlink(dump_dst)
        os.unlink(dump_src_sorted)
        os.unlink(dump_dst_sorted)
        return 0

    def do_sort(self, src: str, dst: str) -> None:
        """ Sort contents of src file, write them to dst file. """

        with subprocess.Popen(["sort", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            s_ver = p.communicate()[0].decode('utf8', 'replace')

        xenv = os.environ.copy()
        xenv['LANG'] = 'C'
        xenv['LC_ALL'] = 'C'

        cmdline = ['sort', '-T', '.']
        if s_ver.find("coreutils") > 0:
            cmdline.append('-S')
            if self.options.sort_bufsize:
                cmdline.append(self.options.sort_bufsize)
            else:
                cmdline.append('30%')
        cmdline.append('-o')
        cmdline.append(dst)
        cmdline.append(src)
        with subprocess.Popen(cmdline, env=xenv) as p:
            if p.wait() != 0:
                raise Exception('sort failed')

    def load_common_columns(self, src_tbl: str, dst_tbl: str, src_curs: Cursor, dst_curs: Cursor) -> None:
        """Get common fields, put pkeys in start."""

        self.pkey_list = skytools.get_table_pkeys(src_curs, src_tbl)
        dst_pkey = skytools.get_table_pkeys(dst_curs, dst_tbl)
        if dst_pkey != self.pkey_list:
            self.log.error('pkeys do not match')
            sys.exit(1)

        src_cols = skytools.get_table_columns(src_curs, src_tbl)
        dst_cols = skytools.get_table_columns(dst_curs, dst_tbl)
        field_list = []
        for f in self.pkey_list:
            field_list.append(f)
        for f in src_cols:
            if f in self.pkey_list:
                continue
            if f in dst_cols:
                field_list.append(f)

        self.common_fields = field_list

        fqlist = [skytools.quote_ident(col) for col in field_list]
        self.fq_common_fields = fqlist

        cols = ",".join(fqlist)
        self.log.debug("using columns: %s", cols)

    def dump_table(self, tbl: str, curs: Cursor, fn: str, whr: str) -> None:
        """Dump table to disk."""
        cols = ','.join(self.fq_common_fields)
        if len(whr) == 0:
            whr = 'true'
        q = "copy (SELECT %s FROM %s WHERE %s) to stdout" % (cols, skytools.quote_fqident(tbl), whr)
        self.log.debug("Query: %s", q)
        with open(fn, "w", 64 * 1024, encoding="utf8") as f:
            curs.copy_expert(q, f)
            size = f.tell()
        self.log.info('%s: Got %d bytes', tbl, size)

    def get_row(self, ln: str) -> Dict[str, str]:
        """Parse a row into dict."""
        t = ln[:-1].split('\t')
        row = {}
        for i, fname in enumerate(self.common_fields):
            row[fname] = t[i]
        return row

    def dump_compare(self, tbl: str, src_fn: str, dst_fn: str) -> None:
        """ Compare two table dumps, create sql file to fix target table
            or apply changes to target table directly.
        """
        with open(src_fn, "r", 64 * 1024, encoding="utf8") as f1:
            with open(dst_fn, "r", 64 * 1024, encoding="utf8") as f2:
                self.dump_compare_streams(tbl, f1, f2)

    def dump_compare_streams(self, tbl: str, f1: IO[str], f2: IO[str]) -> None:
        self.log.info("Comparing dumps: %s", tbl)
        self.cnt_insert = 0
        self.cnt_update = 0
        self.cnt_delete = 0
        self.total_src = 0
        self.total_dst = 0
        src_ln = f1.readline()
        dst_ln = f2.readline()
        if src_ln:
            self.total_src += 1
        if dst_ln:
            self.total_dst += 1

        fix = "fix.%s.sql" % tbl
        if os.path.isfile(fix):
            os.unlink(fix)

        while src_ln or dst_ln:
            keep_src = keep_dst = 0
            if src_ln != dst_ln:
                src_row = self.get_row(src_ln)
                dst_row = self.get_row(dst_ln)

                diff = self.cmp_keys(src_row, dst_row)
                if diff > 0:
                    # src > dst
                    self.got_missed_delete(tbl, dst_row)
                    keep_src = 1
                elif diff < 0:
                    # src < dst
                    self.got_missed_insert(tbl, src_row)
                    keep_dst = 1
                else:
                    if self.cmp_data(src_row, dst_row) != 0:
                        self.got_missed_update(tbl, src_row, dst_row)

            if not keep_src:
                src_ln = f1.readline()
                if src_ln:
                    self.total_src += 1
            if not keep_dst:
                dst_ln = f2.readline()
                if dst_ln:
                    self.total_dst += 1

        self.log.info("finished %s: src: %d rows, dst: %d rows,"
                      " missed: %d inserts, %d updates, %d deletes",
                      tbl, self.total_src, self.total_dst,
                      self.cnt_insert, self.cnt_update, self.cnt_delete)

    def got_missed_insert(self, tbl: str, src_row: Dict[str, str]) -> None:
        """Create sql for missed insert."""
        self.cnt_insert += 1
        fld_list = self.common_fields
        fq_list = []
        val_list = []
        for f in fld_list:
            fq_list.append(skytools.quote_ident(f))
            v = unescape(src_row[f])
            val_list.append(skytools.quote_literal(v))
        q = "insert into %s (%s) values (%s);" % (
            tbl, ", ".join(fq_list), ", ".join(val_list))
        self.show_fix(tbl, q, 'insert')

    def got_missed_update(self, tbl: str, src_row: Dict[str, str], dst_row: Dict[str, str]) -> None:
        """Create sql for missed update."""
        self.cnt_update += 1
        fld_list = self.common_fields
        set_list: List[str] = []
        whe_list: List[str] = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(src_row[f]))
        for f in fld_list:
            v1 = src_row[f]
            v2 = dst_row[f]
            if self.cmp_value(v1, v2) == 0:
                continue

            self.addeq(set_list, skytools.quote_ident(f), unescape(v1))
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(v2))

        q = "update only %s set %s where %s;" % (
            tbl, ", ".join(set_list), " and ".join(whe_list))
        self.show_fix(tbl, q, 'update')

    def got_missed_delete(self, tbl: str, dst_row: Dict[str, str]) -> None:
        """Create sql for missed delete."""
        self.cnt_delete += 1
        whe_list: List[str] = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(dst_row[f]))
        q = "delete from only %s where %s;" % (skytools.quote_fqident(tbl), " and ".join(whe_list))
        self.show_fix(tbl, q, 'delete')

    def show_fix(self, tbl: str, q: str, desc: str) -> None:
        """Print/write/apply repair sql."""
        self.log.debug("missed %s: %s", desc, q)
        if self.apply_curs:
            self.apply_curs.execute(q)
        else:
            fn = "fix.%s.sql" % tbl
            with open(fn, "a", encoding="utf8") as f:
                f.write("%s\n" % q)

    def addeq(self, dst_list: List[str], f: str, v: Any) -> None:
        """Add quoted SET."""
        vq = skytools.quote_literal(v)
        s = "%s = %s" % (f, vq)
        dst_list.append(s)

    def addcmp(self, dst_list: List[str], f: str, v: Any) -> None:
        """Add quoted comparison."""
        if v is None:
            s = "%s is null" % f
        else:
            vq = skytools.quote_literal(v)
            s = "%s = %s" % (f, vq)
        dst_list.append(s)

    def cmp_data(self, src_row: Dict[str, str], dst_row: Dict[str, str]) -> int:
        """Compare data field-by-field."""
        for k in self.common_fields:
            v1 = src_row[k]
            v2 = dst_row[k]
            if self.cmp_value(v1, v2) != 0:
                return -1
        return 0

    def cmp_value(self, v1: str, v2: str) -> int:
        """Compare single field, tolerates tz vs notz dates."""
        if v1 == v2:
            return 0

        # try to work around tz vs. notz
        z1 = len(v1)
        z2 = len(v2)
        if z1 == z2 + 3 and z2 >= 19 and v1[z2] == '+':
            v1 = v1[:-3]
            if v1 == v2:
                return 0
        elif z1 + 3 == z2 and z1 >= 19 and v2[z1] == '+':
            v2 = v2[:-3]
            if v1 == v2:
                return 0

        return -1

    def cmp_keys(self, src_row: Dict[str, str], dst_row: Dict[str, str]) -> int:
        """Compare primary keys of the rows.

        Returns 1 if src > dst, -1 if src < dst and 0 if src == dst"""

        # None means table is done.  tag it larger than any existing row.
        if src_row is None:
            if dst_row is None:
                return 0
            return 1
        elif dst_row is None:
            return -1

        for k in self.pkey_list:
            v1 = src_row[k]
            v2 = dst_row[k]
            if v1 < v2:
                return -1
            elif v1 > v2:
                return 1
        return 0

