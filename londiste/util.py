"""Misc utilities for COPY code.
"""

from typing import Optional, Tuple, Union, Sequence, List, Any, Callable, TYPE_CHECKING

import array
import mmap
import io

import skytools
from skytools.config import read_versioned_config
from skytools.basetypes import Cursor

import londiste.handler

if TYPE_CHECKING:
    import multiprocessing.connection

__all__ = ['handler_allows_copy', 'find_copy_source']

WriteHook = Optional[Callable[[Any, str], str]]
FlushHook = Optional[Callable[[Any, str], str]]


def handler_allows_copy(table_attrs: Optional[str]) -> bool:
    """Decide if table is copyable based on attrs."""
    if not table_attrs:
        return True
    attrs = skytools.db_urldecode(table_attrs)
    hstr = attrs.get('handler', '')
    p = londiste.handler.build_handler('unused.string', hstr, None)
    return p.needs_table()


def find_copy_source(
    script: "skytools.DBScript", queue_name: str, copy_table_name: Union[str, Sequence[str]],
    node_name: str, node_location: str,
) -> Tuple[str, str, Optional[str]]:
    """Find source node for table.

    @param script: DbScript
    @param queue_name: name of the cascaded queue
    @param copy_table_name: name of the table (or list of names)
    @param node_name: target node name
    @param node_location: target node location
    @returns (node_name, node_location, downstream_worker_name) of source node
    """

    # None means no steps upwards were taken, so local consumer is worker
    worker_name = None

    if isinstance(copy_table_name, str):
        need = set([copy_table_name])
    else:
        need = set(copy_table_name)

    while True:
        src_db = script.get_database('_source_db', connstr=node_location, autocommit=1, profile='remote')
        src_curs = src_db.cursor()

        q = "select * from pgq_node.get_node_info(%s)"
        src_curs.execute(q, [queue_name])
        info = src_curs.fetchone()
        if info['ret_code'] >= 400:
            raise skytools.UsageError("Node does not exist")

        script.log.info("Checking if %s can be used for copy", info['node_name'])

        q = "select table_name, local, table_attrs from londiste.get_table_list(%s)"
        src_curs.execute(q, [queue_name])
        got = set()
        for row in src_curs.fetchall():
            tbl = row['table_name']
            if tbl not in need:
                continue
            if not row['local']:
                script.log.debug("Problem: %s is not local", tbl)
                continue
            if not handler_allows_copy(row['table_attrs']):
                script.log.debug("Problem: %s handler does not store data [%s]", tbl, row['table_attrs'])
                continue
            script.log.debug("Good: %s is usable", tbl)
            got.add(tbl)

        script.close_database('_source_db')

        if got == need:
            script.log.info("Node %s seems good source, using it", info['node_name'])
            return node_name, node_location, worker_name
        else:
            script.log.info("Node %s does not have all tables", info['node_name'])

        if info['node_type'] == 'root':
            raise skytools.UsageError("Found root and no source found")

        # walk upwards
        node_name = info['provider_node']
        node_location = info['provider_location']
        worker_name = info['worker_name']


COPY_FROM_BLK = 1024 * 1024
COPY_MERGE_BUF = 256 * 1024


class MPipeReader(io.RawIOBase):
    """Read from pipe
    """
    def __init__(self, p_recv):
        super().__init__()

        self.p_recv = p_recv
        self.buf = b""
        self.blocks = []

    def readable(self):
        return True

    def read(self, size: int = -1) -> bytes:
        # size=-1 means 'all'
        if size < 0:
            size = 1 << 30

        # fetch current block of data
        if self.buf:
            data = self.buf
            self.buf = b""
        else:
            if not self.blocks:
                try:
                    self.blocks = self.p_recv.recv()
                except EOFError:
                    return b""
                self.blocks.reverse()
            data = self.blocks.pop()

        # return part of it
        if len(data) > size:
            data = memoryview(data)
            self.buf = data[size:]
            return data[:size].tobytes()
        return data if isinstance(data, bytes) else data.tobytes()


# args: pipe, sql, cstr, fn, sect, encoding
def copy_worker_proc(
    p_recv: "multiprocessing.connection.Connection",
    sql_from: str,
    dst_db_connstr: str,
    config_file: Optional[str],
    config_section: Optional[str],
    src_encoding: Optional[str],
) -> bool:
    """Launched in separate process.
    """
    if config_file and config_section:
        cf = read_versioned_config([config_file], config_section)
        londiste.handler.load_handler_modules(cf)

    preader = MPipeReader(p_recv)
    with skytools.connect_database(dst_db_connstr) as dst_db:
        if src_encoding and dst_db.encoding != src_encoding:
            dst_db.set_client_encoding(src_encoding)
        with dst_db.cursor() as dst_curs:
            dst_curs.execute("select londiste.set_session_replication_role('replica', true)")
            dst_curs.copy_expert(sql_from, preader, COPY_FROM_BLK)
        dst_db.commit()
    return True


class CopyPipeMultiProc(io.RawIOBase):
    """Pass COPY data over thread.
    """

    block_buf: List[bytes]
    write_hook: WriteHook

    def __init__(
        self,
        sql_from: str,
        dst_db_connstr: str,
        parallel: int = 1,
        config_file: Optional[str] = None,
        config_section: Optional[str] = None,
        write_hook: WriteHook = None,
        src_encoding: Optional[str] = None,
    ) -> None:
        """Setup queue and worker thread.
        """
        import multiprocessing
        import concurrent.futures

        super().__init__()

        self.sql_from = sql_from
        self.total_rows = 0
        self.total_bytes = 0
        self.parallel = parallel
        self.work_threads = []
        self.send_pipes = []
        self.block_buf = []
        self.block_buf_len = 0
        self.send_pos = 0
        self.write_hook = write_hook

        # avoid fork
        mp_ctx = multiprocessing.get_context("spawn")
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=parallel, mp_context=mp_ctx)
        for _ in range(parallel):
            p_recv, p_send = mp_ctx.Pipe(False)
            # args: pipe, sql, cstr, fn, sect, encoding
            f = self.executor.submit(
                copy_worker_proc,
                p_recv, self.sql_from, dst_db_connstr,
                config_file, config_section,
                src_encoding,
            )
            self.work_threads.append(f)
            self.send_pipes.append(p_send)

    def writable(self):
        return True

    def write(self, data: Union[bytes, bytearray, memoryview, array.array, mmap.mmap]) -> int:
        """New row from psycopg
        """
        if not isinstance(data, bytes):
            data = memoryview(data).tobytes()

        write_hook = self.write_hook
        if write_hook:
            data = write_hook(self, data.decode()).encode()     # pylint: disable=not-callable

        self.block_buf.append(data)
        self.block_buf_len += len(data)

        if self.block_buf_len > COPY_MERGE_BUF:
            self.send_blocks()

        self.total_bytes += len(data)
        self.total_rows += 1
        return len(data)

    def send_blocks(self):
        """Send collected rows.
        """
        pos = self.send_pos % self.parallel
        self.send_pipes[pos].send(self.block_buf)
        self.block_buf = []
        self.block_buf_len = 0
        self.send_pos += 1

    def flush(self) -> None:
        """Finish sending.
        """
        if self.block_buf:
            self.send_blocks()
        for p_send in self.send_pipes:
            p_send.close()
        for f in self.work_threads:
            f.result()
        self.executor.shutdown()


def full_copy_parallel(
    tablename: str,
    src_curs: Cursor,
    dst_db_connstr: str,
    column_list: Sequence[str] = (),
    condition: Optional[str] = None,
    dst_tablename: Optional[str] = None,
    dst_column_list: Optional[Sequence[str]] = None,
    config_file: Optional[str] = None,
    config_section: Optional[str] = None,
    write_hook=None,
    flush_hook=None,
    parallel=1,
):
    """COPY table from one db to another."""

    # default dst table and dst columns to source ones
    dst_tablename = dst_tablename or tablename
    dst_column_list = dst_column_list or column_list[:]
    if len(dst_column_list) != len(column_list):
        raise Exception('src and dst column lists must match in length')

    def build_qfields(cols):
        if cols:
            return ",".join([skytools.quote_ident(f) for f in cols])
        else:
            return "*"

    def build_statement(table, cols):
        qtable = skytools.quote_fqident(table)
        if cols:
            qfields = build_qfields(cols)
            return "%s (%s)" % (qtable, qfields)
        else:
            return qtable

    dst = build_statement(dst_tablename, dst_column_list)
    if condition:
        src = "(SELECT %s FROM %s WHERE %s)" % (
            build_qfields(column_list),
            skytools.quote_fqident(tablename),
            condition
        )
    else:
        src = build_statement(tablename, column_list)

    copy_opts = ""
    sql_to = "COPY %s TO stdout%s" % (src, copy_opts)
    sql_from = "COPY %s FROM stdin%s" % (dst, copy_opts)
    bufm = CopyPipeMultiProc(
        config_file=config_file, config_section=config_section,
        sql_from=sql_from, dst_db_connstr=dst_db_connstr, parallel=parallel,
        write_hook=write_hook,
    )
    try:
        src_curs.copy_expert(sql_to, bufm)
    finally:
        bufm.flush()
    if flush_hook:
        flush_hook(bufm)
    return (bufm.total_bytes, bufm.total_rows)

