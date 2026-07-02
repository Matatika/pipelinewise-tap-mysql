import contextlib
import io
import json
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.ipc as ipc

import tap_mysql
from tap_mysql import adbc
from tap_mysql.connection import connect_with_backoff

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils


def _arrow_support_available():
    try:
        adbc.require_arrow_support()
        return True
    except adbc.ArrowSupportError:
        return False


def _read_batch_messages(stdout_text):
    return [json.loads(line) for line in stdout_text.splitlines() if line.strip()]


def _read_arrow_table(batch_message):
    path = batch_message['manifest'][0].removeprefix('file://')
    return ipc.open_file(path).read_all()


@unittest.skipUnless(_arrow_support_available(), 'MySQL ADBC driver not installed')
class TestArrowBatchFullTable(unittest.TestCase):
    """Exercises FULL_TABLE sync with batch_format='arrow' against a real MySQL server.

    Runs through the actual ADBC connection path (tap_mysql/adbc.py), which the mocked
    unit tests in tests/unit/ can't verify: real driver connectivity, and that the
    NULLIF-based invalid-date-to-NULL SQL (common.generate_select_sql) survives round-tripping
    through the Arrow/ADBC driver instead of crashing it.
    """

    def setUp(self):
        self.conn = test_utils.get_test_connection()
        self.singer_messages = []
        self._write_message_patcher = patch('tap_mysql.stream_utils.write_message',
                                            side_effect=self.singer_messages.append)
        self._write_message_patcher.start()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute(
                    'CREATE TABLE arrow_full (id int primary key, name varchar(100), created_at datetime)')
                cursor.execute("INSERT INTO arrow_full (id, name, created_at) VALUES (1, 'a', '2020-01-01 00:00:00')")
                cursor.execute("INSERT INTO arrow_full (id, name, created_at) VALUES (2, 'b', '2020-01-02 00:00:00')")
                cursor.execute("INSERT INTO arrow_full (id, name, created_at) VALUES (3, 'c', '2020-01-03 00:00:00')")
                cursor.execute("SET SESSION sql_mode=''")
                cursor.execute("INSERT INTO arrow_full (id, name, created_at) VALUES (4, 'd', '0000-00-00 00:00:00')")

        self.catalog = test_utils.discover_catalog(self.conn, {})
        for stream in self.catalog.streams:
            stream.metadata = [
                {'breadcrumb': (), 'metadata': {'selected': True, 'table-key-properties': ['id'],
                                                'database-name': 'tap_mysql_test'}},
                {'breadcrumb': ('properties', 'id'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'name'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'created_at'), 'metadata': {'selected': True}},
            ]
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

    def tearDown(self):
        self._write_message_patcher.stop()

    def test_full_table_sync_emits_arrow_batch_files_with_correct_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                tap_mysql.do_sync(self.conn, {
                    'batch_size_rows': 2,
                    'batch_root_dir': tmpdir,
                    'batch_format': 'arrow',
                }, self.catalog, {})

            batch_messages = _read_batch_messages(out.getvalue())
            # batch_size_rows is a flush threshold, not an exact chunk size: the driver may
            # return all rows in a single underlying RecordBatch, producing one BATCH file.
            self.assertGreaterEqual(len(batch_messages), 1)
            for msg in batch_messages:
                self.assertEqual(msg['type'], 'BATCH')
                self.assertEqual(msg['stream'], 'tap_mysql_test-arrow_full')
                self.assertEqual(msg['encoding'], {'format': 'arrow'})

            rows_by_id = {}
            for msg in batch_messages:
                table = _read_arrow_table(msg)
                for id_, name, created_at in zip(table.column('id').to_pylist(),
                                                 table.column('name').to_pylist(),
                                                 table.column('created_at').to_pylist()):
                    rows_by_id[id_] = (name, created_at)

        self.assertEqual(sorted(rows_by_id), [1, 2, 3, 4])
        self.assertEqual(rows_by_id[1][0], 'a')
        self.assertIsNotNone(rows_by_id[1][1])
        # the invalid zero-date row must come through as NULL, not crash the ADBC read
        self.assertIsNone(rows_by_id[4][1])

        # BATCH messages bypass write_message, but SCHEMA/ACTIVATE_VERSION/STATE don't
        message_types = [type(m).__name__ for m in self.singer_messages]
        self.assertIn('SchemaMessage', message_types)
        self.assertIn('ActivateVersionMessage', message_types)
        self.assertIn('StateMessage', message_types)

    def test_batch_format_alone_is_sufficient_to_enable_arrow_batch_mode(self):
        # no batch_size_rows -- batch_format alone should opt into BATCH mode
        with tempfile.TemporaryDirectory() as tmpdir:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                tap_mysql.do_sync(self.conn, {
                    'batch_root_dir': tmpdir,
                    'batch_format': 'arrow',
                }, self.catalog, {})

            batch_messages = _read_batch_messages(out.getvalue())
            self.assertEqual(len(batch_messages), 1)
            self.assertEqual(batch_messages[0]['encoding'], {'format': 'arrow'})
            table = _read_arrow_table(batch_messages[0])

        self.assertEqual(sorted(table.column('id').to_pylist()), [1, 2, 3, 4])


@unittest.skipUnless(_arrow_support_available(), 'MySQL ADBC driver not installed')
class TestArrowBatchIncremental(unittest.TestCase):
    """Exercises INCREMENTAL sync with batch_format='arrow', specifically to verify that
    ADBC's MySQL driver accepts the same named-placeholder parameter style
    (`%(replication_key_value)s`) that incremental.py's WHERE clause uses -- this can't be
    verified by mocked unit tests and was an explicitly-flagged open risk.
    """

    def setUp(self):
        self.conn = test_utils.get_test_connection()
        self._write_message_patcher = patch('tap_mysql.stream_utils.write_message')
        self._write_message_patcher.start()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE arrow_incremental (id int primary key, updated datetime)')
                cursor.execute("INSERT INTO arrow_incremental (id, updated) VALUES (1, '2020-01-01 00:00:00')")
                cursor.execute("INSERT INTO arrow_incremental (id, updated) VALUES (2, '2020-01-02 00:00:00')")
                cursor.execute("INSERT INTO arrow_incremental (id, updated) VALUES (3, '2020-01-03 00:00:00')")

        self.catalog = test_utils.discover_catalog(self.conn, {})
        for stream in self.catalog.streams:
            stream.metadata = [
                {'breadcrumb': (), 'metadata': {'selected': True, 'table-key-properties': [],
                                                'database-name': 'tap_mysql_test'}},
                {'breadcrumb': ('properties', 'id'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'updated'), 'metadata': {'selected': True}},
            ]
            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'INCREMENTAL', 'updated')

    def tearDown(self):
        self._write_message_patcher.stop()

    def test_incremental_sync_filters_rows_via_bookmark(self):
        state = {
            'bookmarks': {
                'tap_mysql_test-arrow_incremental': {
                    'replication_key': 'updated',
                    'replication_key_value': '2020-01-02 00:00:00',
                },
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                tap_mysql.do_sync(self.conn, {
                    'batch_size_rows': 10,
                    'batch_root_dir': tmpdir,
                    'batch_format': 'arrow',
                }, self.catalog, state)

            batch_messages = _read_batch_messages(out.getvalue())
            self.assertEqual(len(batch_messages), 1)
            table = _read_arrow_table(batch_messages[0])

        # WHERE `updated` >= '2020-01-02 00:00:00' -> only ids 2 and 3
        self.assertEqual(sorted(table.column('id').to_pylist()), [2, 3])

        bookmark = state['bookmarks']['tap_mysql_test-arrow_incremental']
        self.assertEqual(bookmark['replication_key'], 'updated')
        self.assertIsNotNone(bookmark['replication_key_value'])


if __name__ == '__main__':
    unittest.main()
