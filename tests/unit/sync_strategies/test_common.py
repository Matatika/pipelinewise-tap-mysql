import datetime
import decimal
import gzip
import io
import json
import os
import tempfile
import unittest
import uuid
from unittest import mock

import orjson
from singer import Schema
from singer.catalog import CatalogEntry

from tap_mysql.sync_strategies import common
from tap_mysql.sync_strategies.common import BatchConfig, BatchWriter, generate_select_sql


def _make_catalog_entry(properties, database_name='my_db', table='mytable'):
    return CatalogEntry(
        table=table,
        stream=f'{database_name}-{table}',
        tap_stream_id=f'{database_name}-{table}',
        schema=Schema(properties=properties),
        metadata=[
            {
                'breadcrumb': [],
                'metadata': {'database-name': database_name},
            },
        ],
    )


class TestBatchWriter(unittest.TestCase):

    def _make_writer(self, tmpdir, batch_size, output=None):
        return BatchWriter('mystream', BatchConfig(batch_size=batch_size, batch_root_dir=tmpdir),
                           output=output)

    # ------------------------------------------------------------------
    # flush() with no rows written is a no-op
    # ------------------------------------------------------------------

    def test_flush_with_no_rows_is_noop(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.flush()
        self.assertEqual(out.getvalue(), '')

    # ------------------------------------------------------------------
    # decimal.Decimal handling
    # ------------------------------------------------------------------

    def test_dump_special_types(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=100, output=out)
            writer.write(
                {
                    'id': 1,
                    'a_decimal': decimal.Decimal('1.618'),
                    'a_date': datetime.datetime.now(tz=datetime.timezone.utc).date(),
                    'a_datetime': datetime.datetime.now(tz=datetime.timezone.utc),
                    'a_uuid': uuid.uuid4(),
                },
            )

    # ------------------------------------------------------------------
    # write() accumulates rows; flush() below batch_size
    # ------------------------------------------------------------------

    def test_partial_batch_emitted_on_flush(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=100, output=out)

            flushed = writer.write({'id': 1, 'name': 'alice'})
            self.assertFalse(flushed)
            flushed = writer.write({'id': 2, 'name': 'bob'})
            self.assertFalse(flushed)

            writer.flush()

            # one BATCH message emitted
            messages = [json.loads(line) for line in out.getvalue().splitlines()]
            self.assertEqual(len(messages), 1)
            msg = messages[0]
            self.assertEqual(msg['type'], 'BATCH')
            self.assertEqual(msg['stream'], 'mystream')
            self.assertEqual(msg['encoding'], {'format': 'jsonl', 'compression': 'gzip'})

            # manifest points to a real file
            path = msg['manifest'][0].removeprefix('file://')
            self.assertTrue(os.path.isfile(path))

            # file contains the two records
            with gzip.open(path, 'rb') as f:
                lines = f.read().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertEqual(orjson.loads(lines[0]), {'id': 1, 'name': 'alice'})
            self.assertEqual(orjson.loads(lines[1]), {'id': 2, 'name': 'bob'})

    # ------------------------------------------------------------------
    # write() returns True and auto-flushes when batch_size is reached
    # ------------------------------------------------------------------

    def test_write_returns_true_and_flushes_at_batch_size(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=3, output=out)

            results = [writer.write({'id': i}) for i in range(3)]

            # only the last write triggers the flush
            self.assertEqual(results, [False, False, True])

            messages = [json.loads(line) for line in out.getvalue().splitlines()]
            self.assertEqual(len(messages), 1)

            path = messages[0]['manifest'][0].removeprefix('file://')
            with gzip.open(path, 'rb') as f:
                lines = f.read().splitlines()
            self.assertEqual(len(lines), 3)

    # ------------------------------------------------------------------
    # multiple full batches produce one BATCH message each
    # ------------------------------------------------------------------

    def test_multiple_full_batches(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=2, output=out)

            for i in range(6):
                writer.write({'id': i})

            writer.flush()  # no remaining rows - no-op

        messages = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(len(messages), 3)
        # each batch has its own unique file
        paths = [m['manifest'][0] for m in messages]
        self.assertEqual(len(set(paths)), 3)

    # ------------------------------------------------------------------
    # partial remainder after full batches
    # ------------------------------------------------------------------

    def test_partial_remainder_after_full_batches(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=3, output=out)

            for i in range(7):
                writer.write({'id': i})
            writer.flush()

        messages = [json.loads(line) for line in out.getvalue().splitlines()]
        # 2 full batches (3 rows each) + 1 partial (1 row)
        self.assertEqual(len(messages), 3)

    # ------------------------------------------------------------------
    # writer resets cleanly after each flush
    # ------------------------------------------------------------------

    def test_writer_resets_after_flush(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)

            writer.write({'id': 1})
            writer.flush()
            self.assertIsNone(writer._gz)
            self.assertIsNone(writer._path)
            self.assertEqual(writer._row_count, 0)

            # second write starts a fresh file
            writer.write({'id': 2})
            writer.flush()

        messages = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(len(messages), 2)
        self.assertNotEqual(messages[0]['manifest'][0], messages[1]['manifest'][0])

    # ------------------------------------------------------------------
    # BATCH message structure
    # ------------------------------------------------------------------

    def test_batch_message_manifest_is_file_uri(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.write({'id': 1})
            writer.flush()

        msg = json.loads(out.getvalue())
        self.assertEqual(len(msg['manifest']), 1)
        self.assertTrue(msg['manifest'][0].startswith('file://'))

    def test_batch_file_written_to_batch_root_dir(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.write({'id': 1})
            writer.flush()

        msg = json.loads(out.getvalue())
        path = msg['manifest'][0].removeprefix('file://')
        self.assertTrue(path.startswith(tmpdir))

    def test_batch_file_name_pattern(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.write({'id': 1})
            writer.flush()

        msg = json.loads(out.getvalue())
        filename = os.path.basename(msg['manifest'][0].removeprefix('file://'))
        self.assertTrue(filename.startswith('tap-mysql-'))
        self.assertTrue(filename.endswith('.jsonl.gz'))


class TestBatchConfig(unittest.TestCase):

    def test_valid_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = BatchConfig(batch_size=1000, batch_root_dir=tmpdir)
            self.assertEqual(cfg.batch_size, 1000)
            self.assertEqual(cfg.batch_root_dir, tmpdir)

    def test_default_batch_root_dir_is_cwd(self):
        cfg = BatchConfig(batch_size=1000)
        self.assertEqual(cfg.batch_root_dir, '.')

    def test_invalid_batch_size_raises(self):
        with self.assertRaises(ValueError):
            BatchConfig(batch_size=0)
        with self.assertRaises(ValueError):
            BatchConfig(batch_size=-1)

    def test_nonexistent_batch_root_dir_raises(self):
        with self.assertRaises(ValueError):
            BatchConfig(batch_size=1000, batch_root_dir='/nonexistent/path/xyz')

    def test_is_frozen(self):
        cfg = BatchConfig(batch_size=1000)
        with self.assertRaises(Exception):
            cfg.batch_size = 999  # type: ignore[misc]

    def test_defaults_to_jsonl_gzip(self):
        cfg = BatchConfig(batch_size=1000)
        self.assertEqual(cfg.format, 'jsonl')
        self.assertEqual(cfg.compression, 'gzip')
        self.assertTrue(cfg.gzip_compressed)

    def test_invalid_batch_format_raises(self):
        with self.assertRaises(ValueError):
            BatchConfig(batch_size=1000, format='xml')

    def test_invalid_compression_raises(self):
        with self.assertRaises(ValueError):
            BatchConfig(batch_size=1000, compression='brotli')

    def test_arrow_format_without_arrow_support_raises_actionable_error(self):
        from tap_mysql import adbc

        with mock.patch.object(adbc, '_import_adbc', side_effect=adbc.ArrowSupportError('missing extra')):
            with self.assertRaises(adbc.ArrowSupportError):
                BatchConfig(batch_size=1000, format='arrow')

    def test_arrow_format_validates_via_require_arrow_support(self):
        with mock.patch('tap_mysql.adbc.require_arrow_support') as mock_require:
            BatchConfig(batch_size=1000, format='arrow')
        mock_require.assert_called_once()


class TestBatchConfigFromConfig(unittest.TestCase):
    """BatchConfig.from_config parses the Meltano Singer SDK-shaped nested `batch_config`
    key: {"batch_config": {"encoding": {...}, "storage": {...}, "batch_size": ...}}."""

    def test_returns_none_when_batch_config_key_absent(self):
        self.assertIsNone(BatchConfig.from_config({}))

    def test_empty_batch_config_is_sufficient_to_opt_in_with_defaults(self):
        cfg = BatchConfig.from_config({'batch_config': {}})
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.batch_size, common.DEFAULT_BATCH_SIZE)
        self.assertEqual(cfg.format, 'jsonl')
        self.assertEqual(cfg.compression, 'gzip')

    def test_defaults_storage_root_to_os_temp_dir(self):
        cfg = BatchConfig.from_config({'batch_config': {}})
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.batch_root_dir, tempfile.gettempdir())

    def test_reads_storage_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = BatchConfig.from_config({'batch_config': {'storage': {'root': tmpdir}}})
            self.assertIsNotNone(cfg)
            self.assertEqual(cfg.batch_root_dir, tmpdir)

    def test_reads_batch_size(self):
        cfg = BatchConfig.from_config({'batch_config': {'batch_size': 250000}})
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.batch_size, 250000)

    def test_coerces_string_batch_size(self):
        cfg = BatchConfig.from_config({'batch_config': {'batch_size': '250000'}})
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.batch_size, 250000)

    def test_reads_encoding_format_and_compression(self):
        with mock.patch('tap_mysql.adbc.require_arrow_support'):
            cfg = BatchConfig.from_config({'batch_config': {'encoding': {'format': 'arrow'}}})
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.format, 'arrow')

    def test_encoding_compression_none_disables_gzip(self):
        cfg = BatchConfig.from_config({'batch_config': {'encoding': {'compression': 'none'}}})
        self.assertIsNotNone(cfg)
        self.assertFalse(cfg.gzip_compressed)


class TestGenerateSelectSql(unittest.TestCase):

    def _entry(self):
        return _make_catalog_entry({
            'id': Schema(type=['null', 'integer']),
            'created_at': Schema(type=['null', 'string'], format='date-time'),
            'updated_at': Schema(type=['null', 'string'], format='time'),
            'photo': Schema(type=['null', 'string'], format='binary'),
            'location': Schema(type=['null', 'object'], format='spatial'),
        })

    def test_null_invalid_dates_false_default_leaves_datetime_columns_unwrapped(self):
        sql = generate_select_sql(self._entry(), ['id', 'created_at'])
        self.assertNotIn('NULLIF', sql)
        self.assertIn('`created_at`', sql)

    def test_null_invalid_dates_true_wraps_date_time_columns_in_nullif_and_cast(self):
        sql = generate_select_sql(self._entry(), ['id', 'created_at'], null_invalid_dates=True)
        self.assertIn(
            "CAST(NULLIF(NULLIF(`created_at`, '0000-00-00'), '0000-00-00 00:00:00') AS DATETIME) as `created_at`",
            sql)

    def test_null_invalid_dates_true_does_not_affect_time_column(self):
        sql = generate_select_sql(self._entry(), ['updated_at'], null_invalid_dates=True)
        self.assertNotIn('NULLIF', sql)
        self.assertIn('`updated_at`', sql)

    def test_null_invalid_dates_true_does_not_affect_binary_or_spatial_columns(self):
        sql = generate_select_sql(self._entry(), ['photo', 'location'], null_invalid_dates=True)
        self.assertNotIn('NULLIF', sql)
        self.assertIn('hex(`photo`) as `photo`', sql)
        self.assertIn('ST_AsGeoJSON(`location`) as `location`', sql)

    def test_null_invalid_dates_true_does_not_affect_plain_columns(self):
        sql = generate_select_sql(self._entry(), ['id'], null_invalid_dates=True)
        self.assertNotIn('NULLIF', sql)
        self.assertIn('`id`', sql)

    def test_percent_escaping_still_applied_with_nullif(self):
        sql = generate_select_sql(self._entry(), ['created_at'], null_invalid_dates=True)
        self.assertNotIn('%%%%', sql)
        self.assertEqual(sql.count('NULLIF'), 2)  # nested NULLIF(NULLIF(...))


class _FakeCursor:
    """Minimal fake mysql-connector-style cursor: returns rows once, then exhausts."""

    def __init__(self, rows):
        self._batches = [rows]

    def execute(self, sql, params):
        pass

    def fetchmany(self, size):
        if self._batches:
            return self._batches.pop(0)
        return []


class TestSyncQueryStateDeduplication(unittest.TestCase):
    """sync_query's JSONL/BATCH path shares the same checkpoint-boundary STATE-emission
    logic as _sync_query_arrow -- guards against the identical duplicate-StateMessage bug
    on that path too."""

    def _full_table_entry(self, stream_id='my_db-mytable'):
        return CatalogEntry(
            table='mytable',
            stream=stream_id,
            tap_stream_id=stream_id,
            schema=Schema(properties={
                'id': Schema(type=['null', 'integer']),
                'name': Schema(type=['null', 'string']),
            }),
            metadata=[
                {
                    'breadcrumb': [],
                    'metadata': {
                        'database-name': 'my_db',
                        'replication-method': 'FULL_TABLE',
                        'table-key-properties': ['id'],
                    },
                },
            ],
        )

    def test_no_duplicate_state_when_last_row_lands_on_checkpoint_boundary(self):
        stream_id = 'my_db-mytable'
        state = {'bookmarks': {stream_id: {'max_pk_values': {'id': 3}}}}
        cursor = _FakeCursor([(1, 'a'), (2, 'b'), (3, 'c')])

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_config = BatchConfig(batch_size=3, batch_root_dir=tmpdir)
            state_messages = []
            with mock.patch('tap_mysql.stream_utils.write_message', side_effect=state_messages.append):
                common.sync_query(cursor, self._full_table_entry(stream_id), state, 'SELECT 1',
                                  ['id', 'name'], 1, {}, batch_config=batch_config)

        self.assertEqual(len(state_messages), 1)


if __name__ == '__main__':
    unittest.main()
