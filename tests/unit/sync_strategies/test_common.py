import datetime
import decimal
import gzip
import io
import json
import os
import tempfile
import uuid
from unittest import mock

import orjson
import pytest
from singer import MetadataMapping, Schema
from singer.catalog import CatalogEntry

from tap_mysql.sync_strategies import common
from tap_mysql.sync_strategies.common import BatchConfig, BatchWriter, generate_select_sql


def _make_catalog_entry(properties, database_name='my_db', table='mytable'):
    return CatalogEntry(
        table=table,
        stream=f'{database_name}-{table}',
        tap_stream_id=f'{database_name}-{table}',
        schema=Schema(properties=properties),
        metadata=MetadataMapping.from_iterable([{'breadcrumb': [], 'metadata': {'database-name': database_name}}]),
    )


class TestBatchWriter:
    def _make_writer(self, tmpdir, batch_size, output=None):
        return BatchWriter('mystream', BatchConfig(batch_size=batch_size, batch_root_dir=tmpdir), output=output)

    # ------------------------------------------------------------------
    # flush() with no rows written is a no-op
    # ------------------------------------------------------------------

    def test_flush_with_no_rows_is_noop(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.flush()
        assert out.getvalue() == ''

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
                }
            )

    # ------------------------------------------------------------------
    # write() accumulates rows; flush() below batch_size
    # ------------------------------------------------------------------

    def test_partial_batch_emitted_on_flush(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=100, output=out)

            flushed = writer.write({'id': 1, 'name': 'alice'})
            assert not flushed
            flushed = writer.write({'id': 2, 'name': 'bob'})
            assert not flushed

            writer.flush()

            # one BATCH message emitted
            messages = [json.loads(line) for line in out.getvalue().splitlines()]
            assert len(messages) == 1
            msg = messages[0]
            assert msg['type'] == 'BATCH'
            assert msg['stream'] == 'mystream'
            assert msg['encoding'] == {'format': 'jsonl', 'compression': 'gzip'}

            # manifest points to a real file
            path = msg['manifest'][0].removeprefix('file://')
            assert os.path.isfile(path)

            # file contains the two records
            with gzip.open(path, 'rb') as f:
                lines = f.read().splitlines()
            assert len(lines) == 2
            assert orjson.loads(lines[0]) == {'id': 1, 'name': 'alice'}
            assert orjson.loads(lines[1]) == {'id': 2, 'name': 'bob'}

    # ------------------------------------------------------------------
    # write() returns True and auto-flushes when batch_size is reached
    # ------------------------------------------------------------------

    def test_write_returns_true_and_flushes_at_batch_size(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=3, output=out)

            results = [writer.write({'id': i}) for i in range(3)]

            # only the last write triggers the flush
            assert results == [False, False, True]

            messages = [json.loads(line) for line in out.getvalue().splitlines()]
            assert len(messages) == 1

            path = messages[0]['manifest'][0].removeprefix('file://')
            with gzip.open(path, 'rb') as f:
                lines = f.read().splitlines()
            assert len(lines) == 3

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
        assert len(messages) == 3
        # each batch has its own unique file
        paths = [m['manifest'][0] for m in messages]
        assert len(set(paths)) == 3

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
        assert len(messages) == 3

    # ------------------------------------------------------------------
    # writer resets cleanly after each flush
    # ------------------------------------------------------------------

    def test_writer_resets_after_flush(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)

            writer.write({'id': 1})
            writer.flush()
            assert writer._gz is None
            assert writer._path is None
            assert writer._row_count == 0

            # second write starts a fresh file
            writer.write({'id': 2})
            writer.flush()

        messages = [json.loads(line) for line in out.getvalue().splitlines()]
        assert len(messages) == 2
        assert messages[0]['manifest'][0] != messages[1]['manifest'][0]

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
        assert len(msg['manifest']) == 1
        assert msg['manifest'][0].startswith('file://')

    def test_batch_file_written_to_batch_root_dir(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.write({'id': 1})
            writer.flush()

        msg = json.loads(out.getvalue())
        path = msg['manifest'][0].removeprefix('file://')
        assert path.startswith(tmpdir)

    def test_batch_file_name_pattern(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.write({'id': 1})
            writer.flush()

        msg = json.loads(out.getvalue())
        filename = os.path.basename(msg['manifest'][0].removeprefix('file://'))
        assert filename.startswith('tap-mysql-')
        assert filename.endswith('.jsonl.gz')


class TestBatchConfig:
    def test_valid_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = BatchConfig(batch_size=1000, batch_root_dir=tmpdir)
            assert cfg.batch_size == 1000
            assert cfg.batch_root_dir == tmpdir

    def test_default_batch_root_dir_is_cwd(self):
        cfg = BatchConfig(batch_size=1000)
        assert cfg.batch_root_dir == '.'

    def test_invalid_batch_size_raises(self):
        with pytest.raises(ValueError):
            BatchConfig(batch_size=0)
        with pytest.raises(ValueError):
            BatchConfig(batch_size=-1)

    def test_nonexistent_batch_root_dir_raises(self):
        with pytest.raises(ValueError):
            BatchConfig(batch_size=1000, batch_root_dir='/nonexistent/path/xyz')

    def test_is_frozen(self):
        cfg = BatchConfig(batch_size=1000)
        with pytest.raises(Exception):
            cfg.batch_size = 999  # type: ignore[misc]

    def test_defaults_to_jsonl_gzip(self):
        cfg = BatchConfig(batch_size=1000)
        assert cfg.format == 'jsonl'
        assert cfg.compression == 'gzip'
        assert cfg.gzip_compressed

    def test_invalid_batch_format_raises(self):
        with pytest.raises(ValueError):
            BatchConfig(batch_size=1000, format='xml')

    def test_invalid_compression_raises(self):
        with pytest.raises(ValueError):
            BatchConfig(batch_size=1000, compression='brotli')

    def test_arrow_format_validates_via_require_arrow_support(self):
        with mock.patch('tap_mysql.adbc.require_arrow_support') as mock_require:
            BatchConfig(batch_size=1000, format='arrow')
        mock_require.assert_called_once()


class TestBatchConfigFromConfig:
    """BatchConfig.from_config parses the Meltano Singer SDK-shaped nested `batch_config`
    key: {"batch_config": {"encoding": {...}, "storage": {...}, "batch_size": ...}}."""

    def test_returns_none_when_batch_config_key_absent(self):
        assert BatchConfig.from_config({}) is None

    def test_empty_batch_config_is_sufficient_to_opt_in_with_defaults(self):
        cfg = BatchConfig.from_config({'batch_config': {}})
        assert cfg is not None
        assert cfg.batch_size == common.DEFAULT_BATCH_SIZE
        assert cfg.format == 'jsonl'
        assert cfg.compression == 'gzip'

    def test_defaults_storage_root_to_os_temp_dir(self):
        cfg = BatchConfig.from_config({'batch_config': {}})
        assert cfg is not None
        assert cfg.batch_root_dir == tempfile.gettempdir()

    def test_reads_storage_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = BatchConfig.from_config({'batch_config': {'storage': {'root': tmpdir}}})
            assert cfg is not None
            assert cfg.batch_root_dir == tmpdir

    def test_reads_batch_size(self):
        cfg = BatchConfig.from_config({'batch_config': {'batch_size': 250000}})
        assert cfg is not None
        assert cfg.batch_size == 250000

    def test_coerces_string_batch_size(self):
        cfg = BatchConfig.from_config({'batch_config': {'batch_size': '250000'}})
        assert cfg is not None
        assert cfg.batch_size == 250000

    def test_reads_encoding_format_and_compression(self):
        with mock.patch('tap_mysql.adbc.require_arrow_support'):
            cfg = BatchConfig.from_config({'batch_config': {'encoding': {'format': 'arrow'}}})
        assert cfg is not None
        assert cfg.format == 'arrow'

    def test_encoding_compression_none_disables_gzip(self):
        cfg = BatchConfig.from_config({'batch_config': {'encoding': {'compression': 'none'}}})
        assert cfg is not None
        assert not cfg.gzip_compressed


class TestGenerateSelectSql:
    def _entry(self):
        return _make_catalog_entry(
            {
                'id': Schema(type=['null', 'integer']),
                'created_at': Schema(type=['null', 'string'], format='date-time'),
                'updated_at': Schema(type=['null', 'string'], format='time'),
                'photo': Schema(type=['null', 'string'], format='binary'),
                'location': Schema(type=['null', 'object'], format='spatial'),
            }
        )

    def test_null_invalid_dates_false_default_leaves_datetime_columns_unwrapped(self):
        sql = generate_select_sql(self._entry(), ['id', 'created_at'])
        assert 'NULLIF' not in sql
        assert '`created_at`' in sql

    def test_null_invalid_dates_true_does_not_affect_time_column(self):
        sql = generate_select_sql(self._entry(), ['updated_at'])
        assert 'NULLIF' not in sql
        assert '`updated_at`' in sql

    def test_null_invalid_dates_true_does_not_affect_binary_or_spatial_columns(self):
        sql = generate_select_sql(self._entry(), ['photo', 'location'])
        assert 'NULLIF' not in sql
        assert 'hex(`photo`) as `photo`' in sql
        assert 'ST_AsGeoJSON(`location`) as `location`' in sql

    def test_null_invalid_dates_true_does_not_affect_plain_columns(self):
        sql = generate_select_sql(self._entry(), ['id'])
        assert 'NULLIF' not in sql
        assert '`id`' in sql

    def test_percent_escaping_still_applied_with_nullif(self):
        sql = generate_select_sql(self._entry(), ['created_at'])
        assert '%%%%' not in sql


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


class TestSyncQueryStateDeduplication:
    """sync_query's JSONL/BATCH path shares the same checkpoint-boundary STATE-emission
    logic as _sync_query_arrow -- guards against the identical duplicate-StateMessage bug
    on that path too."""

    def _full_table_entry(self, stream_id='my_db-mytable'):
        return CatalogEntry(
            table='mytable',
            stream=stream_id,
            tap_stream_id=stream_id,
            schema=Schema(properties={'id': Schema(type=['null', 'integer']), 'name': Schema(type=['null', 'string'])}),
            metadata=MetadataMapping.from_iterable(
                [
                    {
                        'breadcrumb': [],
                        'metadata': {
                            'database-name': 'my_db',
                            'replication-method': 'FULL_TABLE',
                            'table-key-properties': ['id'],
                        },
                    }
                ]
            ),
        )

    def test_no_duplicate_state_when_last_row_lands_on_checkpoint_boundary(self):
        stream_id = 'my_db-mytable'
        state = {'bookmarks': {stream_id: {'max_pk_values': {'id': 3}}}}
        cursor = _FakeCursor([(1, 'a'), (2, 'b'), (3, 'c')])

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_config = BatchConfig(batch_size=3, batch_root_dir=tmpdir)
            state_messages = []
            with mock.patch('tap_mysql.stream_utils.write_message', side_effect=state_messages.append):
                common.sync_query(
                    cursor,
                    self._full_table_entry(stream_id),
                    state,
                    'SELECT 1',
                    ['id', 'name'],
                    1,
                    {},
                    batch_config=batch_config,
                )

        assert len(state_messages) == 1
