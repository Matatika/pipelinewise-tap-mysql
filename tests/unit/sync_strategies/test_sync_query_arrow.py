import contextlib
import io
import json
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import singer.metrics
from singer import Schema
from singer.catalog import CatalogEntry

from tap_mysql.sync_strategies import common

STREAM_ID = 'my_db-mytable'


def _full_table_entry(key_properties=('id',)):
    return CatalogEntry(
        table='mytable',
        stream=STREAM_ID,
        tap_stream_id=STREAM_ID,
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
                    'table-key-properties': list(key_properties),
                },
            },
        ],
    )


def _incremental_entry(replication_key='updated_at'):
    return CatalogEntry(
        table='mytable',
        stream=STREAM_ID,
        tap_stream_id=STREAM_ID,
        schema=Schema(properties={
            'id': Schema(type=['null', 'integer']),
            'updated_at': Schema(type=['null', 'integer']),
        }),
        metadata=[
            {
                'breadcrumb': [],
                'metadata': {
                    'database-name': 'my_db',
                    'replication-method': 'INCREMENTAL',
                    'replication-key': replication_key,
                },
            },
        ],
    )


@contextlib.contextmanager
def _fake_reader(batches):
    yield iter(batches)


class TestSyncQueryDispatch(unittest.TestCase):

    def test_dispatches_to_arrow_branch_when_format_is_arrow(self):
        cursor = mock.MagicMock()
        batch_config = mock.MagicMock(format='arrow')

        with mock.patch.object(common, '_sync_query_arrow') as mock_arrow_sync:
            common.sync_query(cursor, _full_table_entry(), {}, 'SELECT 1', ['id'], 1, {},
                              batch_config=batch_config, mysql_conn=mock.sentinel.mysql_conn)

        mock_arrow_sync.assert_called_once()
        cursor.execute.assert_not_called()

    def test_jsonl_path_never_touches_adbc(self):
        cursor = mock.MagicMock()
        cursor.fetchmany.return_value = []

        with mock.patch('tap_mysql.adbc.connect') as mock_connect:
            common.sync_query(cursor, _full_table_entry(), {}, 'SELECT 1', ['id', 'name'], 1, {},
                              batch_config=None)

        mock_connect.assert_not_called()
        cursor.execute.assert_called_once()


class TestSyncQueryArrow(unittest.TestCase):

    def _run(self, catalog_entry, state, batches, batch_size=100, stdout=None):
        mysql_conn = mock.MagicMock()
        mysql_conn.raw_config = {'host': 'h', 'port': 3306, 'user': 'u', 'password': 'p'}

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch('tap_mysql.adbc.require_arrow_support'):
                batch_config = common.BatchConfig(batch_size=batch_size, batch_root_dir=tmpdir, format='arrow')

            with mock.patch('tap_mysql.adbc.stream_record_batches', return_value=_fake_reader(batches)):
                with contextlib.redirect_stdout(stdout or io.StringIO()):
                    common._sync_query_arrow(mysql_conn, catalog_entry, state, 'SELECT * FROM mytable', {},
                                             batch_config)
        return state

    def test_full_table_updates_last_pk_fetched_per_batch(self):
        state = {'bookmarks': {STREAM_ID: {'max_pk_values': {'id': 5}}}}
        batch1 = pa.RecordBatch.from_pylist([{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}, {'id': 3, 'name': 'c'}])
        batch2 = pa.RecordBatch.from_pylist([{'id': 4, 'name': 'd'}, {'id': 5, 'name': 'e'}])

        state = self._run(_full_table_entry(), state, [batch1, batch2], batch_size=100)

        self.assertEqual(state['bookmarks'][STREAM_ID]['last_pk_fetched'], {'id': 5})

    def test_incremental_updates_replication_key_value_per_batch(self):
        state = {'bookmarks': {STREAM_ID: {'replication_key': 'updated_at'}}}
        batch1 = pa.RecordBatch.from_pylist([{'id': 1, 'updated_at': 10}, {'id': 2, 'updated_at': 20}])
        batch2 = pa.RecordBatch.from_pylist([{'id': 3, 'updated_at': 30}])

        state = self._run(_incremental_entry(), state, [batch1, batch2], batch_size=100)

        self.assertEqual(state['bookmarks'][STREAM_ID]['replication_key_value'], 30)

    def test_state_checkpoints_align_with_batch_writer_flush(self):
        state = {'bookmarks': {STREAM_ID: {'max_pk_values': {'id': 5}}}}
        batch1 = pa.RecordBatch.from_pylist([{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}, {'id': 3, 'name': 'c'}])
        batch2 = pa.RecordBatch.from_pylist([{'id': 4, 'name': 'd'}, {'id': 5, 'name': 'e'}])

        state_messages = []
        with mock.patch('tap_mysql.stream_utils.write_message', side_effect=lambda m: state_messages.append(m)):
            # batch_size=3: batch1 (3 rows) triggers an auto-flush/checkpoint; batch2 (2 rows)
            # doesn't reach the threshold, so it's only flushed (and STATE-written) at the end.
            self._run(_full_table_entry(), state, [batch1, batch2], batch_size=3)

        self.assertEqual(len(state_messages), 2)

    def test_metrics_counter_incremented_by_batch_num_rows_not_per_row(self):
        state = {'bookmarks': {STREAM_ID: {'max_pk_values': {'id': 5}}}}
        batch1 = pa.RecordBatch.from_pylist([{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}, {'id': 3, 'name': 'c'}])
        batch2 = pa.RecordBatch.from_pylist([{'id': 4, 'name': 'd'}, {'id': 5, 'name': 'e'}])

        with mock.patch.object(singer.metrics.Counter, 'increment', autospec=True) as mock_increment:
            self._run(_full_table_entry(), state, [batch1, batch2], batch_size=100)

        call_amounts = [call.args[1] for call in mock_increment.call_args_list]
        self.assertEqual(call_amounts, [3, 2])

    def test_batch_message_emitted_with_arrow_encoding(self):
        state = {'bookmarks': {STREAM_ID: {}}}
        batch1 = pa.RecordBatch.from_pylist([{'id': 1, 'name': 'a'}])

        out = io.StringIO()
        self._run(_full_table_entry(), state, [batch1], batch_size=100, stdout=out)

        lines = [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]
        batch_messages = [line for line in lines if line.get('type') == 'BATCH']
        self.assertEqual(len(batch_messages), 1)
        self.assertEqual(batch_messages[0]['encoding'], {'format': 'arrow'})


if __name__ == '__main__':
    unittest.main()
