import io
import json
import os
import tempfile
import unittest

import pyarrow as pa
import pyarrow.ipc as ipc

from tap_mysql.sync_strategies.common import ArrowBatchWriter, BatchConfig


def _record_batch(ids):
    return pa.RecordBatch.from_pylist([{'id': i} for i in ids])


def _empty_record_batch():
    schema = pa.schema([('id', pa.int64())])
    return pa.RecordBatch.from_pylist([], schema=schema)


class TestArrowBatchWriter(unittest.TestCase):

    def _make_writer(self, tmpdir, batch_size, output=None):
        return ArrowBatchWriter('mystream', BatchConfig(batch_size=batch_size, batch_root_dir=tmpdir, format='arrow'),
                                output=output)

    def test_flush_with_no_rows_is_noop(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.flush()
        self.assertEqual(out.getvalue(), '')

    def test_write_ignores_empty_record_batch(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            flushed = writer.write(_empty_record_batch())
            self.assertFalse(flushed)
            self.assertEqual(writer._row_count, 0)
            self.assertIsNone(writer._schema)
            writer.flush()
        self.assertEqual(out.getvalue(), '')

    def test_partial_batch_emitted_on_flush(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=100, output=out)

            flushed = writer.write(_record_batch([1, 2]))
            self.assertFalse(flushed)
            flushed = writer.write(_record_batch([3]))
            self.assertFalse(flushed)

            writer.flush()

            messages = [json.loads(line) for line in out.getvalue().splitlines()]
            self.assertEqual(len(messages), 1)
            msg = messages[0]
            self.assertEqual(msg['type'], 'BATCH')
            self.assertEqual(msg['stream'], 'mystream')
            self.assertEqual(msg['encoding'], {'format': 'arrow'})

            path = msg['manifest'][0].removeprefix('file://')
            self.assertTrue(os.path.isfile(path))

            table = ipc.open_file(path).read_all()
            self.assertEqual(table.column('id').to_pylist(), [1, 2, 3])

    def test_write_returns_true_and_flushes_at_batch_size(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=3, output=out)

            results = [writer.write(_record_batch([i])) for i in range(3)]
            self.assertEqual(results, [False, False, True])

            messages = [json.loads(line) for line in out.getvalue().splitlines()]
            self.assertEqual(len(messages), 1)

            path = messages[0]['manifest'][0].removeprefix('file://')
            table = ipc.open_file(path).read_all()
            self.assertEqual(table.num_rows, 3)

    def test_multiple_full_batches(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=2, output=out)

            for i in range(6):
                writer.write(_record_batch([i]))

            writer.flush()  # no remaining rows - no-op

        messages = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(len(messages), 3)
        paths = [m['manifest'][0] for m in messages]
        self.assertEqual(len(set(paths)), 3)

    def test_writer_resets_after_flush(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)

            writer.write(_record_batch([1]))
            writer.flush()
            self.assertEqual(writer._batches, [])
            self.assertEqual(writer._row_count, 0)
            self.assertIsNone(writer._schema)

            writer.write(_record_batch([2]))
            writer.flush()

        messages = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(len(messages), 2)
        self.assertNotEqual(messages[0]['manifest'][0], messages[1]['manifest'][0])

    def test_schema_captured_from_first_batch(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            batch = _record_batch([1])
            writer.write(batch)
            writer.write(_record_batch([2]))
            self.assertEqual(writer._schema, batch.schema)
            writer.flush()

            messages = [json.loads(line) for line in out.getvalue().splitlines()]
            path = messages[0]['manifest'][0].removeprefix('file://')
            self.assertEqual(ipc.open_file(path).schema, batch.schema)

    def test_batch_file_name_pattern(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = self._make_writer(tmpdir, batch_size=10, output=out)
            writer.write(_record_batch([1]))
            writer.flush()

        msg = json.loads(out.getvalue())
        filename = os.path.basename(msg['manifest'][0].removeprefix('file://'))
        self.assertTrue(filename.startswith('tap-mysql-'))
        self.assertTrue(filename.endswith('.arrow'))


if __name__ == '__main__':
    unittest.main()
