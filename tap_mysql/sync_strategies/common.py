#!/usr/bin/env python3
# pylint: disable=missing-function-docstring,too-many-arguments,too-many-locals
import copy
import dataclasses
import datetime
import gzip
import os
import sys
import time
import uuid
from typing import Optional

import orjson
import singer
from singer import metadata, metrics, utils

from tap_mysql import stream_utils
from tap_mysql.stream_utils import FastRecordMessage, get_key_properties

LOGGER = singer.get_logger('tap_mysql')


def escape(string):
    if '`' in string:
        raise Exception(f"Can't escape identifier {string} because it contains a backtick")
    return '`' + string + '`'


def generate_tap_stream_id(table_schema, table_name):
    return table_schema + '-' + table_name


def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), 'selected')

    return selected_md


def property_is_selected(stream, property_name):
    md_map = metadata.to_map(stream.metadata)
    return singer.should_sync_field(
        metadata.get(md_map, ('properties', property_name), 'inclusion'),
        metadata.get(md_map, ('properties', property_name), 'selected'),
        True)


def get_is_view(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('is-view')


def get_database_name(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('database-name')


def generate_select_sql(catalog_entry, columns):
    database_name = get_database_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = []

    for col_name in columns:
        # wrap the column name in "`"
        escaped_col = escape(col_name)

        # fetch the column type format from the json schema already built
        property_format = catalog_entry.schema.properties[col_name].format

        # if the column format is binary, fetch the values after removing any trailing
        # null bytes 0x00 and hexifying the column.
        if property_format == 'binary':
            escaped_columns.append(
                f'hex({escaped_col}) as {escaped_col}')
        elif property_format == 'spatial':
            escaped_columns.append(
                f'ST_AsGeoJSON({escaped_col}) as {escaped_col}')
        else:
            escaped_columns.append(escaped_col)

    select_sql = f'SELECT {",".join(escaped_columns)} FROM {escaped_db}.{escaped_table}'

    # escape percent signs
    select_sql = select_sql.replace('%', '%%')
    return select_sql


def row_to_singer_record(catalog_entry, version, row, columns, time_extracted_str):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = catalog_entry.schema.properties[columns[idx]].type
        property_format = catalog_entry.schema.properties[columns[idx]].format

        if isinstance(elem, datetime.datetime):
            row_to_persist += (elem.isoformat() + '+00:00',)

        elif isinstance(elem, datetime.date):
            row_to_persist += (elem.isoformat() + 'T00:00:00+00:00',)

        elif isinstance(elem, datetime.timedelta):
            if property_format == 'time':
                row_to_persist += (str(elem),) # this should convert time column into 'HH:MM:SS' formatted string
            else:
                epoch = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
                timedelta_from_epoch = epoch + elem
                row_to_persist += (timedelta_from_epoch.isoformat() + '+00:00',)

        elif 'boolean' in property_type or property_type == 'boolean':
            if elem is None:
                boolean_representation = None
            elif elem in (0, b'\x00'):
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)

        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return FastRecordMessage(
        stream=catalog_entry.stream,
        record=rec,
        version=version,
        time_extracted_str=time_extracted_str)


def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bookmark_key in [non_whitelisted_bookmark_key for
                         non_whitelisted_bookmark_key in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
                         if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bookmark_key)


FETCH_BATCH_SIZE = 1000


@dataclasses.dataclass(frozen=True)
class BatchConfig:
    """Validated configuration for Singer BATCH message mode.

    Constructing an instance validates that *batch_root_dir* exists.  A value
    of ``None`` from callers means BATCH mode is disabled — use
    ``BatchConfig.from_config`` to build one from a Singer config dict.
    """

    batch_size: int
    batch_root_dir: str = '.'

    def __post_init__(self):
        if self.batch_size <= 0:
            raise ValueError(f'batch_size must be a positive integer, got {self.batch_size}')
        if not os.path.isdir(self.batch_root_dir):
            raise ValueError(f'batch_root_dir does not exist or is not a directory: {self.batch_root_dir!r}')

    @classmethod
    def from_config(cls, config: dict) -> 'Optional[BatchConfig]':
        """Return a BatchConfig if batch_size_rows is set, else None (RECORD mode)."""
        raw = config.get('batch_size_rows') or None
        if raw is None:
            return None
        return cls(batch_size=int(raw), batch_root_dir=config.get('batch_root_dir', '.'))


class BatchWriter:
    """Writes Singer BATCH messages as streaming JSONL.gz files.

    Each call to `write()` appends one record to the current file.  When
    `batch_size` rows have been written the file is closed, a BATCH message is
    emitted to `output`, and the writer resets so the next `write()` starts a
    fresh file.  Call `flush()` at the end of a query to emit any partial batch.

    Passing `output` (any file-like with `.write`/`.flush`) makes the BATCH
    message output injectable for unit tests; it defaults to `sys.stdout`.
    """

    def __init__(self, stream_name: str, batch_config: BatchConfig, output=None):
        self.stream_name = stream_name
        self._batch_config = batch_config
        self._output = output if output is not None else sys.stdout
        self._path = None
        self._gz = None
        self._row_count = 0

    def write(self, record):
        """Append *record* (a dict) to the current batch file.

        Returns True if this write completed a full batch (i.e. a BATCH message
        was emitted and the writer has reset), False otherwise.
        """
        if self._gz is None:
            self._path = os.path.join(self._batch_config.batch_root_dir,
                                      f'tap-mysql-{uuid.uuid4().hex}.jsonl.gz')
            self._gz = gzip.open(self._path, 'wb')
        self._gz.write(orjson.dumps(record) + b'\n')
        self._row_count += 1
        if self._row_count >= self._batch_config.batch_size:
            self.flush()
            return True
        return False

    def flush(self):
        """Emit a BATCH message for any buffered rows, then reset.

        No-op when no rows have been written since the last flush.
        """
        if self._gz is None:
            return
        self._gz.close()
        assert self._path is not None
        size_bytes = os.path.getsize(self._path)
        LOGGER.info('Wrote batch file: %s (%d rows, %d bytes)',
                    self._path, self._row_count, size_bytes)
        msg = {
            'type': 'BATCH',
            'stream': self.stream_name,
            'encoding': {'format': 'jsonl', 'compression': 'gzip'},
            'manifest': [f'file://{self._path}'],
        }
        self._output.write(orjson.dumps(msg).decode() + '\n')
        self._output.flush()
        self._path = None
        self._gz = None
        self._row_count = 0


def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params,
               batch_config: 'Optional[BatchConfig]' = None):
    replication_key = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key')

    time_extracted = utils.now()
    time_extracted_str = utils.strftime(time_extracted)

    LOGGER.info('Running %s', select_sql)
    cursor.execute(select_sql, params)

    rows_saved = 0

    database_name = get_database_name(catalog_entry)

    md_map = metadata.to_map(catalog_entry.metadata)
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get('replication-method')
    key_properties = get_key_properties(catalog_entry) if replication_method in {'FULL_TABLE', 'LOG_BASED'} else []

    batch_writer = BatchWriter(catalog_entry.stream, batch_config) if batch_config else None

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = database_name
        counter.tags['table'] = catalog_entry.table

        while True:
            rows = cursor.fetchmany(FETCH_BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                counter.increment()
                rows_saved += 1
                record_message = row_to_singer_record(catalog_entry,
                                                      stream_version,
                                                      row,
                                                      columns,
                                                      time_extracted_str)

                # Write before updating bookmarks so that a mid-write exception
                # does not advance the bookmark past the last successfully emitted record.
                if batch_writer:
                    checkpoint = batch_writer.write(record_message.record)
                else:
                    stream_utils.write_message(record_message)
                    checkpoint = (rows_saved % 1000 == 0)

                if replication_method in {'FULL_TABLE', 'LOG_BASED'}:
                    max_pk_values = singer.get_bookmark(state,
                                                        catalog_entry.tap_stream_id,
                                                        'max_pk_values')

                    if max_pk_values:
                        last_pk_fetched = {k:v for k, v in record_message.record.items()
                                           if k in key_properties}

                        state = singer.write_bookmark(state,
                                                      catalog_entry.tap_stream_id,
                                                      'last_pk_fetched',
                                                      last_pk_fetched)

                elif replication_method == 'INCREMENTAL':
                    if replication_key is not None:
                        state = singer.write_bookmark(state,
                                                      catalog_entry.tap_stream_id,
                                                      'replication_key',
                                                      replication_key)

                        state = singer.write_bookmark(state,
                                                      catalog_entry.tap_stream_id,
                                                      'replication_key_value',
                                                      record_message.record[replication_key])

                if checkpoint:
                    stream_utils.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    if batch_writer:
        batch_writer.flush()

    stream_utils.write_message(singer.StateMessage(value=copy.deepcopy(state)))
