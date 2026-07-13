#!/usr/bin/env python3
# pylint: disable=missing-function-docstring

import datetime
import sys
import time

import mysql.connector.errors
import singer
from singer import metadata

from tap_mysql import stream_utils
from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import common

MAX_SYNC_RETRIES = 5

if sys.version_info < (3, 11):
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()

LOGGER = singer.get_logger('tap_mysql')

BOOKMARK_KEYS = {'replication_key', 'replication_key_value', 'version'}


def sync_table(mysql_conn, catalog_entry, state, columns, batch_config=None):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    replication_key_metadata = stream_metadata.get('replication-key')
    replication_key_state = singer.get_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key')

    replication_key_value = None

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value')
    else:
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'replication_key',
                                      replication_key_metadata)
        state = singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    stream_utils.write_message(activate_version_message)

    retryable_exceptions = (mysql.connector.errors.OperationalError,)
    if batch_config is not None and batch_config.format == 'arrow':
        from tap_mysql import adbc
        retryable_exceptions += adbc.get_retryable_exceptions()

    last_error = None
    for attempt in range(MAX_SYNC_RETRIES):
        if attempt > 0:
            replication_key_value = singer.get_bookmark(state,
                                                        catalog_entry.tap_stream_id,
                                                        'replication_key_value')
            wait = 2 ** attempt
            LOGGER.warning(
                "Lost connection to MySQL during sync of %s (attempt %d/%d), reconnecting in %ds: %s",
                catalog_entry.table, attempt, MAX_SYNC_RETRIES, wait, last_error,
            )
            time.sleep(wait)

        try:
            with connect_with_backoff(mysql_conn) as open_conn:
                with open_conn.cursor() as cur:
                    select_sql = common.generate_select_sql(catalog_entry, columns)
                    params = {}

                    if replication_key_value is not None:
                        if catalog_entry.schema.properties[replication_key_metadata].format == 'date-time' \
                                and isinstance(replication_key_value, str):
                            replication_key_value = datetime.datetime.fromisoformat(replication_key_value)

                        select_sql += f" WHERE `{replication_key_metadata}` >= %(replication_key_value)s " \
                                      f"ORDER BY `{replication_key_metadata}` ASC"

                        params['replication_key_value'] = replication_key_value
                    elif replication_key_metadata is not None:
                        select_sql += f' ORDER BY `{replication_key_metadata}` ASC'

                    common.sync_query(cur,
                                      catalog_entry,
                                      state,
                                      select_sql,
                                      columns,
                                      stream_version,
                                      params,
                                      batch_config=batch_config,
                                      mysql_conn=mysql_conn)
            break
        except retryable_exceptions as e:
            last_error = e
            if attempt == MAX_SYNC_RETRIES - 1:
                raise
