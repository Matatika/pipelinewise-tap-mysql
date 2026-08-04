# pylint: disable=missing-function-docstring,too-many-arguments,too-many-branches
import codecs
import copy
import datetime
import json
import random
import re
import socket
from typing import Any, Dict, Optional, Set, Tuple, Union

import mysql.connector.errors
import pytz
import singer
import tzlocal
from plpygis import Geometry
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication.event import GtidEvent, MariadbGtidEvent, RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from singer import Schema, metadata, utils

from tap_mysql import connection, stream_utils
from tap_mysql.connection import MySQLConnection, connect_with_backoff, make_connection_wrapper
from tap_mysql.discover_utils import desired_columns, discover_catalog, should_run_discovery
from tap_mysql.stream_utils import FastRecordMessage, write_schema_message
from tap_mysql.sync_strategies import common

LOGGER = singer.get_logger('tap_mysql')

SDC_DELETED_AT = "_sdc_deleted_at"
UPDATE_BOOKMARK_PERIOD = 1000
BOOKMARK_KEYS = {'log_file', 'log_pos', 'version', 'gtid'}

MYSQL_TIMESTAMP_TYPES = {
    FIELD_TYPE.TIMESTAMP,
    FIELD_TYPE.TIMESTAMP2
}

# The placeholder name that mysql-replication gives to a column when the binlog event does not
# carry the column names of the table. See pymysqlreplication.row_event.RowsEvent.
UNKNOWN_COLUMN_PATTERN = re.compile(r'^UNKNOWN_COL\d+$')

# Binlog replication itself needs the binlog_row_image system variable, which MySQL added in 5.6.2.
MIN_SERVER_VERSION_MESSAGE = 'MySQL version must be at least 5.6.2 to use binlog replication.'

# Safe column mapping needs the binlog_row_metadata system variable, which carries the column names
# of a table in the binlog event itself. mysql-replication supports it from MySQL 8.0.14 and
# MariaDB 10.5.0 on. An older server cannot put the column names in the binlog at all, so the tap
# falls back to reading them from the table definition as it is now. See verify_binlog_config.
SAFE_COLUMN_MAPPING_VERSION_MESSAGE = ('MySQL version must be at least 8.0.14, or MariaDB at least '
                                       '10.5, for the tap to map row values to columns safely '
                                       'across a schema change.')

# What goes wrong when the binlog does not hold the column names of a table. Every warning about
# column mapping uses this one sentence, to describe the risk the same way each time.
COLUMN_MAPPING_RISK_MESSAGE = (
    'The tap reads the column names from the table definition as it is now instead, and stops at an '
    'event whose values it cannot place. It counts the columns to find those events, so it cannot '
    'detect a change that keeps the number of columns the same, such as moving a column. The values '
    'of an event that MySQL wrote before such a change go to the wrong columns.')


def add_automatic_properties(catalog_entry, columns):
    catalog_entry.schema.properties[SDC_DELETED_AT] = Schema(
        type=["null", "string"],
        format="date-time"
    )

    columns.append(SDC_DELETED_AT)

    return columns


def verify_binlog_config(mysql_conn) -> bool:
    """
    Check that the source server can stream binlog events that the tap is able to read.

    Returns: True if the server is too old to put the column names of a table in the binlog, so the
    tap has to read them from the table definition as it is now. False if the server puts the column
    names in the binlog, which is the safe way.
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT  @@binlog_format")
            binlog_format = cur.fetchone()[0]

            if binlog_format != 'ROW':
                raise Exception(f"Unable to replicate binlog stream because binlog_format is "
                                f"not set to 'ROW': {binlog_format}.")

            try:
                cur.execute("SELECT  @@binlog_row_image")
                binlog_row_image = cur.fetchone()[0]
            # A server that does not have the variable raises DatabaseError, not InternalError, so
            # catch the base class and go by the error number
            except mysql.connector.errors.Error as ex:
                if ex.errno == 1193:
                    raise Exception("Unable to replicate binlog stream because binlog_row_image "
                                    f"system variable does not exist. {MIN_SERVER_VERSION_MESSAGE}") from ex
                raise ex

            if binlog_row_image != 'FULL':
                raise Exception(f"Unable to replicate binlog stream because binlog_row_image is "
                                f"not set to 'FULL': {binlog_row_image}.")

            try:
                cur.execute("SELECT  @@binlog_row_metadata")
                binlog_row_metadata = cur.fetchone()[0]
            except mysql.connector.errors.Error as ex:
                if ex.errno != 1193:
                    raise ex

                # The server is too old to have the variable, so it cannot put the column names in
                # the binlog whatever the settings are. Keep replicating the way the tap did before
                # it read the names from the event, and say what the risk is.
                LOGGER.warning(
                    'This server does not have the binlog_row_metadata system variable, so the '
                    'binlog does not record which column a row value belongs to. %s %s Upgrade the '
                    'server and set binlog_row_metadata to FULL to remove this risk, or replicate '
                    'these streams with FULL_TABLE or INCREMENTAL, which do not read the binlog.',
                    COLUMN_MAPPING_RISK_MESSAGE, SAFE_COLUMN_MAPPING_VERSION_MESSAGE)

                return True

            # A binlog row event holds a positional row image only. MySQL writes the column names of
            # the table into the binlog itself just for 'FULL'. This server is able to write them,
            # so the fix is a one line change on the server. Warn instead of stopping the sync,
            # to keep the behaviour of the tap the same as it was for a server set up this way.
            if binlog_row_metadata != 'FULL':
                LOGGER.warning(
                    "binlog_row_metadata is set to '%s', not 'FULL', so the binlog does not record "
                    'which column a row value belongs to. %s Run '
                    '"SET GLOBAL binlog_row_metadata = FULL" on the source server, and put it in '
                    'the server configuration to keep it across a restart, to remove this risk.',
                    binlog_row_metadata, COLUMN_MAPPING_RISK_MESSAGE)

                return True

            return False


def verify_gtid_config(mysql_conn: MySQLConnection):
    """
    Checks if GTID is enabled, raises exception if it's not
    Args:
        mysql_conn: instance of MySQLConnection

    Returns: None if gtid is enabled
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("select @@gtid_mode;")
            binlog_format = cur.fetchone()[0]

            if binlog_format != 'ON':
                raise Exception('Unable to replicate binlog stream because GTID mode is not enabled.')


def fetch_current_log_file_and_pos(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        open_conn: MySQLConnection
        server_version = open_conn.get_server_info()

        with open_conn.cursor() as cur:
            if server_version < "8.4":
                cur.execute("SHOW MASTER STATUS")
            else:
                cur.execute("SHOW BINARY LOG STATUS")

            result = cur.fetchone()

            if result is None:
                raise Exception("MySQL binary logging is not enabled.")

            current_log_file, current_log_pos = result[0:2]

            return current_log_file, current_log_pos


def fetch_current_gtid_pos(
        mysql_conn: MySQLConnection,
        engine: str
) -> str:
    """
    Find the given server's current GTID position.

    The sever we're connected to can have a comma separated list of gtids (e.g from past server migrations),
    the right gtid is the one with the same server ID as the given server ID.

    Args:
        mysql_conn: Mysql connection instance
        engine: DB engine (mariadb/mysql)

    Returns: Gtid position if found, otherwise raises exception
    """

    if engine == connection.MARIADB_ENGINE:
        server = str(connection.fetch_server_id(mysql_conn))
    else:
        server = connection.fetch_server_uuid(mysql_conn)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:

            if engine != connection.MARIADB_ENGINE:
                cur.execute('select @@GLOBAL.gtid_executed;')
            else:
                cur.execute('select @@gtid_current_pos;')

            result = cur.fetchone()

            if result is None:
                raise Exception("GTID is not present on this server!")

            gtids = result[0]
            LOGGER.debug('Found GTID(s): %s in server %s', gtids, server)

            gtid_to_use = None

            for gtid in gtids.split(','):
                gtid = gtid.strip()

                if not gtid:
                    continue

                if engine != connection.MARIADB_ENGINE:
                    gtid_parts = gtid.split(':')

                    if len(gtid_parts) != 2:
                        continue

                    if gtid_parts[0] == server:
                        gtid_to_use = gtid
                else:
                    gtid_parts = gtid.split('-')

                    if len(gtid_parts) != 3:
                        continue

                    if gtid_parts[1] == server:
                        gtid_to_use = gtid

            if gtid_to_use:
                LOGGER.info('Using GTID %s for state bookmark', gtid_to_use)
                return gtid_to_use

    raise Exception(f'No suitable GTID was found for server {server}.')


def json_bytes_to_string(data):
    if isinstance(data, bytes):
        return data.decode()

    if isinstance(data, dict):
        return dict(map(json_bytes_to_string, data.items()))

    if isinstance(data, tuple):
        return tuple(map(json_bytes_to_string, data))

    if isinstance(data, list):
        return list(map(json_bytes_to_string, data))

    return data


# pylint: disable=too-many-locals
def row_to_singer_record(catalog_entry, version, db_column_map, row, time_extracted):
    row_to_persist = {}

    for column_name, val in row.items():
        property_type = catalog_entry.schema.properties[column_name].type
        property_format = catalog_entry.schema.properties[column_name].format
        db_column = db_column_map.get(column_name)
        db_column_type = db_column.type if db_column else None

        if isinstance(val, datetime.datetime):
            if db_column_type in MYSQL_TIMESTAMP_TYPES:
                # The mysql-replication library creates datetimes from TIMESTAMP columns using fromtimestamp which
                # will use the local timezone thus we must set tzinfo accordingly See:
                # https://github.com/noplay/python-mysql-replication/blob/master/pymysqlreplication/row_event.py#L143
                # -L145
                timezone = tzlocal.get_localzone()
                local_datetime = val.replace(tzinfo=timezone)
                utc_datetime = local_datetime.astimezone(pytz.UTC)
                row_to_persist[column_name] = utc_datetime.isoformat()
            else:
                row_to_persist[column_name] = val.isoformat() + '+00:00'

        elif isinstance(val, datetime.date):
            row_to_persist[column_name] = val.isoformat()

        elif isinstance(val, datetime.timedelta):
            if property_format == 'time':
                # this should convert time column into 'HH:MM:SS' formatted string
                row_to_persist[column_name] = str(val)
            else:
                timedelta_from_epoch = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc) + val
                row_to_persist[column_name] = timedelta_from_epoch.isoformat() + '+00:00'

        elif db_column_type == FIELD_TYPE.JSON:
            row_to_persist[column_name] = json.dumps(json_bytes_to_string(val))

        elif property_format == 'spatial':
            if val:
                srid = int.from_bytes(val[:4], byteorder='little')
                geom = Geometry(val[4:], srid=srid)
                row_to_persist[column_name] = json.dumps(geom.geojson)
            else:
                row_to_persist[column_name] = None

        elif property_format == 'binary' and isinstance(val, bytes):
            # Encode bytes as hex to match the FULL_TABLE path, which selects the column
            # as `hex(col)`. Pad to the column's declared max_length because
            # mysql-replication strips the trailing null bytes that MySQL uses to pad a
            # BINARY(N) value, while `hex(col)` keeps them.
            # Only `binary` and `varbinary` columns get format == 'binary', so a
            # character column never reaches this branch.
            hex_val = codecs.encode(val, 'hex').decode('utf-8')
            max_length = getattr(db_column, 'max_length', None)
            if max_length is not None:
                hex_val = hex_val.ljust(max_length * 2, '0')
            row_to_persist[column_name] = hex_val

        elif 'boolean' in property_type or property_type == 'boolean':
            if val is None:
                boolean_representation = None
            elif val in (0, b'\x00'):
                boolean_representation = False
            elif db_column_type == FIELD_TYPE.BIT:
                boolean_representation = int.from_bytes(val, 'big') != 0 \
                    if isinstance(val, bytes) else int(val) != 0
            else:
                boolean_representation = True
            row_to_persist[column_name] = boolean_representation

        elif isinstance(val, bytes):
            # mysql-replication can return the value of a character column as bytes,
            # depending on the column charset. The FULL_TABLE path gets a str from the
            # MySQL client for the same column, so decode here to keep both paths equal.
            row_to_persist[column_name] = val.decode('utf-8', errors='replace')

        else:
            row_to_persist[column_name] = val

    return FastRecordMessage(
        stream=catalog_entry.stream,
        record=row_to_persist,
        version=version,
        time_extracted_str=time_extracted)


def calculate_gtid_bookmark(
        mysql_conn: MySQLConnection,
        binlog_streams_map: Dict[str, Any],
        state: Dict,
        engine: str
) -> str:
    """
    Finds the earliest bookmarked gtid in the state
    Args:
        mysql_conn: instance of MySqlConnection
        binlog_streams_map: dictionary of selected streams
        state: state dict with bookmarks
        engine: the DB flavor mysql/mariadb

    Returns: Min Gtid
    """
    min_gtid = None
    min_seq_no = None

    for tap_stream_id, bookmark in state.get('bookmarks', {}).items():
        stream = binlog_streams_map.get(tap_stream_id)

        if not stream:
            continue

        gtid = bookmark.get('gtid')

        if gtid:
            if engine == connection.MARIADB_ENGINE:
                gtid_seq_no = int(gtid.split('-')[2])
            else:
                gtid_interval = gtid.split(':')[1]

                if '-' in gtid_interval:
                    gtid_seq_no = int(gtid_interval.split('-')[1])
                else:
                    gtid_seq_no = int(gtid_interval)

            if min_seq_no is None or gtid_seq_no < min_seq_no:
                min_seq_no = gtid_seq_no
                min_gtid = gtid

    if not min_gtid:

        # Mariadb has a handy sql function BINLOG_GTID_POS to infer the gtid position from given binlog coordinates so
        # we will use that, as for mysql, there is no such thing, the only available option is using the cli utility
        # mysqlbinlog which we deemed as not nice to use, and we don't wanna make it a system requirement of this tap,
        # hence, this functionality of inferring gtid is not implemented for it.

        if engine != connection.MARIADB_ENGINE:
            raise Exception("Couldn't find any gtid in state bookmarks to resume logical replication")

        LOGGER.info("Couldn't find a gtid in state, will try to infer one from binlog coordinates if they exist ..")
        log_file, log_pos = calculate_bookmark(mysql_conn, binlog_streams_map, state)

        if not (log_file and log_pos):
            raise Exception("No binlog coordinates in state to infer gtid position! Cannot resume logical replication")

        min_gtid = _find_gtid_by_binlog_coordinates(mysql_conn, log_file, log_pos)

        if not min_gtid:
            raise Exception("Couldn't infer any gtid from binlog coordinates to resume logical replication")

        LOGGER.info('The inferred GTID is "%s", it will be used to resume replication',
                    min_gtid)
    else:
        LOGGER.info('The earliest bookmarked GTID found in the state is "%s", and will be used to resume replication',
                    min_gtid)

    return min_gtid


def _find_gtid_by_binlog_coordinates(mysql_conn: MySQLConnection, log_file: str, log_pos: int) -> Optional[str]:
    """
    Finds the equivalent gtid position from the given binlog file and pos.
    This only works on MariaDB

    Args:
        mysql_conn: instance of MySQLConnection
        log_file: a binlog file
        log_pos: a position in the log file

    Returns: gtid position
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute(f"select BINLOG_GTID_POS('{log_file}', {log_pos});")
            gtids = cur.fetchone()[0]

    LOGGER.debug('BINLOG_GTID_POS returned gtids: %s', gtids)

    if not gtids:
        return None

    server_id = str(connection.fetch_server_id(mysql_conn))

    gtid_to_use = None
    for gtid in gtids.split(','):
        gtid_parts = gtid.split('-')

        if len(gtid_parts) != 3:
            continue

        if gtid_parts[1] == server_id:
            gtid_to_use = gtid

    return gtid_to_use


def get_min_log_pos_per_log_file(binlog_streams_map, state) -> Dict[str, Dict]:
    min_log_pos_per_file = {}

    for tap_stream_id, bookmark in state.get('bookmarks', {}).items():
        stream = binlog_streams_map.get(tap_stream_id)

        if not stream:
            continue

        log_file = bookmark.get('log_file')
        log_pos = bookmark.get('log_pos')

        if not min_log_pos_per_file.get(log_file):
            min_log_pos_per_file[log_file] = {
                'log_pos': log_pos,
                'streams': [tap_stream_id]
            }

        elif min_log_pos_per_file[log_file]['log_pos'] > log_pos:
            min_log_pos_per_file[log_file]['log_pos'] = log_pos
            min_log_pos_per_file[log_file]['streams'].append(tap_stream_id)

        else:
            min_log_pos_per_file[log_file]['streams'].append(tap_stream_id)

    return min_log_pos_per_file


def calculate_bookmark(mysql_conn, binlog_streams_map, state) -> Tuple[str, int]:
    min_log_pos_per_file = get_min_log_pos_per_log_file(binlog_streams_map, state)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SHOW BINARY LOGS")

            binary_logs = cur.fetchall()

            if binary_logs:
                server_logs_set = {log[0] for log in binary_logs}
                state_logs_set = set(min_log_pos_per_file.keys())
                expired_logs = state_logs_set.difference(server_logs_set)

                if expired_logs:
                    raise Exception('Unable to replicate binlog stream because the following binary log(s) no longer '
                                    f'exist: {", ".join(expired_logs)}')

                for log_file in sorted(server_logs_set):
                    if min_log_pos_per_file.get(log_file):
                        return log_file, min_log_pos_per_file[log_file]['log_pos']

            raise Exception("Unable to replicate binlog stream because no binary logs exist on the server.")


def update_bookmarks(
        state: Dict,
        binlog_streams_map: Dict,
        log_file: str,
        log_pos: int,
        gtid: Optional[str]) -> Dict:
    """
    Updates the state bookmarks with the given binlog file & position or GTID
    Args:
        state: state to update
        binlog_streams_map: dictionary of log based streams
        log_file: new binlog file
        log_pos: new binlog pos
        gtid: new gtid pos

    Returns: updated state
    """
    LOGGER.debug('Updating state bookmark to binlog file and pos and GTID: %s, %d, %s', log_file, log_pos, gtid)

    if log_file and not log_pos:
        raise ValueError("binlog_file is present but binlog_pos is null! Please provide a binlog position "
                         "to properly update the state")

    for tap_stream_id in binlog_streams_map.keys():
        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'log_file',
                                      log_file)

        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'log_pos',
                                      log_pos)

        # update gtid only if it's not null
        if gtid:
            state = singer.write_bookmark(state,
                                          tap_stream_id,
                                          'gtid',
                                          gtid)

    return state


def get_db_column_types(event):
    return {c.name: c for c in event.columns}


def handle_write_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        filtered_vals = {k: v for k, v in row['values'].items()
                         if k in columns}

        record_message = row_to_singer_record(catalog_entry,
                                              stream_version,
                                              db_column_types,
                                              filtered_vals,
                                              time_extracted)

        stream_utils.write_message(record_message)
        rows_saved += 1

    return rows_saved


def handle_update_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        filtered_vals = {k: v for k, v in row['after_values'].items()
                         if k in columns}

        record_message = row_to_singer_record(catalog_entry,
                                              stream_version,
                                              db_column_types,
                                              filtered_vals,
                                              time_extracted)

        stream_utils.write_message(record_message)

        rows_saved += 1

    return rows_saved


def handle_delete_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    event_ts = datetime.datetime.fromtimestamp(event.timestamp, tz=datetime.timezone.utc).isoformat()

    for row in event.rows:
        vals = row['values']
        vals[SDC_DELETED_AT] = event_ts

        filtered_vals = {k: v for k, v in vals.items()
                         if k in columns}

        record_message = row_to_singer_record(catalog_entry,
                                              stream_version,
                                              db_column_types,
                                              filtered_vals,
                                              time_extracted)

        stream_utils.write_message(record_message)

        rows_saved += 1

    return rows_saved


def generate_streams_map(binlog_streams):
    stream_map = {}

    for catalog_entry in binlog_streams:
        columns = add_automatic_properties(catalog_entry,
                                           list(catalog_entry.schema.properties.keys()))

        stream_map[catalog_entry.tap_stream_id] = {
            'catalog_entry': catalog_entry,
            'desired_columns': columns
        }

    return stream_map


def __get_diff_in_columns_list(
        binlog_event: Union[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        schema_properties: Set[str],
        ignore_columns: Optional[Set[str]] = None) -> Set[str]:
    """
    Compare event's columns to the schema properties and get the difference

    Args:
        binlog_event: Row type binlog event
        schema_properties: stream known and supported schema properties
        ignore_columns: an optional set of binlog event columns to ignore and not include in the diff

    Returns: Difference as a set of column names

    """

    if ignore_columns is None:
        ignore_columns = set()

    # if a column no longer exists, the event will have something like __dropped_col_XY__
    # to refer to this column, we don't want these columns to be included in the difference
    # we also will ignore any column using the given ignore_columns argument.
    binlog_columns_filtered = filter(
        lambda col_name, ignored_cols=ignore_columns:
        col_name is not None
        and not bool(re.match(r'__dropped_col_\d+__', col_name) or col_name in ignored_cols),
        [col.name for col in binlog_event.columns])

    return set(binlog_columns_filtered).difference(schema_properties)


def verify_event_column_names(
        binlog_event: Union[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        tap_stream_id: str,
        log_file: str,
        log_pos: int) -> None:
    """
    Make sure the event carries the column names of the table.

    mysql-replication takes the column names of a row event from the event itself, which MySQL
    writes only while binlog_row_metadata is 'FULL'. Where MySQL did not write them, the library
    falls back to the table definition as it is now, and it uses those names only while the table
    still has as many columns as the event holds. A count that does not match means the table gained
    or lost a column after MySQL wrote the event, so mapping the values by position would put them
    in the wrong columns. Either way the event has no usable names, and the tap must not emit it.

    Check this BEFORE reading the rows of the event. mysql-replication fills in the missing names by
    position while it reads them, which hides the mismatch and gives every value the wrong name.

    Args:
        binlog_event: Row type binlog event
        tap_stream_id: stream id, for the error message
        log_file: binlog file of the event, for the error message
        log_pos: binlog position of the event, for the error message

    Returns: None if the event carries all of its column names

    """
    unnamed_columns = [i for i, col in enumerate(binlog_event.columns)
                       if col.name is None or UNKNOWN_COLUMN_PATTERN.match(col.name)]

    if not unnamed_columns:
        return

    raise Exception(
        f"Stream `{tap_stream_id}`: the values of the binlog event at {log_file}:{log_pos} cannot be "
        f"mapped to columns safely, so the tap does not emit it ({len(unnamed_columns)} of "
        f"{len(binlog_event.columns)} columns have no name). Either MySQL wrote the event while "
        f"binlog_row_metadata was not 'FULL', or the table gained or lost a column after MySQL wrote "
        f"it. Set binlog_row_metadata to 'FULL' on the source server, then re-sync this stream with "
        f"FULL_TABLE to get past the events that MySQL has already written.")


# pylint: disable=R1702,R0915
def _run_binlog_sync(
        mysql_conn: MySQLConnection,
        reader: BinLogStreamReader,
        binlog_streams_map: Dict,
        state: Dict,
        config: Dict,
        end_log_file: str,
        end_log_pos: int):

    processed_rows_events = 0
    events_skipped = 0

    log_file = None
    log_pos = None
    gtid_pos = reader.auto_position  # initial gtid, we set this when we created the reader's instance

    # A set to hold all columns that are detected as we sync but should be ignored cuz they are unsupported types.
    # Saving them here to avoid doing the check if we should ignore a column over and over again
    ignored_columns = set()

    # Exit from the loop when the reader either runs out of streams to return or we reach
    # the end position (which is Master's)
    for binlog_event in reader:

        # get reader current binlog file and position
        log_file = reader.log_file
        log_pos = reader.log_pos

        # The iterator across python-mysql-replication's fetchone method should ultimately terminate
        # upon receiving an EOF packet. There seem to be some cases when a MySQL server will not send
        # one causing binlog replication to hang.
        if (log_file > end_log_file) or (end_log_file == log_file and log_pos >= end_log_pos):
            LOGGER.info('BinLog reader (file: %s, pos:%s) has reached or exceeded end position, exiting!',
                        log_file,
                        log_pos)

            # There are cases when a mass operation (inserts, updates, deletes) starts right after we get the Master
            # binlog file and position above, making the latter behind the stream reader and it causes some data loss
            # in the next run by skipping everything between end_log_file and log_pos
            # so we need to update log_pos back to master's position
            log_file = end_log_file
            log_pos = end_log_pos

            break

        if isinstance(binlog_event, RotateEvent):
            LOGGER.debug('RotateEvent: log_file=%s, log_pos=%d',
                         binlog_event.next_binlog,
                         binlog_event.position)

            state = update_bookmarks(state,
                                     binlog_streams_map,
                                     binlog_event.next_binlog,
                                     binlog_event.position,
                                     gtid_pos
                                     )

        elif isinstance(binlog_event, (MariadbGtidEvent, GtidEvent)):
            gtid_pos = binlog_event.gtid

            LOGGER.debug('%s: gtid=%s',
                         binlog_event.__class__.__name__,
                         gtid_pos)

            state = update_bookmarks(state,
                                     binlog_streams_map,
                                     log_file,
                                     log_pos,
                                     gtid_pos
                                     )

            # There is strange behavior happening when using GTID in the pymysqlreplication lib,
            # explained here: https://github.com/noplay/python-mysql-replication/issues/367
            # Fix: Updating the reader's auto-position to the newly encountered gtid means we won't have to restart
            # consuming binlog from old GTID pos when connection to server is lost.
            reader.auto_position = gtid_pos

        else:
            time_extracted = utils.strftime(utils.now())

            tap_stream_id = common.generate_tap_stream_id(binlog_event.schema, binlog_event.table)
            streams_map_entry = binlog_streams_map.get(tap_stream_id, {})
            catalog_entry = streams_map_entry.get('catalog_entry')
            columns = streams_map_entry.get('desired_columns')

            if not catalog_entry:
                events_skipped += 1

                if events_skipped % UPDATE_BOOKMARK_PERIOD == 0:
                    LOGGER.debug("Skipped %s events so far as they were not for selected tables; %s rows extracted",
                                 events_skipped,
                                 processed_rows_events)
            else:
                # Refuse to map an event that does not carry the column names of the table,
                # because every value would go to the wrong column or get dropped
                verify_event_column_names(binlog_event, tap_stream_id, log_file, log_pos)

                # Compare event's columns to the schema properties
                diff = __get_diff_in_columns_list(binlog_event,
                                                  catalog_entry.schema.properties.keys(),
                                                  ignored_columns)

                # If there are additional cols in the event then run discovery if needed and update the catalog
                if diff:

                    LOGGER.info('Stream `%s`: Difference detected between event and schema: %s', tap_stream_id, diff)

                    md_map = metadata.to_map(catalog_entry.metadata)

                    if not should_run_discovery(diff, md_map):
                        LOGGER.info('Stream `%s`: Not running discovery. Ignoring all detected columns in %s',
                                    tap_stream_id,
                                    diff)
                        ignored_columns = ignored_columns.union(diff)

                    else:
                        LOGGER.info('Stream `%s`: Running discovery ... ', tap_stream_id)

                        # run discovery for the current table only
                        new_catalog_entry = discover_catalog(
                            mysql_conn,
                            config.get('filter_dbs'),
                            catalog_entry.table,
                            json_as_type=config.get('json_as_type', 'object')).streams[0]

                        selected = {k for k, v in new_catalog_entry.schema.properties.items()
                                    if common.property_is_selected(new_catalog_entry, k)}

                        # the new catalog has "stream" property = table name, we need to update that to make it the
                        # same as the result of the "resolve_catalog" function
                        new_catalog_entry.stream = tap_stream_id

                        # These are the columns we need to select
                        new_columns = desired_columns(selected, new_catalog_entry.schema)

                        cols = set(new_catalog_entry.schema.properties.keys())

                        # drop unsupported properties from schema
                        for col in cols:
                            if col not in new_columns:
                                new_catalog_entry.schema.properties.pop(col, None)

                        # Add the _sdc_deleted_at col
                        new_columns = add_automatic_properties(new_catalog_entry, list(new_columns))

                        # send the new scheme to target if we have a new schema
                        if new_catalog_entry.schema.properties != catalog_entry.schema.properties:
                            write_schema_message(catalog_entry=new_catalog_entry)
                            catalog_entry = new_catalog_entry

                            # update this dictionary while we're at it
                            binlog_streams_map[tap_stream_id]['catalog_entry'] = new_catalog_entry
                            binlog_streams_map[tap_stream_id]['desired_columns'] = new_columns
                            columns = new_columns

                        # A column that discovery does not find at all no longer exists in the
                        # table, so MySQL wrote this event before a rename or a drop. The value
                        # has no column to go to, and the tap leaves it out of the record. Say so,
                        # because a dropped value is easy to mistake for an empty one.
                        vanished_columns = diff.difference(cols)

                        if vanished_columns:
                            LOGGER.warning(
                                'Stream `%s`: column(s) %s of the binlog event at %s:%s are not in the table '
                                'any more, so the tap leaves their values out of the record. MySQL wrote this '
                                'event before the column(s) were renamed or dropped. Re-sync this stream with '
                                'FULL_TABLE to bring those values across.',
                                tap_stream_id, sorted(vanished_columns), log_file, log_pos)

                        # Discovery cannot place any of these columns, so ignore them from here on.
                        # Without this, every later event that holds one of them runs discovery again.
                        unresolved_columns = diff.difference(new_catalog_entry.schema.properties)

                        if unresolved_columns:
                            ignored_columns = ignored_columns.union(unresolved_columns)

                if isinstance(binlog_event, WriteRowsEvent):
                    processed_rows_events = handle_write_rows_event(binlog_event,
                                                                    catalog_entry,
                                                                    state,
                                                                    columns,
                                                                    processed_rows_events,
                                                                    time_extracted)

                elif isinstance(binlog_event, UpdateRowsEvent):
                    processed_rows_events = handle_update_rows_event(binlog_event,
                                                                     catalog_entry,
                                                                     state,
                                                                     columns,
                                                                     processed_rows_events,
                                                                     time_extracted)

                elif isinstance(binlog_event, DeleteRowsEvent):
                    processed_rows_events = handle_delete_rows_event(binlog_event,
                                                                     catalog_entry,
                                                                     state,
                                                                     columns,
                                                                     processed_rows_events,
                                                                     time_extracted)
                else:
                    LOGGER.debug("Skipping event for table %s.%s as it is not an INSERT, UPDATE, or DELETE",
                                 binlog_event.schema,
                                 binlog_event.table)

        # Update singer bookmark and send STATE message periodically
        if ((processed_rows_events and processed_rows_events % UPDATE_BOOKMARK_PERIOD == 0) or
                (events_skipped and events_skipped % UPDATE_BOOKMARK_PERIOD == 0)):
            state = update_bookmarks(state,
                                     binlog_streams_map,
                                     log_file,
                                     log_pos,
                                     gtid_pos
                                     )
            stream_utils.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    LOGGER.info('Processed %s rows', processed_rows_events)

    # reader.log_pos is the START of the last returned event, not the end.
    # Filtered events (XID, GTID) after the last DML event advance the master's
    # position but are never surfaced to the loop, so the loop exits naturally
    # with log_pos < end_log_pos and the last DML event gets replayed next run.
    # Advance to end_log_pos whenever the loop exhausted the stream without break.
    if log_file and log_pos and log_file == end_log_file and log_pos < end_log_pos:
        log_pos = end_log_pos

    # Update singer bookmark at the last time to point it the last processed binlog event
    if log_file and log_pos:
        state = update_bookmarks(state,
                                 binlog_streams_map,
                                 log_file,
                                 log_pos,
                                 gtid_pos)


def create_binlog_stream_reader(
        config: Dict,
        log_file: Optional[str],
        log_pos: Optional[int],
        gtid_pos: Optional[str],
        legacy_column_mapping: bool = False
) -> BinLogStreamReader:
    """
    Create an instance of BinlogStreamReader with the right config

    Args:
        config: dictionary of the content of tap config.json
        log_file: binlog file name to start replication from (Optional if using gtid)
        log_pos: binlog pos to start replication from (Optional if using gtid)
        gtid_pos: GTID pos to start replication from (Optional if using log_file & pos)
        legacy_column_mapping: True if the server cannot put the column names in the binlog, so
            mysql-replication has to read them from the table definition as it is now

    Returns: Instance of BinlogStreamReader
    """
    if config.get('server_id'):
        server_id = int(config.get('server_id'))
        LOGGER.info("Using provided server_id=%s", server_id)
    else:
        server_id = random.randint(1, 2 ^ 32)  # generate random server id for this slave
        LOGGER.info("Using randomly generated server_id=%s", server_id)

    engine = config['engine']

    kwargs = {
        'connection_settings': {},
        'pymysql_wrapper': make_connection_wrapper(config),
        'is_mariadb': connection.MARIADB_ENGINE == engine,
        'server_id': server_id,  # slave server ID
        'report_slave': socket.gethostname() or 'pipelinewise',  # this is so this slave appears in SHOW SLAVE HOSTS;
        'only_events': [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
    }

    # only fetch events pertaining to the schemas in filter db.
    if config.get('filter_dbs'):
        kwargs['only_schemas'] = config['filter_dbs'].split(',')

    # Only ask mysql-replication to read the column names from the table definition when the server
    # is too old to put them in the binlog. Keep it switched off otherwise, because it maps the
    # values of an event by position and gets them wrong across a schema change.
    if legacy_column_mapping:
        kwargs['use_column_name_cache'] = True

    if config['use_gtid']:

        if not gtid_pos:
            raise ValueError(f'gtid_pos is empty "{gtid_pos}"! Cannot start logical replication from empty gtid.')

        LOGGER.info("Starting logical replication from GTID '%s' on engine '%s'", gtid_pos, engine)

        # When using GTID, we want to listen in for GTID events and start from given gtid pos
        kwargs['only_events'].extend([GtidEvent, MariadbGtidEvent])
        kwargs['auto_position'] = gtid_pos

    else:
        if not log_file or not log_pos or log_pos < 0:
            raise ValueError(f'log file or pos is empty ("{log_file}", "{log_pos}")! '
                             f'Cannot start logical replication from invalid log file/pos.')

        LOGGER.info("Starting logical replication from binlog file ['%s', %d]", log_file, log_pos)

        # When not using GTID, we want to listen in for rotate events, and start from given log position and file
        kwargs['only_events'].append(RotateEvent)
        kwargs['log_file'] = log_file
        kwargs['log_pos'] = log_pos
        kwargs['resume_stream'] = True

    return BinLogStreamReader(**kwargs)


def sync_binlog_stream(
        mysql_conn: MySQLConnection,
        config: Dict,
        binlog_streams_map: Dict[str, Any],
        state: Dict) -> None:
    """
    Capture the binlog events created between the pos in the state and current Master position and creates Singer
    streams to be flushed to stdout
    Args:
        mysql_conn: mysql connection instance
        config: tap config
        binlog_streams_map: tables to stream using binlog
        state: the current state
    """
    # Check the server on every run, not just on the run that takes the initial snapshot, so that a
    # server whose settings change later does not go unnoticed
    legacy_column_mapping = verify_binlog_config(mysql_conn)

    for tap_stream_id in binlog_streams_map:
        common.whitelist_bookmark_keys(BOOKMARK_KEYS, tap_stream_id, state)

    log_file = log_pos = gtid = None

    if config['use_gtid']:
        gtid = calculate_gtid_bookmark(mysql_conn, binlog_streams_map, state, config['engine'])
    else:
        log_file, log_pos = calculate_bookmark(mysql_conn, binlog_streams_map, state)

    reader = None

    try:
        reader = create_binlog_stream_reader(config, log_file, log_pos, gtid, legacy_column_mapping)

        end_log_file, end_log_pos = fetch_current_log_file_and_pos(mysql_conn)
        LOGGER.info('Current Master binlog file and pos: %s %s', end_log_file, end_log_pos)

        _run_binlog_sync(mysql_conn, reader, binlog_streams_map, state, config, end_log_file, end_log_pos)

    finally:
        # BinLogStreamReader doesn't implement the `with` methods
        # So, try/finally will close the chain from the top
        if reader:
            reader.close()

    stream_utils.write_message(singer.StateMessage(value=copy.deepcopy(state)))
