# pylint: disable=missing-docstring,too-many-locals

import collections
import itertools
import re
from collections.abc import Iterable
from typing import List, Optional, Set

from singer import MetadataMapping, Schema, get_logger
from singer.catalog import Catalog, CatalogEntry, Metadata

from tap_mysql.connection import MySQLConnection, connect_with_backoff
from tap_mysql.sync_strategies import common

LOGGER = get_logger('tap_mysql')

Column = collections.namedtuple(
    'Column',
    [
        'table_schema',
        'table_name',
        'column_name',
        'data_type',
        'character_maximum_length',
        'numeric_precision',
        'numeric_scale',
        'column_type',
        'column_key',
    ],
)

STRING_TYPES = {'char', 'enum', 'tinytext', 'longtext', 'mediumtext', 'text', 'varchar'}

BYTES_FOR_INTEGER_TYPE = {'tinyint': 1, 'smallint': 2, 'mediumint': 3, 'int': 4, 'bigint': 8}

BOOL_TYPES = {'bit'}

JSON_TYPES = {'json'}

FLOAT_TYPES = {'float', 'double', 'decimal'}

DATETIME_TYPES = {'datetime', 'timestamp', 'time', 'date'}

BINARY_TYPES = {'binary', 'varbinary'}

SPATIAL_TYPES = {
    'geometry',
    'point',
    'linestring',
    'polygon',
    'multipoint',
    'multilinestring',
    'multipolygon',
    'geometrycollection',
}

# MySQL 8.0+ uses 'geomcollection' as the internal name for GEOMETRYCOLLECTION.
_DATA_TYPE_ALIASES = {'geomcollection': 'geometrycollection'}

_INTEGER_DISPLAY_WIDTH_RE = re.compile(r'^(tinyint|smallint|mediumint|int|bigint|year)\((\d+)\)(.*)')


def _normalize_data_type(data_type: str) -> str:
    """Normalize data_type aliases introduced by newer MySQL versions."""
    return _DATA_TYPE_ALIASES.get(data_type, data_type)


def _normalize_column_type(col: 'Column') -> str:
    """Return a normalized sql-datatype string for consistent cross-version output.

    - Normalizes geomcollection -> geometrycollection (MySQL 8.0+ internal rename)
    - Strips default integer/year display widths (MySQL 8.0.17+ removed them; MariaDB still reports them)
    - Preserves tinyint(1) unsigned as-is (canonical boolean+unsigned form)
    """
    column_type = col.column_type.lower()

    # Normalize geometry type aliases (e.g. geomcollection -> geometrycollection)
    if column_type in _DATA_TYPE_ALIASES:
        return _DATA_TYPE_ALIASES[column_type]

    m = _INTEGER_DISPLAY_WIDTH_RE.match(column_type)
    if m:
        base_type, width, modifiers = m.group(1), m.group(2), m.group(3).strip()
        # tinyint(1) unsigned is the canonical boolean+unsigned form - preserve it
        if base_type == 'tinyint' and width == '1' and modifiers == 'unsigned':
            return column_type
        return (base_type + (' ' + modifiers if modifiers else '')).strip()
    return column_type


# A set of all supported column types listed above
SUPPORTED_COLUMN_TYPES_AGGREGATED = (
    STRING_TYPES.union(FLOAT_TYPES)
    .union(DATETIME_TYPES)
    .union(BINARY_TYPES)
    .union(SPATIAL_TYPES)
    .union(BOOL_TYPES)
    .union(JSON_TYPES)
    .union(BYTES_FOR_INTEGER_TYPE.keys())
)


def is_supported_column_type(column_datatype: str | None) -> bool:
    """
    Checks if the given sql datatype is supported

    Args:
        column_datatype: Column sql data type from the catalog metadata

    Returns: True if column type is supported, False otherwise
    """
    return column_datatype in SUPPORTED_COLUMN_TYPES_AGGREGATED


def should_run_discovery(column_names: Set[str], md_map: MetadataMapping) -> bool:
    """
    Checks if we need to run discovery using a given metadata mapping.

    This function is helpful to refresh a stream schema when we detect a new column while syncing.

    The discovery will run if one of the following conditions are met:
        - one of the given columns is not in the given metadata, ie we know nothing about this column
        - the column is selected by default and its type is among the supported sql types.

    Args:
        column_names: A set of column names as strings
        md_map: a stream metadata as a map

    Returns: True if we should run discovery, False otherwise

    """
    LOGGER.debug('should_run_discovery with (%s)...', column_names)

    for column_name in column_names:
        md_properties = md_map.get(('properties', column_name))

        # this column doesn't exists in the metadata so we know nothing about it
        # so will have to run discovery
        if not md_properties:
            LOGGER.debug('Will run discovery because `%s` not in stream metadata', column_name)
            return True

        if md_properties.selected_by_default and is_supported_column_type(md_properties.datatype):
            LOGGER.debug('Will run discovery because `%s` is selected by default and of supported type', column_name)
            return True

    return False


def discover_catalog(mysql_conn: MySQLConnection, dbs: str | None = None, tables: Optional[str] = None) -> Catalog:
    """Returns a Catalog describing the structure of the database."""

    catalog = Catalog()

    if dbs:
        filter_dbs_clause = ','.join([f"'{db_name}'" for db_name in dbs.split(',')])
        table_schema_clause = f'WHERE table_schema IN ({filter_dbs_clause})'
    else:
        table_schema_clause = """
        WHERE table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'mysql',
        'sys'
        )"""

    tables_clause = ''

    if tables is not None and tables != '':
        filter_tables_clause = ','.join([f"'{table_name}'" for table_name in tables.split(',')])
        tables_clause = f' AND table_name IN ({filter_tables_clause})'

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute(f"""
            SELECT table_schema,
                   table_name,
                   table_type,
                   table_rows
                FROM information_schema.tables
                {table_schema_clause}{tables_clause}
            """)

            table_info = {}

            for db_name, table, table_type, rows in cur.fetchall():
                if db_name not in table_info:
                    table_info[db_name] = {}

                table_info[db_name][table] = {'row_count': rows, 'is_view': table_type == 'VIEW'}

            cur.execute(f"""
                SELECT table_schema,
                       table_name,
                       column_name,
                       data_type,
                       character_maximum_length,
                       numeric_precision,
                       numeric_scale,
                       column_type,
                       column_key
                    FROM information_schema.columns
                    {table_schema_clause}{tables_clause}
                    ORDER BY table_schema, table_name
            """)

            columns = []
            rec = cur.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = cur.fetchone()

            entries = []
            for k, cols in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
                cols = list(cols)
                (table_schema, table_name) = k

                schema = Schema(type='object', properties={c.column_name: schema_for_column(c)[0] for c in cols})
                mdata = create_column_metadata(cols)
                mdata.root.database_name = table_schema

                is_view = table_info[table_schema][table_name]['is_view']

                if table_schema in table_info and table_name in table_info[table_schema]:
                    row_count = table_info[table_schema][table_name].get('row_count')

                    if row_count is not None:
                        mdata.root.row_count = row_count

                    mdata.root.is_view = is_view

                def column_is_key_prop(c: Column, m: MetadataMapping):
                    return c.column_key == 'PRI' and m['properties', c.column_name].inclusion != 'unsupported'

                key_properties = [c.column_name for c in cols if column_is_key_prop(c, mdata)]

                if not is_view:
                    mdata.root.table_key_properties = key_properties

                entry = CatalogEntry(
                    table=table_name,
                    stream=table_name,
                    metadata=mdata,
                    tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                    schema=schema,
                )

                entries.append(entry)
                catalog.add_stream(entry)

    return catalog


def schema_for_column(column) -> tuple[Schema, Metadata.InclusionType]:  # pylint: disable=too-many-branches
    """Returns the Schema object for the given Column."""

    data_type = _normalize_data_type(column.data_type.lower())
    column_type = column.column_type.lower()

    inclusion = Metadata.InclusionType.AVAILABLE
    # We want to automatically include all primary key columns
    if column.column_key.lower() == 'pri':
        inclusion = Metadata.InclusionType.AUTOMATIC

    result = Schema()

    if data_type in BOOL_TYPES or column_type.startswith('tinyint(1)'):
        result.type = ['null', 'boolean']

    elif data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        if 'unsigned' in column_type:
            result.minimum = 0
            result.maximum = 2**bits - 1
        else:
            result.minimum = 0 - 2 ** (bits - 1)
            result.maximum = 2 ** (bits - 1) - 1

    elif data_type in FLOAT_TYPES:
        result.type = ['null', 'number']

        if data_type == 'decimal':
            result.multipleOf = 10 ** (0 - column.numeric_scale)

    elif data_type in JSON_TYPES:
        result.type = ['null', 'object']

    elif data_type in STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = column.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ['null', 'string']

        if data_type == 'time':
            result.format = 'time'
        else:
            result.format = 'date-time'

    elif data_type in BINARY_TYPES:
        result.type = ['null', 'string']
        result.format = 'binary'

    elif data_type in SPATIAL_TYPES:
        result.type = ['null', 'object']
        result.format = 'spatial'

    else:
        inclusion = Metadata.InclusionType.UNSUPPORTED
        result = Schema(None, description=f'Unsupported column type {column_type}')

    return result, inclusion


def create_column_metadata(cols: List[Column]) -> MetadataMapping:
    mdata = MetadataMapping()
    mdata.root.selected_by_default = False
    for col in cols:
        schema, inclusion = schema_for_column(col)
        mdata['properties', col.column_name].inclusion = inclusion
        mdata['properties', col.column_name].selected_by_default = inclusion != 'unsupported'
        mdata['properties', col.column_name].sql_datatype = _normalize_column_type(col)
        mdata['properties', col.column_name].datatype = _normalize_data_type(col.data_type.lower())

    return mdata


def resolve_catalog(discovered_catalog: Catalog, streams_to_sync: Iterable[CatalogEntry]):
    result = Catalog()

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        replication_key = catalog_entry.metadata.root.replication_key

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist', database_name, catalog_entry.table)
            continue

        input_properties = catalog_entry.schema.properties or {}
        discovered_properties = discovered_table.schema.properties or {}

        mask = catalog_entry.metadata.resolve_selection()
        selected = {k for k in input_properties if mask['properties', k] or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema, discovered_table.metadata)

        result.add_stream(
            CatalogEntry(
                tap_stream_id=catalog_entry.tap_stream_id,
                metadata=catalog_entry.metadata,
                stream=catalog_entry.tap_stream_id,
                table=catalog_entry.table,
                schema=Schema(
                    type='object',
                    properties={col: discovered_properties[col] for col in columns},
                ),
            )
        )

    return result


def desired_columns(selected: set[str], table_schema: Schema, table_metadata: MetadataMapping) -> Set:
    """
    Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    table_metadata - the most recently discovered metadata for the table, which
        carries each column's inclusion (available/automatic/unsupported);
        the schema's own properties only describe JSON-schema type information
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    props = table_schema.properties or {}
    for column in props:
        all_columns.add(column)
        inclusion = table_metadata['properties', column].inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception(f'Unknown inclusion {inclusion}')

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning('Columns %s were selected but are not supported. Skipping them.', selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning('Columns %s were selected but do not exist.', selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning('Columns %s are primary keys but were not selected. Adding them.', not_selected_but_automatic)

    return selected.intersection(available).union(automatic)
