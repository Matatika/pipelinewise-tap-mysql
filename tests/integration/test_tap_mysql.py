import os
import re
from unittest.mock import patch

import mysql.connector.errors
import pytest
import singer

import tap_mysql
import tap_mysql.discover_utils
import tap_mysql.stream_utils
from tap_mysql.connection import MARIADB_ENGINE, MYSQL_ENGINE, MySQLConnection, connect_with_backoff

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils

import tap_mysql.sync_strategies.binlog as binlog
import tap_mysql.sync_strategies.common as common

SINGER_MESSAGES = []


def accumulate_singer_messages(message):
    SINGER_MESSAGES.append(message)


tap_mysql.stream_utils.write_message = accumulate_singer_messages


def _get_prop(entry: singer.CatalogEntry, name: str) -> singer.Schema:
    assert entry.schema.properties is not None
    return entry.schema.properties[name]


def _get_meta(entry: singer.CatalogEntry, name: str) -> singer.Metadata:
    return entry.metadata['properties', name]


class TestTypeMapping:
    @pytest.fixture(scope='class')
    @classmethod
    def entry(cls) -> singer.CatalogEntry:
        conn = test_utils.get_test_connection()

        with connect_with_backoff(conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE test_type_mapping (
                c_pk INTEGER PRIMARY KEY,
                c_decimal DECIMAL,
                c_decimal_2_unsigned DECIMAL(5, 2) UNSIGNED,
                c_decimal_2 DECIMAL(11, 2),
                c_tinyint TINYINT,
                c_tinyint_1 TINYINT(1),
                c_tinyint_1_unsigned TINYINT(1) UNSIGNED,
                c_smallint SMALLINT,
                c_tinytext TINYTEXT,
                c_mediumint MEDIUMINT,
                c_int INT,
                c_bigint BIGINT,
                c_bigint_unsigned BIGINT(20) UNSIGNED,
                c_float FLOAT,
                c_double DOUBLE,
                c_bit BIT(4),
                c_date DATE,
                c_time TIME,
                c_year YEAR,
                c_geometry GEOMETRY,
                c_point POINT,
                c_linestring LINESTRING,
                c_polygon POLYGON,
                c_multipoint MULTIPOINT,
                c_multilinestring MULTILINESTRING,
                c_multipolygon MULTIPOLYGON,
                c_geometrycollection GEOMETRYCOLLECTION,
                c_blob BLOB
                )""")

        catalog = tap_mysql.discover_catalog(conn)
        return next(s for s in catalog.streams if s.table == 'test_type_mapping')

    def test_decimal(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_decimal') == singer.Schema(type=['null', 'number'], multipleOf=1)
        assert _get_meta(entry, 'c_decimal') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='decimal(10,0)',
            datatype='decimal',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_decimal_unsigned(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_decimal_2_unsigned') == singer.Schema(type=['null', 'number'], multipleOf=0.01)
        assert _get_meta(entry, 'c_decimal_2_unsigned') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='decimal(5,2) unsigned',
            datatype='decimal',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_decimal_with_defined_scale_and_precision(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_decimal_2') == singer.Schema(type=['null', 'number'], multipleOf=0.01)
        assert _get_meta(entry, 'c_decimal_2') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='decimal(11,2)',
            datatype='decimal',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_tinyint(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_tinyint') == singer.Schema(type=['null', 'integer'], minimum=-128, maximum=127)
        assert _get_meta(entry, 'c_tinyint') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='tinyint',
            datatype='tinyint',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_tinyint_1(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_tinyint_1') == singer.Schema(type=['null', 'boolean'])
        assert _get_meta(entry, 'c_tinyint_1') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='tinyint',
            datatype='tinyint',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_tinyint_1_unsigned(self, entry: singer.CatalogEntry):
        engine = os.getenv('TAP_MYSQL_ENGINE', MYSQL_ENGINE)
        if engine == MYSQL_ENGINE:
            # MySQL 8.0.17+ strips the display width from TINYINT(1) UNSIGNED, making it
            # indistinguishable from TINYINT UNSIGNED - boolean detection is not possible.

            assert _get_prop(entry, 'c_tinyint_1_unsigned') == singer.Schema(
                type=['null', 'integer'], minimum=0, maximum=255
            )
            assert _get_meta(entry, 'c_tinyint_1_unsigned') == singer.Metadata(
                selected_by_default=True,
                sql_datatype='tinyint unsigned',
                datatype='tinyint',
                inclusion=singer.Metadata.InclusionType.AVAILABLE,
            )
        else:
            assert _get_prop(entry, 'c_tinyint_1_unsigned') == singer.Schema(type=['null', 'boolean'])
            assert _get_meta(entry, 'c_tinyint_1_unsigned') == singer.Metadata(
                selected_by_default=True,
                sql_datatype='tinyint(1) unsigned',
                datatype='tinyint',
                inclusion=singer.Metadata.InclusionType.AVAILABLE,
            )

    def test_smallint(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_smallint') == singer.Schema(type=['null', 'integer'], minimum=-32768, maximum=32767)
        assert _get_meta(entry, 'c_smallint') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='smallint',
            datatype='smallint',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_mediumint(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_mediumint') == singer.Schema(
            type=['null', 'integer'], minimum=-8388608, maximum=8388607
        )
        assert _get_meta(entry, 'c_mediumint') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='mediumint',
            datatype='mediumint',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_int(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_int') == singer.Schema(
            type=['null', 'integer'], minimum=-2147483648, maximum=2147483647
        )
        assert _get_meta(entry, 'c_int') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='int',
            datatype='int',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_bigint(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_bigint') == singer.Schema(
            type=['null', 'integer'], minimum=-9223372036854775808, maximum=9223372036854775807
        )
        assert _get_meta(entry, 'c_bigint') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='bigint',
            datatype='bigint',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_bigint_unsigned(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_bigint_unsigned') == singer.Schema(
            type=['null', 'integer'], minimum=0, maximum=18446744073709551615
        )

        assert _get_meta(entry, 'c_bigint_unsigned') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='bigint unsigned',
            datatype='bigint',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_float(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_float') == singer.Schema(type=['null', 'number'])
        assert _get_meta(entry, 'c_float') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='float',
            datatype='float',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_double(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_double') == singer.Schema(type=['null', 'number'])
        assert _get_meta(entry, 'c_double') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='double',
            datatype='double',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_bit(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_bit') == singer.Schema(type=['null', 'boolean'])
        assert _get_meta(entry, 'c_bit') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='bit(4)',
            datatype='bit',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_date(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_date') == singer.Schema(type=['null', 'string'], format='date-time')
        assert _get_meta(entry, 'c_date') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='date',
            datatype='date',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_time(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_time') == singer.Schema(type=['null', 'string'], format='time')
        assert _get_meta(entry, 'c_time') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='time',
            datatype='time',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_year(self, entry: singer.CatalogEntry):
        assert _get_meta(entry, 'c_year') == singer.Metadata(
            selected_by_default=False,
            sql_datatype='year',
            datatype='year',
            inclusion=singer.Metadata.InclusionType.UNSUPPORTED,
        )

    def test_pk(self, entry: singer.CatalogEntry):
        assert _get_meta(entry, 'c_pk').inclusion == singer.Metadata.InclusionType.AUTOMATIC

    def test_geometry(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_geometry') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_geometry') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='geometry',
            datatype='geometry',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_point(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_point') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_point') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='point',
            datatype='point',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_linestring(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_linestring') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_linestring') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='linestring',
            datatype='linestring',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_polygon(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_polygon') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_polygon') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='polygon',
            datatype='polygon',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_multipoint(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_multipoint') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_multipoint') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='multipoint',
            datatype='multipoint',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_multilinestring(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_multilinestring') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_multilinestring') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='multilinestring',
            datatype='multilinestring',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_multipolygon(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_multipolygon') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_multipolygon') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='multipolygon',
            datatype='multipolygon',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )

    def test_geometrycollection(self, entry: singer.CatalogEntry):
        assert _get_prop(entry, 'c_geometrycollection') == singer.Schema(type=['null', 'object'], format='spatial')
        assert _get_meta(entry, 'c_geometrycollection') == singer.Metadata(
            selected_by_default=True,
            sql_datatype='geometrycollection',
            datatype='geometrycollection',
            inclusion=singer.Metadata.InclusionType.AVAILABLE,
        )


class TestSelectsAppropriateColumns:
    def test_keeps_automatic_and_selected_available_columns(self):
        selected_cols = {'a', 'b', 'd'}
        table_schema = singer.Schema(
            type='object',
            properties={
                'a': singer.Schema(),
                'b': singer.Schema(),
                'c': singer.Schema(),
            },
        )
        table_metadata = singer.MetadataMapping()
        table_metadata['properties', 'a'].inclusion = singer.Metadata.InclusionType.AVAILABLE
        table_metadata['properties', 'b'].inclusion = singer.Metadata.InclusionType.UNSUPPORTED
        table_metadata['properties', 'c'].inclusion = singer.Metadata.InclusionType.AUTOMATIC

        got_cols = tap_mysql.discover_utils.desired_columns(selected_cols, table_schema, table_metadata)

        assert got_cols == {'a', 'c'}, 'Keep automatic as well as selected, available columns.'


class TestSchemaMessages:
    def test_new_columns_are_selected_by_default(self):
        conn = test_utils.get_test_connection()

        with connect_with_backoff(conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE tab (
                      id INTEGER PRIMARY KEY,
                      a INTEGER,
                      b INTEGER)
                """)

        catalog = test_utils.discover_catalog(conn)
        catalog.streams[0].stream = 'tab'
        catalog.streams[0].metadata = singer.MetadataMapping.from_iterable(
            [
                {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                {'breadcrumb': ('properties', 'a'), 'metadata': {'selected': True}},
            ]
        )

        test_utils.set_replication_method_and_key(catalog.streams[0], 'FULL_TABLE', None)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(conn, {}, catalog, {})

        schema_message = list(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))[0]
        assert isinstance(schema_message, singer.SchemaMessage)
        # tap-mysql selects new fields by default. If a field doesn't appear in the schema, then it should be
        # selected
        expectedKeys = ['id', 'a', 'b']

        assert schema_message.schema['properties'].keys() == set(expectedKeys)


def currently_syncing_seq(messages):
    return ''.join(
        [(m.value.get('currently_syncing', '_') or '_')[-1] for m in messages if isinstance(m, singer.StateMessage)]
    )


class TestCurrentStream:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE a (val int)')
                cursor.execute('CREATE TABLE b (val int)')
                cursor.execute('CREATE TABLE c (val int)')
                cursor.execute('INSERT INTO a (val) VALUES (1)')
                cursor.execute('INSERT INTO b (val) VALUES (1)')
                cursor.execute('INSERT INTO c (val) VALUES (1)')

        self.catalog = test_utils.discover_catalog(self.conn)

        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = singer.MetadataMapping.from_iterable(
                [
                    {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                    {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}},
                ]
            )

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

    def test_emit_currently_syncing(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)
        assert re.search('^a+b+c+_+', currently_syncing_seq(SINGER_MESSAGES))

    def test_start_at_currently_syncing(self):
        state = {
            'currently_syncing': 'tap_mysql_test-b',
            'bookmarks': {'tap_mysql_test-a': {'version': 123}, 'tap_mysql_test-b': {'version': 456}},
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        assert re.search('^b+c+a+_+', currently_syncing_seq(SINGER_MESSAGES))


def message_types_and_versions(messages):
    message_types = []
    versions = []
    for message in messages:
        if isinstance(message, singer.RecordMessage):
            message_types.append('RecordMessage')
            versions.append(message.version)
        elif isinstance(message, singer.ActivateVersionMessage):
            message_types.append('ActivateVersionMessage')
            versions.append(message.version)
    return message_types, versions


class TestStreamVersionFullTable:
    @pytest.fixture(autouse=True)
    def setup(self):
        original_get_stream_version = common.get_stream_version
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE full_table (val int)')
                cursor.execute('INSERT INTO full_table (val) VALUES (1)')

        self.catalog = test_utils.discover_catalog(self.conn)
        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = singer.MetadataMapping.from_iterable(
                [
                    {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                    {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}},
                ]
            )

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

        yield

        common.get_stream_version = original_get_stream_version

    def test_with_no_state(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        assert message_types == ['ActivateVersionMessage', 'RecordMessage', 'ActivateVersionMessage']
        assert isinstance(versions[0], int)
        assert versions[0] == versions[1]

    def test_with_no_initial_full_table_complete_in_state(self):
        common.get_stream_version = lambda a, b: 12345

        state = {'bookmarks': {'tap_mysql_test-full_table': {'version': None}}}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        assert message_types == ['RecordMessage', 'ActivateVersionMessage']
        assert versions == [12345, 12345]

        assert 'version' not in state['bookmarks']['tap_mysql_test-full_table'].keys()
        assert state['bookmarks']['tap_mysql_test-full_table']['initial_full_table_complete']

    def test_with_initial_full_table_complete_in_state(self):
        common.get_stream_version = lambda a, b: 12345

        state = {'bookmarks': {'tap_mysql_test-full_table': {'initial_full_table_complete': True}}}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        assert message_types == ['RecordMessage', 'ActivateVersionMessage']
        assert versions == [12345, 12345]

    def test_version_cleared_from_state_after_full_table_success(self):
        common.get_stream_version = lambda a, b: 12345

        state = {'bookmarks': {'tap_mysql_test-full_table': {'version': 1, 'initial_full_table_complete': True}}}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        assert message_types == ['RecordMessage', 'ActivateVersionMessage']
        assert versions == [12345, 12345]

        assert 'version' not in state['bookmarks']['tap_mysql_test-full_table'].keys()
        assert state['bookmarks']['tap_mysql_test-full_table']['initial_full_table_complete']


class TestIncrementalReplication:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE incremental (val int, updated datetime, ctime time)')
                cursor.execute("INSERT INTO incremental (val, updated, ctime) VALUES (1, '2017-06-01', current_time())")
                cursor.execute("INSERT INTO incremental (val, updated, ctime) VALUES (2, '2017-06-20', current_time())")
                cursor.execute("INSERT INTO incremental (val, updated, ctime) VALUES (3, '2017-09-22', current_time())")
                cursor.execute('CREATE TABLE integer_incremental (val int, updated int)')
                cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (1, 1)')
                cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (2, 2)')
                cursor.execute('INSERT INTO integer_incremental (val, updated) VALUES (3, 3)')

        self.catalog = test_utils.discover_catalog(self.conn)

        for stream in self.catalog.streams:
            stream.metadata = singer.MetadataMapping.from_iterable(
                [
                    {
                        'breadcrumb': (),
                        'metadata': {'selected': True, 'table-key-properties': [], 'database-name': 'tap_mysql_test'},
                    },
                    {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}},
                ]
            )

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'INCREMENTAL', 'updated')

    def test_with_no_state(self):
        state = {}

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        assert message_types == [
            'ActivateVersionMessage',
            'RecordMessage',
            'RecordMessage',
            'RecordMessage',
            'ActivateVersionMessage',
            'RecordMessage',
            'RecordMessage',
            'RecordMessage',
        ]
        assert isinstance(versions[0], int)
        assert versions[0] == versions[1]

    def test_with_state(self):
        state = {
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                    'replication_key_value': '2017-06-20',
                    'replication_key': 'updated',
                },
                'tap_mysql_test-integer_incremental': {
                    'version': 1,
                    'replication_key_value': 3,
                    'replication_key': 'updated',
                },
            }
        }

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        (message_types, versions) = message_types_and_versions(SINGER_MESSAGES)

        assert message_types == [
            'ActivateVersionMessage',
            'RecordMessage',
            'RecordMessage',
            'ActivateVersionMessage',
            'RecordMessage',
        ]
        assert isinstance(versions[0], int)
        assert versions[0] == versions[1]
        assert versions[1] == 1

    def test_change_replication_key(self):
        state = {
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                    'replication_key_value': '2017-06-20',
                    'replication_key': 'updated',
                }
            }
        }

        stream = [x for x in self.catalog.streams if x.stream == 'incremental'][0]

        stream.metadata = singer.MetadataMapping.from_iterable(
            [
                {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}},
                {'breadcrumb': ('properties', 'updated'), 'metadata': {'selected': True}},
            ]
        )

        test_utils.set_replication_method_and_key(stream, 'INCREMENTAL', 'val')

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        assert state['bookmarks']['tap_mysql_test-incremental']['replication_key'] == 'val'
        assert state['bookmarks']['tap_mysql_test-incremental']['replication_key_value'] == 3
        assert state['bookmarks']['tap_mysql_test-incremental']['version'] == 1

    def test_version_not_cleared_from_state_after_incremental_success(self):
        state = {
            'bookmarks': {
                'tap_mysql_test-incremental': {
                    'version': 1,
                    'replication_key_value': '2017-06-20',
                    'replication_key': 'updated',
                }
            }
        }

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        assert state['bookmarks']['tap_mysql_test-incremental']['version'] == 1


class TestBinlogReplication:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.state = {}
        self.conn = test_utils.get_test_connection()

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        log_file, log_pos = binlog.fetch_current_log_file_and_pos(self.conn)

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE binlog_1 (id int, updated datetime, created_date Date)')
                cursor.execute("""
                    CREATE TABLE binlog_2 (id int,
                    updated datetime,
                    is_good bool default False,
                    ctime time,
                    cjson json)
                """)
                cursor.execute(
                    "INSERT INTO binlog_1 (id, updated, created_date) VALUES (1, '2017-06-01', current_date())"
                )
                cursor.execute(
                    "INSERT INTO binlog_1 (id, updated, created_date) VALUES (2, '2017-06-20', current_date())"
                )
                cursor.execute(
                    "INSERT INTO binlog_1 (id, updated, created_date) VALUES (3, '2017-09-22', current_date())"
                )
                cursor.execute(
                    "INSERT INTO binlog_2 (id, updated, ctime, cjson) VALUES (1, '2017-10-22', "
                    'current_time(), \'[{"key1": "A", "key2": ["B", 2], "key3": {}}]\')'
                )
                cursor.execute(
                    "INSERT INTO binlog_2 (id, updated, ctime, cjson) VALUES (2, '2017-11-10', "
                    'current_time(), \'[{"key1": "A", "key2": ["B", 2], "key3": {}}]\')'
                )
                cursor.execute(
                    "INSERT INTO binlog_2 (id, updated, ctime, cjson) VALUES (3, '2017-12-10', "
                    'current_time(), \'[{"key1": "A", "key2": ["B", 2], "key3": {}}]\')'
                )
                cursor.execute("UPDATE binlog_1 set updated='2018-06-18' WHERE id = 3")
                cursor.execute("UPDATE binlog_2 set updated='2018-06-18' WHERE id = 2")
                cursor.execute('DELETE FROM binlog_1 WHERE id = 2')
                cursor.execute('DELETE FROM binlog_2 WHERE id = 1')

            open_conn.commit()

        self.catalog = test_utils.discover_catalog(self.conn)

        for stream in self.catalog.streams:
            stream.stream = stream.table

            stream.metadata = singer.MetadataMapping.from_iterable(
                [
                    {
                        'breadcrumb': (),
                        'metadata': {
                            'selected': True,
                            'database-name': 'tap_mysql_test',
                            'table-key-properties': ['id'],
                        },
                    },
                    {'breadcrumb': ('properties', 'id'), 'metadata': {'selected': True}},
                    {'breadcrumb': ('properties', 'updated'), 'metadata': {'selected': True}},
                ]
            )

            test_utils.set_replication_method_and_key(stream, 'LOG_BASED', None)

            self.state = singer.write_bookmark(self.state, stream.tap_stream_id, 'log_file', log_file)

            self.state = singer.write_bookmark(self.state, stream.tap_stream_id, 'log_pos', log_pos)

            self.state = singer.write_bookmark(self.state, stream.tap_stream_id, 'version', singer.utils.now())

        yield

        SINGER_MESSAGES.clear()

    def test_initial_full_table(self):
        state = {}

        global SINGER_MESSAGES

        tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        def _norm(m):
            return singer.RecordMessage if isinstance(m, singer.RecordMessage) else type(m)

        message_types = [_norm(m) for m in SINGER_MESSAGES]

        assert message_types == [
            singer.StateMessage,
            singer.SchemaMessage,
            singer.ActivateVersionMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.StateMessage,
            singer.ActivateVersionMessage,
            singer.StateMessage,
            singer.SchemaMessage,
            singer.ActivateVersionMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.StateMessage,
            singer.ActivateVersionMessage,
            singer.StateMessage,
        ]

        activate_version_message_1 = list(
            filter(
                lambda m: isinstance(m, singer.ActivateVersionMessage) and m.stream == 'tap_mysql_test-binlog_1',
                SINGER_MESSAGES,
            )
        )[0]

        activate_version_message_2 = list(
            filter(
                lambda m: isinstance(m, singer.ActivateVersionMessage) and m.stream == 'tap_mysql_test-binlog_2',
                SINGER_MESSAGES,
            )
        )[0]

        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'log_file') is not None
        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'log_pos') is not None
        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'gtid') is None

        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'log_file') is not None
        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'log_pos') is not None
        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'gtid') is None

        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_1', 'version') == activate_version_message_1.version

        assert singer.get_bookmark(state, 'tap_mysql_test-binlog_2', 'version') == activate_version_message_2.version

    def test_fail_on_view(self):
        for stream in self.catalog.streams:
            stream.metadata.root.is_view = True

        state = {}

        expected_exception_message = (
            'Unable to replicate stream(tap_mysql_test-{}) with binlog because it is a view.'.format(
                self.catalog.streams[0].stream
            )
        )

        with pytest.raises(Exception) as exc_info:
            tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        assert expected_exception_message == str(exc_info.value)

    def test_fail_if_log_file_does_not_exist(self):
        log_file = 'chicken'
        stream = self.catalog.streams[0]
        state = {
            'bookmarks': {stream.tap_stream_id: {'version': singer.utils.now(), 'log_file': log_file, 'log_pos': 1}}
        }

        expected_exception_message = (
            'Unable to replicate binlog stream because the following binary log(s) no longer exist: {}'.format(log_file)
        )

        with pytest.raises(Exception) as exc_info:
            tap_mysql.do_sync(self.conn, {}, self.catalog, state)

        assert expected_exception_message == str(exc_info.value)

    def test_binlog_stream(self):
        global SINGER_MESSAGES

        config = test_utils.get_db_config()
        config['server_id'] = '100'

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        schema_messages = list(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))

        for schema_msg in schema_messages:
            for prop, val in schema_msg.schema['properties'].items():
                assert 'type' in val, f'property "{prop}" has no "type" in stream "{schema_msg.stream}"'

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        def _norm(m):
            return singer.RecordMessage if isinstance(m, singer.RecordMessage) else type(m)

        message_types = [_norm(m) for m in SINGER_MESSAGES]
        assert message_types == [
            singer.StateMessage,
            singer.SchemaMessage,
            singer.SchemaMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.StateMessage,
        ]

        assert [
            ('tap_mysql_test-binlog_1', 1, '2017-06-01T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 3, '2017-09-22T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 2, '2017-11-10T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 3, '2017-12-10T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 3, '2018-06-18T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 2, '2018-06-18T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', True),
            ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', True),
        ] == [
            (m.stream, m.record['id'], m.record['updated'], m.record.get(binlog.SDC_DELETED_AT) is not None)
            for m in record_messages
        ]

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos') is not None

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos') is not None

    def test_binlog_stream_with_alteration(self):
        global SINGER_MESSAGES

        config = test_utils.get_db_config()
        config['server_id'] = '100'

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('ALTER TABLE binlog_1 add column data blob;')
                cursor.execute('ALTER TABLE binlog_1 add column is_cancelled boolean;')
                cursor.execute(
                    "INSERT INTO binlog_1 (id, updated, is_cancelled, data) VALUES (2, '2017-06-20', true, 'blob content')"  # noqa: E501
                )
                cursor.execute("INSERT INTO binlog_1 (id, updated, is_cancelled) VALUES (3, '2017-09-21', false)")
                cursor.execute("INSERT INTO binlog_2 (id, updated) VALUES (3, '2017-12-10')")
                cursor.execute('ALTER TABLE binlog_1 change column updated date_updated datetime;')
                cursor.execute("UPDATE binlog_1 set date_updated='2018-06-18' WHERE id = 3")

            open_conn.commit()

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        schema_messages = list(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))

        for schema_msg in schema_messages:
            for prop, val in schema_msg.schema['properties'].items():
                assert 'type' in val, f'property "{prop}" has no "type" in stream "{schema_msg.stream}"'

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        def _norm(m):
            return singer.RecordMessage if isinstance(m, singer.RecordMessage) else type(m)

        message_types = [_norm(m) for m in SINGER_MESSAGES]
        assert message_types == [
            singer.StateMessage,
            singer.SchemaMessage,
            singer.SchemaMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.StateMessage,  # end of 1st sync
            singer.StateMessage,  # start of 2nd sync
            singer.SchemaMessage,
            singer.SchemaMessage,
            singer.SchemaMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.StateMessage,
        ]

        assert [
            ('tap_mysql_test-binlog_1', 1, '2017-06-01T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 3, '2017-09-22T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 2, '2017-11-10T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 3, '2017-12-10T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 3, '2018-06-18T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_2', 2, '2018-06-18T00:00:00+00:00', False),
            ('tap_mysql_test-binlog_1', 2, '2017-06-20T00:00:00+00:00', True),
            ('tap_mysql_test-binlog_2', 1, '2017-10-22T00:00:00+00:00', True),
        ] == [
            (m.stream, m.record['id'], m.record['updated'], m.record.get(binlog.SDC_DELETED_AT) is not None)
            for m in record_messages[:10]
        ]

        assert 'tap_mysql_test-binlog_1' in SINGER_MESSAGES[15].stream
        assert 'date_updated' not in SINGER_MESSAGES[15].schema['properties']
        assert 'is_cancelled' not in SINGER_MESSAGES[15].schema['properties']

        assert 'tap_mysql_test-binlog_1' in SINGER_MESSAGES[17].stream
        assert 'date_updated' in SINGER_MESSAGES[17].schema['properties']
        assert 'is_cancelled' in SINGER_MESSAGES[17].schema['properties']

        assert 'tap_mysql_test-binlog_1' in SINGER_MESSAGES[18].stream
        assert 'date_updated' in SINGER_MESSAGES[18].record
        assert 'is_cancelled' in SINGER_MESSAGES[18].record

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos') is not None

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos') is not None

    def test_binlog_stream_with_gtid(self):
        global SINGER_MESSAGES

        engine = os.getenv('TAP_MYSQL_ENGINE', MYSQL_ENGINE)
        gtid = binlog.fetch_current_gtid_pos(self.conn, os.environ['TAP_MYSQL_ENGINE'])

        config = test_utils.get_db_config()
        config['use_gtid'] = True
        config['engine'] = engine

        self.state = singer.write_bookmark(self.state, 'tap_mysql_test-binlog_1', 'gtid', gtid)

        self.state = singer.write_bookmark(self.state, 'tap_mysql_test-binlog_2', 'gtid', gtid)

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute("INSERT INTO binlog_1 (id, updated) VALUES (4, '2022-06-20')")
                cursor.execute("INSERT INTO binlog_1 (id, updated) VALUES (5, '2022-09-21')")
                cursor.execute("INSERT INTO binlog_2 (id, updated) VALUES (4, '2017-12-10')")
                cursor.execute('delete from binlog_1 WHERE id = 3')

            open_conn.commit()

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        def _norm(m):
            return singer.RecordMessage if isinstance(m, singer.RecordMessage) else type(m)

        message_types = [_norm(m) for m in SINGER_MESSAGES]
        assert message_types == [
            singer.StateMessage,
            singer.SchemaMessage,
            singer.SchemaMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.StateMessage,
        ]

        assert [
            ('tap_mysql_test-binlog_1', 4, False),
            ('tap_mysql_test-binlog_1', 5, False),
            ('tap_mysql_test-binlog_2', 4, False),
            ('tap_mysql_test-binlog_1', 3, True),
        ] == [(m.stream, m.record['id'], m.record.get(binlog.SDC_DELETED_AT) is not None) for m in record_messages]

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'gtid') is not None

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'gtid') is not None

    def test_binlog_stream_switching_from_binlog_to_gtid_with_mysql_fails(self):
        engine = os.getenv('TAP_MYSQL_ENGINE', MYSQL_ENGINE)

        if engine != MYSQL_ENGINE:
            pytest.skip('This test is only meant for Mysql flavor')

        log_file, log_pos = binlog.fetch_current_log_file_and_pos(self.conn)

        self.state = singer.write_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file', log_file)

        self.state = singer.write_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos', log_pos)

        config = test_utils.get_db_config()

        config['use_gtid'] = True
        config['engine'] = engine

        with pytest.raises(Exception) as exc_info:
            tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        assert "Couldn't find any gtid in state bookmarks to resume logical replication" == str(exc_info.value)

    def test_binlog_stream_switching_from_binlog_to_gtid_with_mariadb_success(self):
        engine = os.getenv('TAP_MYSQL_ENGINE', MYSQL_ENGINE)

        if engine != MARIADB_ENGINE:
            pytest.skip('This test is only meant for Mariadb flavor')

        config = test_utils.get_db_config()

        config['use_gtid'] = True
        config['engine'] = engine

        tap_mysql.do_sync(self.conn, config, self.catalog, self.state)

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        def _norm(m):
            return singer.RecordMessage if isinstance(m, singer.RecordMessage) else type(m)

        message_types = [_norm(m) for m in SINGER_MESSAGES]
        assert message_types == [
            singer.StateMessage,
            singer.SchemaMessage,
            singer.SchemaMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.RecordMessage,
            singer.StateMessage,
        ]

        assert [
            ('tap_mysql_test-binlog_1', 1, False),
            ('tap_mysql_test-binlog_1', 2, False),
            ('tap_mysql_test-binlog_1', 3, False),
            ('tap_mysql_test-binlog_2', 1, False),
            ('tap_mysql_test-binlog_2', 2, False),
            ('tap_mysql_test-binlog_2', 3, False),
            ('tap_mysql_test-binlog_1', 3, False),
            ('tap_mysql_test-binlog_2', 2, False),
            ('tap_mysql_test-binlog_1', 2, True),
            ('tap_mysql_test-binlog_2', 1, True),
        ] == [(m.stream, m.record['id'], m.record.get(binlog.SDC_DELETED_AT) is not None) for m in record_messages]

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'log_pos') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_1', 'gtid') is not None

        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_file') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'log_pos') is not None
        assert singer.get_bookmark(self.state, 'tap_mysql_test-binlog_2', 'gtid') is not None


class TestViews:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE a_table (
                      id int primary key,
                      a int,
                      b int)
                    """
                )

                cursor.execute(
                    """
                    CREATE VIEW a_view AS SELECT id, a FROM a_table
                    """
                )

    def test_discovery_sets_is_view(self):
        catalog = test_utils.discover_catalog(self.conn)
        is_view = {}

        for stream in catalog.streams:
            is_view[stream.table] = stream.metadata.root.is_view

        assert is_view == {'a_table': False, 'a_view': True}

    def test_do_not_discover_key_properties_for_view(self):
        catalog = test_utils.discover_catalog(self.conn)
        primary_keys = {}
        for c in catalog.streams:
            primary_keys[c.table] = c.metadata.root.table_key_properties

        assert primary_keys == {'a_table': ['id'], 'a_view': None}


class TestEscaping:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE a (`b c` int)')
                cursor.execute('INSERT INTO a (`b c`) VALUES (1)')

        self.catalog = test_utils.discover_catalog(self.conn)

        self.catalog.streams[0].stream = 'some_stream_name'

        self.catalog.streams[0].metadata = singer.MetadataMapping.from_iterable(
            [
                {
                    'breadcrumb': (),
                    'metadata': {'selected': True, 'table-key-properties': [], 'database-name': 'tap_mysql_test'},
                },
                {'breadcrumb': ('properties', 'b c'), 'metadata': {'selected': True}},
            ]
        )

        test_utils.set_replication_method_and_key(self.catalog.streams[0], 'FULL_TABLE', None)

    def test_columns_with_spaces_are_escaped(self):
        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        record_message = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))[0]

        assert isinstance(record_message, singer.RecordMessage)
        assert record_message.record == {'b c': 1}


class TestJsonTables:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE json_tab (val json)')
                cursor.execute('INSERT INTO json_tab (val) VALUES ( \'{"a": 10, "b": "c"}\')')

        self.catalog = test_utils.discover_catalog(self.conn)
        for stream in self.catalog.streams:
            stream.key_properties = []

            stream.metadata = singer.MetadataMapping.from_iterable(
                [
                    {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'tap_mysql_test'}},
                    {'breadcrumb': ('properties', 'val'), 'metadata': {'selected': True}},
                ]
            )

            stream.stream = stream.table
            test_utils.set_replication_method_and_key(stream, 'FULL_TABLE', None)

    def test_json_column_is_synced_as_string(self):
        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()
        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        record_message = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))[0]
        assert isinstance(record_message, singer.RecordMessage)
        assert record_message.record == {'val': '{"a": 10, "b": "c"}'}


class TestSupportedPK:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute(
                    # BINARY is presently supported
                    'CREATE TABLE good_pk_tab (good_pk BINARY(10), age INT, PRIMARY KEY (good_pk))'
                )

                cursor.execute(
                    'INSERT INTO good_pk_tab (good_pk, age) VALUES '
                    "(BINARY('a'), 20), "
                    "(BINARY('b'), 30), "
                    "(BINARY('c'), 30), "
                    "(BINARY('d'), 40)"
                )

        self.catalog = test_utils.discover_catalog(self.conn)

        yield

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('DROP TABLE good_pk_tab;')

    def test_primary_key_is_in_metadata(self):
        primary_keys = {}
        for c in self.catalog.streams:
            primary_keys[c.table] = c.metadata.root.table_key_properties

        assert primary_keys == {'good_pk_tab': ['good_pk']}

    def test_sync_messages_are_correct(self):
        self.catalog.streams[0] = test_utils.set_replication_method_and_key(self.catalog.streams[0], 'LOG_BASED', None)
        self.catalog.streams[0] = test_utils.set_selected(self.catalog.streams[0], True)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        # inital sync
        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        # get schema message to test that it has all the table's columns
        schema_message = next(filter(lambda m: isinstance(m, singer.SchemaMessage), SINGER_MESSAGES))
        expectedKeys = ['good_pk', 'age']

        assert schema_message.schema['properties'].keys() == set(expectedKeys)

        # get the records, these are generated by Full table replication
        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        assert len(record_messages) == 4
        assert [rec.record for rec in record_messages] == [
            {'age': 20, 'good_pk': '61000000000000000000'},
            {'age': 30, 'good_pk': '62000000000000000000'},
            {'age': 30, 'good_pk': '63000000000000000000'},
            {'age': 40, 'good_pk': '64000000000000000000'},
        ]

        # get the last state message to be fed to the next sync
        state_message = list(filter(lambda m: isinstance(m, singer.StateMessage), SINGER_MESSAGES))[-1]

        SINGER_MESSAGES.clear()

        # run some queries
        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('UPDATE good_pk_tab set age=age+5')
                cursor.execute("INSERT INTO good_pk_tab (good_pk, age) VALUES (BINARY('e'), 16), (BINARY('f'), 5)")

        # do a sync and give the state so that binlog replication start from the last synced position
        tap_mysql.do_sync(self.conn, test_utils.get_db_config(), self.catalog, state_message.value)

        # get the changed/new records
        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        assert len(record_messages) == 6
        assert [rec.record for rec in record_messages] == [
            {'age': 25, 'good_pk': '61000000000000000000'},
            {'age': 35, 'good_pk': '62000000000000000000'},
            {'age': 35, 'good_pk': '63000000000000000000'},
            {'age': 45, 'good_pk': '64000000000000000000'},
            {'age': 16, 'good_pk': '65000000000000000000'},
            {'age': 5, 'good_pk': '66000000000000000000'},
        ]


class MySQLConnectionMock(MySQLConnection):
    """
    Mocked MySQLConnection class
    """

    def __init__(self, config):
        super().__init__(config)

        self.executed_queries = []

    def run_sql(self, sql):
        self.executed_queries.append(sql)


class TestSessionSqls:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.executed_queries = []

    def run_sql_mock(self, connection, sql):
        if sql.startswith('INVALID-SQL'):
            raise mysql.connector.errors.InternalError

        self.executed_queries.append(sql)

    def test_open_connections_with_default_session_sqls(self):
        """Default session parameters should be applied if no custom session SQLs"""
        with patch('tap_mysql.connection.MySQLConnection.connect'):
            with patch('tap_mysql.connection.run_sql') as run_sql_mock:
                run_sql_mock.side_effect = self.run_sql_mock
                conn = MySQLConnectionMock(config=test_utils.get_db_config())
                connect_with_backoff(conn)

        # Test if session variables applied on connection
        assert self.executed_queries == tap_mysql.connection.DEFAULT_SESSION_SQLS

    def test_open_connections_with_session_sqls(self):
        """Custom session parameters should be applied if defined"""
        session_sqls = ['SET SESSION max_statement_time=0', 'SET SESSION wait_timeout=28800']

        with patch('tap_mysql.connection.MySQLConnection.connect'):
            with patch('tap_mysql.connection.run_sql') as run_sql_mock:
                run_sql_mock.side_effect = self.run_sql_mock
                conn = MySQLConnectionMock(config={**test_utils.get_db_config(), **{'session_sqls': session_sqls}})
                connect_with_backoff(conn)

        # Test if session variables applied on connection
        assert self.executed_queries == session_sqls

    def test_open_connections_with_invalid_session_sqls(self):
        """Invalid SQLs in session_sqls should be ignored"""
        session_sqls = [
            'SET SESSION max_statement_time=0',
            'INVALID-SQL-SHOULD-BE-SILENTLY-IGNORED',
            'SET SESSION wait_timeout=28800',
        ]

        with patch('tap_mysql.connection.MySQLConnection.connect'):
            with patch('tap_mysql.connection.run_sql') as run_sql_mock:
                run_sql_mock.side_effect = self.run_sql_mock
                conn = MySQLConnectionMock(config={**test_utils.get_db_config(), **{'session_sqls': session_sqls}})
                connect_with_backoff(conn)

        # Test if session variables applied on connection
        assert self.executed_queries == ['SET SESSION max_statement_time=0', 'SET SESSION wait_timeout=28800']


class TestBitBooleanMapping:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.conn = test_utils.get_test_connection()

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('CREATE TABLE bit_booleans_table(`id` int, `c_bit` BIT(4))')
                cursor.execute(
                    "INSERT INTO bit_booleans_table(`id`,`c_bit`) VALUES (1, b'0000'),(2, NULL),(3, b'0010')"
                )

        self.catalog = test_utils.discover_catalog(self.conn)

        yield

        with connect_with_backoff(self.conn) as open_conn:
            with open_conn.cursor() as cursor:
                cursor.execute('DROP TABLE bit_booleans_table;')

    def test_sync_messages_are_correct(self):
        self.catalog.streams[0] = test_utils.set_replication_method_and_key(self.catalog.streams[0], 'FULL_TABLE', None)
        self.catalog.streams[0] = test_utils.set_selected(self.catalog.streams[0], True)

        global SINGER_MESSAGES
        SINGER_MESSAGES.clear()

        tap_mysql.do_sync(self.conn, {}, self.catalog, {})

        record_messages = list(filter(lambda m: isinstance(m, singer.RecordMessage), SINGER_MESSAGES))

        assert len(record_messages) == 3
        assert [rec.record for rec in record_messages] == [
            {'id': 1, 'c_bit': False},
            {'id': 2, 'c_bit': None},
            {'id': 3, 'c_bit': True},
        ]
