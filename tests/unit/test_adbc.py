import unittest
from unittest import mock

from tap_mysql import adbc


class TestBuildUri(unittest.TestCase):

    def test_build_uri_encodes_password(self):
        uri = adbc._build_uri('h', 3306, 'u', 'p@ss/word', 'db')
        self.assertEqual(uri, 'mysql://u:p%40ss%2Fword@h:3306/db')

    def test_build_uri_no_database_uses_root_path(self):
        uri = adbc._build_uri('h', 3306, 'u', 'p', None)
        self.assertTrue(uri.endswith('/'))
        self.assertNotIn('//h:3306//', uri)


class TestBuildDbKwargs(unittest.TestCase):

    def test_build_db_kwargs_maps_ssl_keys(self):
        kwargs = adbc._build_db_kwargs({'ssl_ca': 'ca-data', 'ssl_cert': 'cert-data', 'ssl_key': 'key-data'})
        self.assertEqual(kwargs, {
            'adbc.mysql.connect_string.tls_ca': 'ca-data',
            'adbc.mysql.connect_string.tls_cert': 'cert-data',
            'adbc.mysql.connect_string.tls_key': 'key-data',
        })

    def test_build_db_kwargs_omits_absent_ssl_keys(self):
        self.assertEqual(adbc._build_db_kwargs({}), {})
        self.assertEqual(adbc._build_db_kwargs({'ssl_ca': 'ca-data'}),
                         {'adbc.mysql.connect_string.tls_ca': 'ca-data'})


class TestToQmark(unittest.TestCase):

    def test_no_placeholders_returns_sql_unchanged_and_empty_values(self):
        sql, values = adbc._to_qmark('SELECT * FROM t', {})
        self.assertEqual(sql, 'SELECT * FROM t')
        self.assertEqual(values, [])

    def test_translates_single_named_param(self):
        sql, values = adbc._to_qmark('SELECT * FROM t WHERE x >= %(x)s', {'x': 5})
        self.assertEqual(sql, 'SELECT * FROM t WHERE x >= ?')
        self.assertEqual(values, [5])

    def test_translates_multiple_named_params_in_appearance_order(self):
        sql, values = adbc._to_qmark(
            'SELECT * FROM t WHERE a >= %(a)s AND b <= %(b)s', {'a': 1, 'b': 2})
        self.assertEqual(sql, 'SELECT * FROM t WHERE a >= ? AND b <= ?')
        self.assertEqual(values, [1, 2])


class TestConnectAndStreamRecordBatches(unittest.TestCase):

    def _make_fake_adbc_dbapi(self, fake_conn):
        fake_dbapi = mock.MagicMock()
        fake_dbapi.connect.return_value = fake_conn
        return fake_dbapi

    def test_connect_closes_connection_on_normal_exit(self):
        fake_conn = mock.MagicMock()

        with mock.patch.object(adbc.dbapi, 'connect', return_value=fake_conn):
            with adbc.connect({'host': 'h', 'port': 3306, 'user': 'u', 'password': 'p'}) as conn:
                self.assertIs(conn, fake_conn)

        fake_conn.close.assert_called_once()

    def test_connect_closes_connection_when_body_raises(self):
        fake_conn = mock.MagicMock()

        with mock.patch.object(adbc.dbapi, 'connect', return_value=fake_conn):
            with self.assertRaises(RuntimeError):
                with adbc.connect({'host': 'h', 'port': 3306, 'user': 'u', 'password': 'p'}):
                    raise RuntimeError('boom')

        fake_conn.close.assert_called_once()

    def test_stream_record_batches_executes_and_yields_reader(self):
        fake_reader = mock.sentinel.record_batch_reader
        fake_cursor = mock.MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.fetch_record_batch.return_value = fake_reader

        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor

        with mock.patch.object(adbc.dbapi, 'connect', return_value=fake_conn):
            with adbc.stream_record_batches({'host': 'h', 'port': 3306, 'user': 'u', 'password': 'p'},
                                            'SELECT * FROM t WHERE x >= %(x)s', {'x': 1}) as reader:
                self.assertIs(reader, fake_reader)

        # runs the translated qmark query
        fake_cursor.execute.assert_called_with('SELECT * FROM t WHERE x >= ?', [1])
        fake_conn.close.assert_called_once()


class TestRequireArrowSupport(unittest.TestCase):

    def test_wraps_driver_load_failure(self):
        with mock.patch.object(
            adbc,
            "AdbcDatabase",
            side_effect=Exception("NOT_FOUND: could not load driver"),
        ):
            with self.assertRaises(adbc.ArrowSupportError) as ctx:
                adbc.require_arrow_support()

        self.assertIn('dbc install mysql', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
