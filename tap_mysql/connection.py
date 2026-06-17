#!/usr/bin/env python3
# pylint: disable=missing-docstring,arguments-differ,missing-function-docstring

import os
import ssl
import tempfile

import backoff
import mysql.connector
import mysql.connector.errors
import pymysql
import pymysql.cursors
import singer
from pymysql.constants import CLIENT

LOGGER = singer.get_logger('tap_mysql')

CONNECT_TIMEOUT_SECONDS = 30

MATCH_HOSTNAME = getattr(ssl, 'match_hostname', None)
MARIADB_ENGINE = 'mariadb'
MYSQL_ENGINE = 'mysql'

DEFAULT_SESSION_SQLS = ['SET @@session.time_zone="+0:00"',
                        'SET @@session.wait_timeout=28800',
                        'SET @@session.net_read_timeout=3600',
                        'SET @@session.innodb_lock_wait_timeout=3600']


@backoff.on_exception(backoff.expo,
                      (mysql.connector.errors.OperationalError, pymysql.err.OperationalError),
                      max_tries=5,
                      factor=2)
def connect_with_backoff(connection):
    connection.connect()
    run_session_sqls(connection)

    return connection


def run_session_sqls(connection):
    session_sqls = connection.session_sqls

    warnings = []
    if session_sqls and isinstance(session_sqls, list):
        for sql in session_sqls:
            try:
                run_sql(connection, sql)
            except (mysql.connector.errors.InternalError, pymysql.err.InternalError) as exc:
                warnings.append(f'Could not set session variable `{sql}`: {exc}')

    if warnings:
        LOGGER.warning('Encountered non-fatal errors when configuring session that could impact performance:')
    for warning in warnings:
        LOGGER.warning(warning)


def run_sql(connection, sql):
    with connection.cursor() as cur:
        cur.execute(sql)


def parse_internal_hostname(hostname):
    # special handling for google cloud
    if ":" in hostname:
        parts = hostname.split(":")
        if len(parts) == 3:
            return parts[0] + ":" + parts[2]
        return parts[0] + ":" + parts[1]

    return hostname


class MySQLConnection:
    """mysql-connector-python based connection used for all SELECT-based syncs."""

    def __init__(self, config):
        self._config = config
        self._conn = None
        self._ssl_temp_files: list[str] = []
        self.session_sqls = config.get('session_sqls', DEFAULT_SESSION_SQLS)

    @staticmethod
    def _write_ssl_tempfile(pem_data: str) -> str:
        """Write PEM data to a private temp file, returning its path."""
        fd, path = tempfile.mkstemp(suffix='.pem')
        try:
            os.write(fd, pem_data.encode('utf-8'))
        finally:
            os.close(fd)
        os.chmod(path, 0o600)
        return path

    def connect(self):
        config = self._config
        args = {
            'user': config['user'],
            'password': config['password'],
            'host': config['host'],
            'port': int(config['port']),
            'charset': 'utf8',
            'connection_timeout': CONNECT_TIMEOUT_SECONDS,
            'autocommit': True,
            'compress': True,
        }

        if config.get('database'):
            args['database'] = config['database']

        use_self_signed_ssl = config.get('ssl_ca') and config.get('ssl_cert') and config.get('ssl_key')

        if use_self_signed_ssl:
            LOGGER.info('Using custom certificate authority')

            ca_path = self._write_ssl_tempfile(config['ssl_ca'])
            cert_path = self._write_ssl_tempfile(config['ssl_cert'])
            key_path = self._write_ssl_tempfile(config['ssl_key'])
            self._ssl_temp_files = [ca_path, cert_path, key_path]

            args.update({'ssl_ca': ca_path, 'ssl_cert': cert_path, 'ssl_key': key_path})

            if config.get('internal_hostname') and MATCH_HOSTNAME is not None:
                parsed_hostname = parse_internal_hostname(config['internal_hostname'])
                _match = MATCH_HOSTNAME
                setattr(ssl, 'match_hostname', lambda cert, _h: _match(cert, parsed_hostname))

        elif config.get('ssl') == 'true':
            LOGGER.info('Attempting SSL connection')
            args.update({'ssl_disabled': False, 'ssl_verify_cert': False, 'ssl_verify_identity': False})

        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # pylint: disable=broad-except
                pass
        self._conn = mysql.connector.connect(**args)

    def commit(self):
        self._conn.commit()

    def get_server_info(self) -> str:
        return ".".join(str(v) for v in self._conn.server_version)

    def cursor(self, buffered=False):
        return self._conn.cursor(buffered=buffered)

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
        for path in self._ssl_temp_files:
            try:
                os.unlink(path)
            except OSError:
                pass
        self._ssl_temp_files = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


class _PyMySQLConnectionBase(pymysql.connections.Connection):
    """Internal pymysql connection — kept only for BinLogStreamReader compatibility."""

    def __init__(self, config):
        args = {
            'user': config['user'],
            'password': config['password'],
            'host': config['host'],
            'port': int(config['port']),
            'cursorclass': config.get('cursorclass') or pymysql.cursors.SSCursor,
            'connect_timeout': CONNECT_TIMEOUT_SECONDS,
            'charset': 'utf8',
        }

        ssl_arg = None

        if config.get('database'):
            args['database'] = config['database']

        use_self_signed_ssl = config.get('ssl_ca') and config.get('ssl_cert') and config.get('ssl_key')

        if use_self_signed_ssl:
            ssl_arg = {'ca': './ca.pem', 'cert': './cert.pem', 'key': './key.pem'}

            if config.get('internal_hostname') and MATCH_HOSTNAME is not None:
                parsed_hostname = parse_internal_hostname(config['internal_hostname'])
                _match = MATCH_HOSTNAME
                setattr(ssl, 'match_hostname', lambda cert, _h: _match(cert, parsed_hostname))

        super().__init__(defer_connect=True, ssl=ssl_arg, **args)

        if config.get('ssl') == 'true' and not use_self_signed_ssl:
            LOGGER.info('Attempting SSL connection')
            self.ssl = True
            self.ctx = ssl.create_default_context()
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE
            self.client_flag |= CLIENT.SSL

        self.session_sqls = config.get('session_sqls', DEFAULT_SESSION_SQLS)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(_PyMySQLConnectionBase):
        def __init__(self, *args, **kwargs):  # pylint: disable=unused-argument
            config['cursorclass'] = kwargs.get('cursorclass')
            super().__init__(config)

            connect_with_backoff(self)

    return ConnectionWrapper


def fetch_server_id(mysql_conn: MySQLConnection) -> int:
    """
    Finds server ID
    Args:
        mysql_conn: Mysql connection instance

    Returns: server ID
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT @@server_id")
            server_id = cur.fetchone()[0]

            return server_id


def fetch_server_uuid(mysql_conn: MySQLConnection) -> str:
    """
    Finds server UUID
    Args:
        mysql_conn: Mysql connection instance

    Returns: server UUID
    """
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT @@server_uuid")
            server_uuid = cur.fetchone()[0]

            return server_uuid
