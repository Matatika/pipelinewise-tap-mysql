#!/usr/bin/env python3
"""Apache Arrow / ADBC connectivity for batch_config.encoding.format='arrow' mode.

``pyarrow`` and ``adbc-driver-manager`` are regular (required) dependencies of
this package -- both are pure-wheel and pip-installable. The native MySQL ADBC
driver itself is *not* pip-installable, though, and must be installed
separately, e.g. via ``dbc install mysql`` (the CLI that ships with
``adbc-driver-manager`` - see https://docs.adbc-drivers.org/drivers/mysql/).
Arrow BATCH mode fails fast with an actionable error (see
``require_arrow_support``) if that native driver isn't present, without
requiring any extra Python packages beyond the tap's normal install.

Nothing in this module is imported eagerly by the rest of the tap; it is
only touched when ``batch_config.encoding.format == 'arrow'``.
"""

from __future__ import annotations

import contextlib
import re
from typing import Optional
from urllib.parse import quote_plus

import singer
from adbc_driver_manager import AdbcDatabase, dbapi

LOGGER = singer.get_logger('tap_mysql')

# Relax invalid-date rejection for the duration of this connection so the NULLIF-based
# invalid-date-to-NULL SQL (see common.generate_select_sql's null_invalid_dates option) can
# run at all -- MySQL rejects a '0000-00-00'-style literal at parse time under the default
# strict sql_mode, even when it's wrapped in NULLIF and never actually returned. Only the two
# zero-date-related flags are removed, leaving the rest of sql_mode (and any DML-time
# protections, though this connection is read-only) untouched.
_RELAX_ZERO_DATE_SQL = (
    "SET SESSION sql_mode = REPLACE(REPLACE(@@sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')"
)

_NAMED_PARAM_RE = re.compile(r'%\((\w+)\)s')


class ArrowSupportError(RuntimeError):
    """Raised when Arrow BATCH mode is requested but Arrow/ADBC support is unavailable."""


def require_arrow_support() -> None:
    """Eagerly validate Arrow/ADBC support is usable.

    Raises ArrowSupportError with an actionable message if pyarrow/adbc-driver-manager
    aren't installed, or if the native MySQL ADBC driver can't be loaded. Called from
    BatchConfig.__post_init__ so failures surface at startup, not mid-sync.
    """
    try:
        # Constructing *without* a `uri` never opens a network connection: the MySQL
        # ADBC driver only reaches its "missing required option uri" validation error
        # after its native shared library has been located and loaded successfully.
        # This confirms the driver is installed without needing a reachable MySQL server.
        db = AdbcDatabase(driver='mysql')
    except Exception as exc:  # pylint: disable=broad-except
        if 'uri' in str(exc).lower():
            return
        raise ArrowSupportError(
            "Arrow BATCH mode is configured but the native MySQL ADBC driver could not "
            "be loaded. The MySQL ADBC driver itself is missing. Install it with "
            "`dbc install mysql` (see https://docs.adbc-drivers.org/drivers/mysql/). "
            f"Underlying error: {exc}"
        ) from exc
    else:
        db.close()


def _build_uri(host: str, port: int, user: str, password: str, database: Optional[str]) -> str:
    encoded_pass = quote_plus(password)
    db_part = f'/{database}' if database else '/'
    return f'mysql://{user}:{encoded_pass}@{host}:{port}{db_part}'


def _build_db_kwargs(config: dict) -> dict:
    """Map tap-mysql config keys (reused verbatim, no new SSL config keys) to ADBC db_kwargs."""
    kwargs: dict[str, str] = {}
    if config.get('ssl_ca'):
        kwargs['adbc.mysql.connect_string.tls_ca'] = config['ssl_ca']
    if config.get('ssl_cert'):
        kwargs['adbc.mysql.connect_string.tls_cert'] = config['ssl_cert']
    if config.get('ssl_key'):
        kwargs['adbc.mysql.connect_string.tls_key'] = config['ssl_key']
    return kwargs


def _to_qmark(select_sql: str, params: dict) -> tuple:
    """Translate mysql-connector-style named placeholders (`%(name)s`) plus a params dict
    into ADBC's qmark paramstyle (`?` placeholders plus a positional values list).

    ADBC's MySQL driver doesn't support pyformat/named parameters the way mysql-connector
    does -- passing `%(name)s` through verbatim produces a MySQL syntax error, since ADBC
    never substitutes it.
    """
    values = []

    def _replace(match):
        values.append(params[match.group(1)])
        return '?'

    return _NAMED_PARAM_RE.sub(_replace, select_sql), values


@contextlib.contextmanager
def connect(config: dict):
    """Yield an ADBC DBAPI connection built from a tap-mysql config dict.

    Reuses the same config keys as MySQLConnection (host/port/user/password/database/
    ssl_ca/ssl_cert/ssl_key) - no new config keys introduced for connectivity.
    """
    db_kwargs = _build_db_kwargs(config)
    db_kwargs['uri'] = _build_uri(
        config['host'],
        int(config['port']),
        config['user'],
        config['password'],
        config.get('database'),
    )

    # TODO: Stop passing autocommit=True once the ADBC driver support transactions properly
    # https://github.com/adbc-drivers/mysql/issues/55
    conn = dbapi.connect(
        driver='mysql',
        db_kwargs=db_kwargs,
        conn_kwargs={
            # https://github.com/adbc-drivers/mysql/issues/114
            # https://github.com/adbc-drivers/mysql/pull/116
            'mysql.query.zero_datetime_behavior': 'convert_to_null',
        },
        autocommit=True,
    )
    try:
        yield conn
    finally:
        conn.close()


@contextlib.contextmanager
def stream_record_batches(config: dict, select_sql: str, params: Optional[dict] = None):
    """Execute select_sql over ADBC and yield a pyarrow.RecordBatchReader.

    select_sql is expected to already be fully built (by common.generate_select_sql plus
    the caller's WHERE/ORDER BY clauses) - this function does no SQL construction itself.
    """
    qmark_sql, values = _to_qmark(select_sql, params or {})
    with connect(config) as conn:
        with conn.cursor() as cur:
            cur.execute(qmark_sql, values)
            yield cur.fetch_record_batch()


def get_retryable_exceptions() -> tuple:
    """Return the ADBC exception types that should trigger the same sync-retry behavior
    as mysql.connector.errors.OperationalError does today. Lazily imported so callers
    (full_table.py/incremental.py) don't need an unconditional top-level ADBC import."""
    from adbc_driver_manager import OperationalError
    return (OperationalError,)
