import os

import pymysql
import singer

import tap_mysql
import tap_mysql.sync_strategies.common as common
from tap_mysql.connection import MySQLConnection

DB_NAME = 'tap_mysql_test'


def get_db_config():
    config = {
        'host': os.environ['TAP_MYSQL_HOST'],
        'port': int(os.environ['TAP_MYSQL_PORT']),
        'user': os.environ['TAP_MYSQL_USER'],
        'password': os.environ['TAP_MYSQL_PASSWORD'],
        'charset': 'utf8',
    }
    if not config['password']:
        del config['password']

    return config


def get_test_connection(extra_config=None):
    db_config = get_db_config()

    con = pymysql.connect(**db_config)

    try:
        with con.cursor() as cur:
            try:
                cur.execute('DROP DATABASE {}'.format(DB_NAME))
            except:
                pass
            cur.execute('CREATE DATABASE {}'.format(DB_NAME))
    finally:
        con.close()

    db_config['database'] = DB_NAME
    db_config['autocommit'] = True

    if not extra_config:
        extra_config = {}
    mysql_conn = MySQLConnection({**db_config, **extra_config})
    mysql_conn.autocommit_mode = True

    return mysql_conn


def discover_catalog(connection) -> singer.Catalog:
    catalog = tap_mysql.discover_catalog(connection)
    streams = [stream for stream in catalog.streams if common.get_database_name(stream) == DB_NAME]

    return singer.Catalog.from_entries(streams)


def set_replication_method_and_key(stream, r_method, r_key):
    root = stream.metadata.root
    if r_method:
        root.replication_method = r_method

    if r_key:
        root.replication_key = r_key

    return stream


def set_selected(stream, selected=False):
    stream.metadata.root.selected = selected
    return stream
