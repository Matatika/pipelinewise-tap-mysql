import os

import pymysql
import pytest
from testcontainers.community.mysql import MySqlContainer

from tap_mysql.connection import MARIADB_ENGINE, MYSQL_ENGINE

DB_USER = 'replication_user'
DB_PASSWORD = 'secret123passwd'
DB_ROOT_PASSWORD = 'my-secret-passwd'
DB_NAME = 'tap_mysql_test'

ENGINE_IMAGES = {
    MYSQL_ENGINE: 'mysql:8.4',
    MARIADB_ENGINE: 'mariadb:11.4',
}

ENGINE_COMMANDS = {
    MYSQL_ENGINE: (
        '--server-id=1'
        ' --gtid-mode=ON'
        ' --enforce-gtid-consistency=ON'
        ' --binlog-checksum=NONE'
        ' --log-replica-updates=ON'
        ' --log-bin=binlog'
        ' --binlog-format=ROW'
        ' --binlog-row-metadata=FULL'
    ),
    MARIADB_ENGINE: (
        '--server-id=1'
        ' --default-authentication-plugin=mysql_native_password'
        ' --log-bin=mysql-bin'
        ' --binlog-format=ROW'
        ' --binlog-row-metadata=FULL'
    ),
}


@pytest.fixture(scope='session', autouse=True)
def mysql_container():
    if os.getenv('TAP_MYSQL_EXTERNAL'):
        # Use an externally managed server described by the TAP_MYSQL_* env vars
        yield None
        return

    engine = os.environ.setdefault('TAP_MYSQL_ENGINE', MYSQL_ENGINE)
    container = MySqlContainer(
        ENGINE_IMAGES[engine],
        username=DB_USER,
        password=DB_PASSWORD,
        root_password=DB_ROOT_PASSWORD,
        dbname=DB_NAME,
    ).with_command(ENGINE_COMMANDS[engine])

    with container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(3306))

        # The entrypoint only grants privileges on DB_NAME; the tests also read
        # the binlog and drop/recreate the test database
        with pymysql.connect(host=host, port=port, user='root', password=DB_ROOT_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(f"GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '{DB_USER}'@'%'")
            conn.commit()

        os.environ['TAP_MYSQL_HOST'] = host
        os.environ['TAP_MYSQL_PORT'] = str(port)
        os.environ['TAP_MYSQL_USER'] = DB_USER
        os.environ['TAP_MYSQL_PASSWORD'] = DB_PASSWORD

        yield container
