# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "python-dotenv",
#   "adbc-driver-manager>=1.9.0",
#   "pyarrow>=20.0.0",
# ]
# ///

import logging
import os
import time

from adbc_driver_manager import dbapi
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bench_adbc")

load_dotenv()

host = os.environ["TAP_MYSQL_HOST"]
port = os.environ["TAP_MYSQL_PORT"]
user = os.environ["TAP_MYSQL_USER"]
password = os.environ["TAP_MYSQL_PASSWORD"]

uri = f"{user}:{password}@tcp({host}:{port})/perf-test"

log.info("Connecting…")
t0 = time.perf_counter()
with (
    dbapi.connect(driver="mysql", db_kwargs={"uri": uri}) as con,
    con.cursor() as cursor,
):
    log.info("Connected in %.3fs", time.perf_counter() - t0)

    log.info("Executing query…")
    t1 = time.perf_counter()
    cursor.execute("SELECT * FROM `demo`")
    log.info("Query executed in %.3fs", time.perf_counter() - t1)

    log.info("Fetching as Arrow table…")
    t2 = time.perf_counter()
    table = cursor.fetch_arrow_table()
    log.info("Fetched %d rows in %.3fs", len(table), time.perf_counter() - t2)

    log.info("Schema: %s", table.schema)
    log.info("Total time: %.3fs", time.perf_counter() - t0)
