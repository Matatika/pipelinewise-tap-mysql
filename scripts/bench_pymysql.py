# /// script
# dependencies = [
#   "python-dotenv",
#   "PyMySQL",
# ]
# ///

import logging
import os
import time

import pymysql
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bench_pymysql")

load_dotenv()

log.info("Connecting…")
t0 = time.perf_counter()
cnx = pymysql.connect(
    host=os.environ["TAP_MYSQL_HOST"],
    port=int(os.environ["TAP_MYSQL_PORT"]),
    user=os.environ["TAP_MYSQL_USER"],
    password=os.environ["TAP_MYSQL_PASSWORD"],
    database="perf-test",
)
log.info("Connected in %.3fs", time.perf_counter() - t0)

cur = cnx.cursor()

log.info("Executing query…")
t1 = time.perf_counter()
cur.execute("SELECT * FROM `demo`")
log.info("Query executed in %.3fs", time.perf_counter() - t1)

log.info("Fetching all rows…")
t2 = time.perf_counter()
rows = cur.fetchall()
log.info("Fetched %d rows in %.3fs", len(rows), time.perf_counter() - t2)

log.info("Total time: %.3fs", time.perf_counter() - t0)

cur.close()
cnx.close()
