# /// script
# dependencies = [
#   "python-dotenv",
# ]
# ///

import logging
import os
import subprocess
import time

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bench_mysqldump")

load_dotenv()

host = os.environ["TAP_MYSQL_HOST"]
port = os.environ["TAP_MYSQL_PORT"]
user = os.environ["TAP_MYSQL_USER"]
password = os.environ["TAP_MYSQL_PASSWORD"]

output_file = "dump_demo.sql"

cmd = [
    "/opt/homebrew/opt/mysql-client/bin/mysqldump",
    f"--host={host}",
    f"--port={port}",
    f"--user={user}",
    f"--password={password}",
    "--no-create-info",
    "--skip-triggers",
    "--result-file",
    output_file,
    "perf-test",
    "demo",
]

log.info("Running mysqldump…")
t0 = time.perf_counter()
result = subprocess.run(cmd, capture_output=True, text=True)
elapsed = time.perf_counter() - t0

if result.returncode != 0:
    log.error("mysqldump failed after %.3fs: %s", elapsed, result.stderr.strip())
else:
    if result.stderr.strip():
        log.warning(result.stderr.strip())
    size = os.path.getsize(output_file)
    log.info("Dumped to %s (%s bytes) in %.3fs", output_file, f"{size:,}", elapsed)
