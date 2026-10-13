# Benchmark scripts

Quick scripts for measuring raw MySQL fetch throughput against the `perf-test.demo` table (10M rows). All read credentials from `.env`.

## Results (2026-06-16, remote MySQL 8.4.8 over TLS)

| Script | Fetch time | Throughput |
|---|---|---|
| `bench_mysql.py` (compress=True) | **38.7s** | ~258k rows/s |
| `bench_mysql.py` (no compress) | 158.8s | ~63k rows/s |
| `bench_adbc.py` | 155.0s | ~64k rows/s |
| `bench_mysqldump.py` | 159.6s | 805 MB SQL |
| `bench_pymysql.py` | 251.8s | ~40k rows/s |

`compress=True` (MySQL protocol zlib compression) gives a **4× speedup** on this workload — `email`/`name`/`amount` fields are highly compressible. This is now set in `tap_mysql/connection.py`.

## Running

```sh
# requires .env with TAP_MYSQL_HOST, TAP_MYSQL_PORT, TAP_MYSQL_USER, TAP_MYSQL_PASSWORD
uv run scripts/bench_mysql.py
uv run scripts/bench_adbc.py
uv run scripts/bench_pymysql.py
uv run scripts/bench_mysqldump.py   # needs: brew install mysql-client
                                    # binary: /opt/homebrew/opt/mysql-client/bin/mysqldump
```
