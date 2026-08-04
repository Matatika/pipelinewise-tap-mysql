Unreleased
------------------
* LOG_BASED:
   * Fix: Stop a column added in the middle of a table from moving the values of the events that
     MySQL wrote before the DDL into the wrong columns
   * Fix: Keep the value of a character column as a string when the MySQL client returns it as
     bytes, instead of encoding it as hex and padding it like a `BINARY(N)` value
   * Bump mysql-replication from `0.43` to `1.0.16`, to take the column names of a table from the
     binlog event instead of from the table definition as it is now
   * Set `binlog_row_metadata` to `FULL` on the source server to get the fix. No server defaults to
     `FULL` (MySQL uses `MINIMAL`, MariaDB uses `NO_LOG`), and without `FULL` the binlog holds no
     column names. MySQL supports it from `8.0.14` on, and MariaDB from `10.5` on. See the README.
   * A source server without `binlog_row_metadata = FULL`, including one too old to have the
     variable at all, keeps replicating: the tap warns and reads the column names from the table
     definition as it is now. A sync of a table that nobody alters behaves as it did before, so an
     existing pipeline keeps running.
   * Stop with an error, naming the stream and the binlog position, at an event whose values the tap
     cannot place, instead of emitting them into the wrong columns. This covers an event that carries
     no column names at all, and an event from before a column was added to or dropped from the
     middle of a table. Re-sync the stream with `FULL_TABLE` to get past those events. The check
     compares column counts, so a change that keeps the count the same, such as reordering columns,
     still needs `binlog_row_metadata = FULL` to be safe.
   * Check the server settings on every LOG_BASED run, not only on the run that takes the initial
     snapshot
   * A column rename needs a `FULL_TABLE` re-sync: an event that MySQL wrote before the rename
     carries the old name of the column, so the tap leaves the value out of the record. The tap
     warns with the stream, the column and the binlog position when this happens.
   * Run discovery once for a column that no longer exists, instead of once per event that holds it
   * Fix: Report a missing `binlog_row_image` or `binlog_row_metadata` variable properly. The tap
     caught `InternalError`, but the MySQL client raises `DatabaseError` for an unknown system
     variable, so the message never appeared.


1.5.6 (2023-08-10)
------------------
* LOG_BASED, INCREMENTAL and FULL TABLE: 
   * Zero-pad fixed-length binary fields


1.5.5 (2023-07-05)
------------------
* LOG_BASED: 
   * Fix: `LookupError: unknown encoding: utf8mb3`
   * Bump plpygis from `0.2.0` to `0.2.1`
  

1.5.4 (2023-05-22)
------------------
* LOG_BASED: 
   * Bump pymsql-replication from `0.30` to `0.40`
   * Remove the custom BinlogStreamReader


1.5.3 (2023-04-25)
------------------
* LOG_BASED: Set mariadb slave capability to 4 to mitigate bug in Mariadb 10.6.12 (https://github.com/transferwise/pipelinewise-tap-mysql/pull/149)


1.5.2 (2022-08-12)
------------------
* Bump mysql-replication to 0.30


1.5.1 (2022-04-04)
------------------
* Fix: Handle case when BINLOG_GTID_POS returns multiple comma separated GTIDs


1.5.0 (2022-03-11)
------------------
* Support logical replication using GTID, for both Mariadb & MySql 
* Log error message when session sqls fail
* Bump depenedencies to support Mysql 8
* Migrate CI to Github Actions.

1.4.3 (2021-04-09)
------------------
* Fix in LOG_BASED method: re-discovery constantly running when table has unsupported column type.
* Add support for tinytext column type
* Bump `pendulum` to 1.5.1
* Add unit tests and re-arrange tests folder

1.4.2 (2021-03-15)
------------------
* Fix a typo

1.4.1 (2021-03-12)
------------------
* Fix data loss during log based replication by processesing binglog events until a saved Master position.
* Bump mysql-replication from 0.22 to 0.23

1.4.0 (2020-11-09)
------------------
Support MySQL spatial types

1.3.8 (2020-10-16)
------------------
Fix mapping bit to boolean values

1.3.7 (2020-09-04)
------------------
Fix for time sql type.

1.3.6 (2020-09-02)
------------------
Remove info log

1.3.5 (2020-08-27)
------------------
Properly support `time` sql type.

1.3.4 (2020-08-05)
------------------
Fix few issues with new discovered schema after changes are detected during LOG_BASED runtime.

1.3.3 (2020-07-23)
------------------
During LOG_BASED runtime, detect new columns, incl renamed ones, by comparing the columns in the binlog event to the stream schema, and if there are any additional columns, run discovery and send a new SCHEMA message to target. This helps avoid data loss.


1.3.2 (2020-06-15)
-------------------

-  Revert `pymysql` back to `0.7.11`.
   `pymysql >= 0.8.1` introducing some not expected and not backward compatible changes how it's dealing with
   invalid datetime columns.

1.3.1 (2020-06-15)
-------------------

-  Fix dependency issue by removing `attrs` from `setup.py`
-  Bump `pymysql` to `0.9.3`

1.3.0 (2020-05-18)
-------------------

-  Add optional `session_sqls` connection parameter
-  Support `JSON` column types

1.2.0 (2020-02-18)
-------------------

- Make logging customizable

1.1.5 (2020-01-21)
-------------------

- Update bookmark only if binlog position is valid

1.1.4 (2020-01-21)
-------------------

- Update bookmark when reading bookmark finished

1.1.3 (2020-01-20)
-------------------

- Update bookmark only before writing state message

1.1.2 (2020-01-14)
-------------------

- Handle null bytes in `BINARY` type columns using SQL

1.1.1 (2020-01-07)
-------------------

- Handle padding zeros in `BINARY` type columns

1.1.0 (2019-12-27)
-------------------

- Support `BINARY` and `VARBINARY` column types

1.0.7 (2019-12-06)
-------------------

- Bump `mysql-replication` to 0.21

1.0.6 (2019-08-16)
-------------------

- Add license classifier

1.0.5 (2019-05-28)
-------------------

- Remove faulty `BINARY` and `VARBINARY` support
