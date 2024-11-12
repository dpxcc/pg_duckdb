CREATE TABLE t(a int);
INSERT INTO t VALUES (1);

CREATE TEMP TABLE t_ddb(a int) USING duckdb;
INSERT INTO t_ddb VALUES (1);

BEGIN;
SELECT * FROM t_ddb;
INSERT INTO t_ddb VALUES (2);
SELECT * FROM t_ddb ORDER BY a;
ROLLBACK;

SELECT * FROM t_ddb;

-- Writing to PG and DDB tables in the same transaction is not supported. We
-- fail early for simple DML (no matter the order).
BEGIN;
INSERT INTO t_ddb VALUES (2);
INSERT INTO t VALUES (2);
ROLLBACK;

BEGIN;
INSERT INTO t VALUES (2);
INSERT INTO t_ddb VALUES (2);
ROLLBACK;

-- And for other writes that are not easy to detect, such as CREATE TABLE, we
-- fail on COMMIT.
BEGIN;
INSERT INTO t_ddb VALUES (2);
CREATE TABLE t2(a int);
COMMIT;

-- Savepoints in PG-only transactions should still work
BEGIN;
INSERT INTO t VALUES (2);
SAVEPOINT my_savepoint;
INSERT INTO t VALUES (3);
ROLLBACK TO SAVEPOINT my_savepoint;
COMMIT;

-- But savepoints are not allowed in DuckDB transactions
BEGIN;
INSERT INTO t_ddb VALUES (2);
SAVEPOINT my_savepoint;
ROLLBACK;;

-- Also not already started ones
BEGIN;
SAVEPOINT my_savepoint;
INSERT INTO t_ddb VALUES (2);
ROLLBACK;;

-- Unless the subtransaction is already completed
BEGIN;
SET LOCAL duckdb.force_execution = false;
SAVEPOINT my_savepoint;
SELECT count(*) FROM t;
RELEASE SAVEPOINT my_savepoint;
INSERT INTO t_ddb VALUES (2);
COMMIT;

TRUNCATE t_ddb;
INSERT INTO t_ddb VALUES (1);

BEGIN;
DECLARE c SCROLL CURSOR FOR SELECT a FROM t_ddb;
FETCH NEXT FROM c;
FETCH NEXT FROM c;
-- FIXME: Fix the crash that occurs when uncommenting the next line
-- FETCH PRIOR FROM c;
COMMIT;
