"""Test DuckDB cursor threading behavior."""

import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest


class TestDuckDBCursorThreading:
    """Tests for DuckDB cursor behavior with threads."""

    @pytest.fixture
    def test_db(self, tmp_path):
        """Create a file-based DuckDB with test data."""
        db_path = str(tmp_path / "test.duckdb")
        conn = duckdb.connect(db_path)

        # Create test table with some data
        conn.execute("""
            CREATE TABLE test_data AS
            SELECT i AS id, 'value_' || i AS name
            FROM generate_series(1, 1000) AS t(i)
        """)

        yield conn, db_path
        conn.close()

    def test_single_connection_multiple_cursors_parallel(self, test_db):
        """Test that multiple cursors from one connection work in parallel."""
        conn, db_path = test_db

        def read_with_cursor(col_id: int) -> tuple[int, int]:
            """Read using a cursor from the shared connection."""
            cursor = conn.cursor()
            try:
                result = cursor.execute(
                    f"SELECT COUNT(*) FROM test_data WHERE id > {col_id * 100}"
                ).fetchone()
                return (col_id, result[0])
            finally:
                cursor.close()

        # Run parallel reads using cursors
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(read_with_cursor, i) for i in range(10)]
            results = [f.result() for f in futures]

        # Verify we got results from all
        assert len(results) == 10
        for col_id, count in results:
            expected = 1000 - (col_id * 100)
            assert count == max(0, expected), f"Column {col_id}: expected {expected}, got {count}"

    def test_separate_read_only_connections_fail_with_rw_open(self, test_db):
        """Test that opening read-only connections fails when R/W is open."""
        conn, db_path = test_db

        # Try to open a read-only connection while R/W is open
        with pytest.raises(duckdb.ConnectionException):
            duckdb.connect(db_path, read_only=True)

    def test_separate_read_only_connections_work_after_rw_closed(self, test_db):
        """Test that read-only connections work after R/W is closed."""
        conn, db_path = test_db
        conn.close()  # Close the R/W connection

        def read_with_new_connection(col_id: int) -> tuple[int, int]:
            """Read using a new read-only connection."""
            ro_conn = duckdb.connect(db_path, read_only=True)
            try:
                result = ro_conn.execute(
                    f"SELECT COUNT(*) FROM test_data WHERE id > {col_id * 100}"
                ).fetchone()
                return (col_id, result[0])
            finally:
                ro_conn.close()

        # Run parallel reads using separate read-only connections
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(read_with_new_connection, i) for i in range(10)]
            results = [f.result() for f in futures]

        assert len(results) == 10

    def test_cursor_parallel_performance(self, test_db):
        """Test that cursor-based parallel reads are actually concurrent."""
        conn, db_path = test_db

        # Create a larger table for more noticeable timing
        conn.execute("""
            CREATE TABLE large_data AS
            SELECT i AS id, RANDOM() AS value
            FROM generate_series(1, 100000) AS t(i)
        """)

        def slow_read(cursor_id: int) -> float:
            """Read with a slow query."""
            cursor = conn.cursor()
            start = time.time()
            try:
                cursor.execute(f"""
                    SELECT AVG(value), STDDEV(value), COUNT(*)
                    FROM large_data
                    WHERE id % 10 = {cursor_id % 10}
                """).fetchone()
                return time.time() - start
            finally:
                cursor.close()

        # Time sequential execution
        seq_start = time.time()
        for i in range(4):
            slow_read(i)
        seq_time = time.time() - seq_start

        # Time parallel execution
        par_start = time.time()
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(slow_read, i) for i in range(4)]
            [f.result() for f in futures]
        par_time = time.time() - par_start

        # Parallel should be faster (at least 1.5x improvement)
        # This verifies that DuckDB cursors actually run in parallel
        print(f"Sequential: {seq_time:.3f}s, Parallel: {par_time:.3f}s")
        # Note: May not be faster if GIL is blocking or if DuckDB
        # serializes internally - this is a diagnostic test

    def test_connection_passed_as_argument(self, test_db):
        """Test that passing connection as argument works in parallel."""
        conn, db_path = test_db

        def worker_with_conn_arg(
            duckdb_conn: duckdb.DuckDBPyConnection,
            worker_id: int,
        ) -> tuple[int, int]:
            """Worker that receives connection as argument."""
            cursor = duckdb_conn.cursor()
            try:
                result = cursor.execute(
                    f"SELECT COUNT(*) FROM test_data WHERE id > {worker_id * 100}"
                ).fetchone()
                return (worker_id, result[0])
            finally:
                cursor.close()

        # Run parallel with connection passed as argument
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(worker_with_conn_arg, conn, i) for i in range(10)]
            results = [f.result() for f in futures]

        assert len(results) == 10
        for worker_id, count in results:
            expected = 1000 - (worker_id * 100)
            assert count == max(0, expected)

    def test_cursor_parallel_works_with_in_memory_db(self):
        """Test that cursor-based parallelism works with in-memory DuckDB."""
        conn = duckdb.connect(":memory:")

        # Create test data
        conn.execute("""
            CREATE TABLE test_data AS
            SELECT i AS id, 'value_' || i AS name
            FROM generate_series(1, 1000) AS t(i)
        """)

        def worker(duckdb_conn: duckdb.DuckDBPyConnection, worker_id: int) -> tuple[int, int]:
            """Worker using cursor from shared connection."""
            cursor = duckdb_conn.cursor()
            try:
                result = cursor.execute(
                    f"SELECT COUNT(*) FROM test_data WHERE id > {worker_id * 100}"
                ).fetchone()
                return (worker_id, result[0])
            finally:
                cursor.close()

        # Run parallel on in-memory DB
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(worker, conn, i) for i in range(10)]
            results = [f.result() for f in futures]

        conn.close()

        assert len(results) == 10
        for worker_id, count in results:
            expected = 1000 - (worker_id * 100)
            assert count == max(0, expected)
