"""Proof of concept: Sync SQLAlchemy + ThreadPoolExecutor.

Tests that sync sessions work correctly with threads, which would
enable true parallelism with free-threaded Python.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
from sqlalchemy import String, create_engine, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


# Simple test model
class Base(DeclarativeBase):
    pass


class SampleRecord(Base):
    __tablename__ = "test_records"
    id: Mapped[int] = mapped_column(primary_key=True)
    thread_name: Mapped[str] = mapped_column(String(100))
    value: Mapped[int]


def worker_sync(
    session_factory: sessionmaker,
    duckdb_path: Path,
    worker_id: int,
) -> dict:
    """Simulates a phase running in a thread with sync session."""
    thread_name = threading.current_thread().name
    start = time.time()

    # Each thread gets its own session
    with session_factory() as session:
        # Simulate some work
        record = SampleRecord(
            thread_name=thread_name,
            value=worker_id * 100,
        )
        session.add(record)
        session.commit()

        # Read back
        stmt = select(SampleRecord).where(SampleRecord.thread_name == thread_name)
        result = session.execute(stmt).scalar_one()

    # Each thread gets its own DuckDB cursor
    conn = duckdb.connect(str(duckdb_path), read_only=False)
    cursor = conn.cursor()
    try:
        # Create table if not exists (only one will succeed, others will skip)
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS thread_results (
                    worker_id INTEGER,
                    thread_name VARCHAR,
                    computed_value INTEGER
                )
            """)
        except Exception:
            pass

        # Insert result
        cursor.execute(
            "INSERT INTO thread_results VALUES (?, ?, ?)",
            [worker_id, thread_name, worker_id * 100],
        )

        # Read count
        count = cursor.execute("SELECT COUNT(*) FROM thread_results").fetchone()[0]
    finally:
        cursor.close()
        conn.close()

    duration = time.time() - start
    return {
        "worker_id": worker_id,
        "thread": thread_name,
        "sqlite_value": result.value,
        "duckdb_count": count,
        "duration": duration,
    }


def test_sync_threading_basic():
    """Test that sync SQLAlchemy + ThreadPoolExecutor works."""
    with TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        sqlite_path = tmppath / "test.db"
        duckdb_path = tmppath / "test.duckdb"

        # Create sync engine (not async!)
        engine = create_engine(
            f"sqlite:///{sqlite_path}",
            echo=False,
        )
        Base.metadata.create_all(engine)

        # Create sync session factory
        factory = sessionmaker(engine, expire_on_commit=False)

        # Initialize DuckDB
        conn = duckdb.connect(str(duckdb_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS thread_results (
                worker_id INTEGER,
                thread_name VARCHAR,
                computed_value INTEGER
            )
        """)
        conn.close()

        # Run workers in parallel
        num_workers = 4
        results = []

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = [
                pool.submit(worker_sync, factory, duckdb_path, i) for i in range(num_workers)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        # Verify results
        assert len(results) == num_workers

        # Check all workers completed
        worker_ids = {r["worker_id"] for r in results}
        assert worker_ids == set(range(num_workers))

        # Verify SQLite has all records
        with factory() as session:
            count = session.execute(text("SELECT COUNT(*) FROM test_records")).scalar()
            assert count == num_workers

        # Verify DuckDB has all records
        conn = duckdb.connect(str(duckdb_path))
        duckdb_count = conn.execute("SELECT COUNT(*) FROM thread_results").fetchone()[0]
        conn.close()
        assert duckdb_count == num_workers

        print(f"\n✓ All {num_workers} workers completed successfully")
        for r in sorted(results, key=lambda x: x["worker_id"]):
            print(f"  Worker {r['worker_id']}: {r['thread']} - {r['duration']:.3f}s")


def test_sync_threading_with_gil_disabled():
    """Test that works with GIL disabled (free-threaded Python)."""
    import sys

    # Check if we're on free-threaded Python
    if hasattr(sys, "_is_gil_enabled"):
        gil_enabled = sys._is_gil_enabled()
        print(f"\nGIL enabled: {gil_enabled}")
    else:
        print("\nNot free-threaded Python, skipping GIL check")

    # Run the same test - should work regardless of GIL
    test_sync_threading_basic()


if __name__ == "__main__":
    print("Testing sync SQLAlchemy + ThreadPoolExecutor...")
    test_sync_threading_basic()
    print("\nTesting with GIL status check...")
    test_sync_threading_with_gil_disabled()
