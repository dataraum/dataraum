#!/usr/bin/env python3
"""Phase 6 verification: analysis/relationships module."""

import asyncio
from pathlib import Path

import duckdb
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.analysis.relationships import (
    detect_relationships,
    find_relationships,
)
from dataraum_context.analysis.typing import infer_type_candidates, resolve_types
from dataraum_context.core.models import SourceConfig
from dataraum_context.sources.csv import CSVLoader
from dataraum_context.storage import Column, Table, init_database


async def main():
    print("=" * 70)
    print("Phase 6: analysis/relationships")
    print("=" * 70)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    await init_database(engine)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    duckdb_conn = duckdb.connect(":memory:")

    csv_dir = Path("examples/finance_csv_example")
    csv_files = [
        csv_dir / "customer_table.csv",
        csv_dir / "vendor_table.csv",
        csv_dir / "payment_method.csv",
    ]

    print(f"\n1. Loading {len(csv_files)} tables")

    table_ids = []
    async with async_session() as session:
        loader = CSVLoader()

        for csv_path in csv_files:
            if not csv_path.exists():
                print(f"   Missing: {csv_path}")
                continue

            config = SourceConfig(name=csv_path.stem.lower(), source_type="csv", path=str(csv_path))
            load_result = await loader.load(config, duckdb_conn, session)
            if not load_result.success:
                continue

            staged = load_result.unwrap().tables[0]
            raw_table = await session.get(Table, staged.table_id)
            if not raw_table:
                continue

            # Drop junk columns
            for junk in ["column00", "Unnamed: 0.2", "Unnamed: 0.1", "Unnamed: 0"]:
                try:
                    duckdb_conn.execute(f'ALTER TABLE {raw_table.duckdb_path} DROP COLUMN "{junk}"')
                    stmt = select(Column).where(
                        Column.table_id == raw_table.table_id, Column.column_name == junk
                    )
                    col = (await session.execute(stmt)).scalar_one_or_none()
                    if col:
                        await session.delete(col)
                except Exception:
                    pass
            await session.commit()

            # Type resolution
            await infer_type_candidates(raw_table, duckdb_conn, session)
            resolve_result = await resolve_types(staged.table_id, duckdb_conn, session)
            if resolve_result.success:
                resolution = resolve_result.unwrap()
                stmt = select(Table).where(Table.duckdb_path == resolution.typed_table_name)
                typed = (await session.execute(stmt)).scalar_one_or_none()
                if typed:
                    table_ids.append(typed.table_id)
                    print(f"   {csv_path.name} -> {resolution.typed_table_name}")

        # Test direct finder
        print("\n2. Direct find_relationships()")
        print("-" * 50)

        tables_data = {}
        for tid in table_ids:
            t = await session.get(Table, tid)
            if t:
                # Sample for TDA only; join detection uses DuckDB SQL on full data
                df = duckdb_conn.execute(f"SELECT * FROM {t.duckdb_path} USING SAMPLE 10%").df()
                tables_data[t.table_name] = (t.duckdb_path, df)

        results = find_relationships(duckdb_conn, tables_data)
        for r in results:
            print(f"\n   {r['table1']} <-> {r['table2']}")
            print(
                f"   confidence={r['confidence']:.3f}, topology_sim={r['topology_similarity']:.3f}"
            )
            print(f"   type={r['relationship_type']}")
            for j in r["join_columns"]:
                print(
                    f"      {j['column1']} <-> {j['column2']}: {j['confidence']:.3f} ({j['cardinality']})"
                )

        # Test async detector
        print("\n3. Async detect_relationships()")
        print("-" * 50)

        result = await detect_relationships(table_ids, duckdb_conn, session)
        if result.success:
            r = result.unwrap()
            print(
                f"   tables={r.total_tables}, candidates={r.total_candidates}, high_conf={r.high_confidence_count}"
            )
            print(f"   duration={r.duration_seconds:.2f}s")

    duckdb_conn.close()
    await engine.dispose()
    print("\n" + "=" * 70)
    print("Phase 6 COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
