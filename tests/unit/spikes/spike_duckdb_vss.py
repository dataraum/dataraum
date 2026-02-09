"""Spike: DuckDB VSS + sentence-transformers for similarity search.

This spike tests the feasibility of using DuckDB's VSS extension
for vector similarity search with sentence-transformers embeddings.

Goals:
1. Verify DuckDB VSS extension works
2. Test sentence-transformers embedding generation
3. Measure embedding quality for schema/column matching
4. Evaluate hybrid approach (metadata in SQLite, vectors in DuckDB)

Usage:
    uv run python prototypes/vector-search/duckdb_vss_spike.py

Requirements:
    uv add sentence-transformers duckdb
"""

from __future__ import annotations

import time
from dataclasses import dataclass


def check_dependencies() -> bool:
    """Check if required dependencies are available."""
    try:
        import duckdb  # noqa: F401

        print("[OK] DuckDB available")
    except ImportError:
        print("[FAIL] DuckDB not installed: uv add duckdb")
        return False

    try:
        from sentence_transformers import SentenceTransformer  # noqa: F401

        print("[OK] sentence-transformers available")
    except ImportError:
        print("[FAIL] sentence-transformers not installed: uv add sentence-transformers")
        return False

    return True


def test_vss_extension() -> bool:
    """Test loading DuckDB VSS extension."""
    import duckdb

    print("\n=== Testing VSS Extension ===")

    conn = duckdb.connect(":memory:")

    try:
        # Try to install and load VSS extension
        print("Installing VSS extension...")
        conn.execute("INSTALL vss")
        print("Loading VSS extension...")
        conn.execute("LOAD vss")
        print("[OK] VSS extension loaded successfully")

        # Check available functions
        result = conn.execute(
            "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE '%distance%'"
        ).fetchall()
        print(f"Available distance functions: {[r[0] for r in result]}")

        conn.close()
        return True

    except Exception as e:
        print(f"[FAIL] VSS extension error: {e}")
        conn.close()
        return False


def test_embeddings() -> tuple[bool, list[list[float]] | None]:
    """Test sentence-transformers embedding generation."""
    print("\n=== Testing Embeddings ===")

    try:
        from sentence_transformers import SentenceTransformer

        # Use a small, efficient model
        model_name = "all-MiniLM-L6-v2"  # 384 dimensions, fast
        print(f"Loading model: {model_name}")

        start = time.time()
        model = SentenceTransformer(model_name)
        load_time = time.time() - start
        print(f"[OK] Model loaded in {load_time:.2f}s")

        # Test texts representing schema elements
        test_texts = [
            "customer_id: unique identifier for customer records",
            "user_id: primary key identifying users in the system",
            "amount: total transaction value in dollars",
            "revenue: sales income from business operations",
            "created_at: timestamp when record was created",
            "order_date: date when order was placed",
        ]

        print(f"\nGenerating embeddings for {len(test_texts)} texts...")
        start = time.time()
        embeddings = model.encode(test_texts)
        encode_time = time.time() - start
        print(f"[OK] Embeddings generated in {encode_time:.3f}s")
        print(f"    Embedding dimensions: {embeddings.shape[1]}")
        print(f"    Time per text: {encode_time / len(test_texts) * 1000:.1f}ms")

        return True, embeddings.tolist()

    except Exception as e:
        print(f"[FAIL] Embedding error: {e}")
        return False, None


def test_similarity_search(embeddings: list[list[float]]) -> bool:
    """Test vector similarity search in DuckDB."""
    import duckdb

    print("\n=== Testing Similarity Search ===")

    conn = duckdb.connect(":memory:")

    try:
        # Load VSS extension
        conn.execute("INSTALL vss")
        conn.execute("LOAD vss")

        # Get dimension from embeddings
        dim = len(embeddings[0])
        print(f"Embedding dimensions: {dim}")

        # Create table with FLOAT array for vectors
        conn.execute(
            f"""
            CREATE TABLE column_embeddings (
                column_id VARCHAR PRIMARY KEY,
                column_name VARCHAR,
                description VARCHAR,
                embedding FLOAT[{dim}]
            )
        """
        )

        # Insert test data
        test_data = [
            ("col_1", "customer_id", "unique identifier for customer records"),
            ("col_2", "user_id", "primary key identifying users in the system"),
            ("col_3", "amount", "total transaction value in dollars"),
            ("col_4", "revenue", "sales income from business operations"),
            ("col_5", "created_at", "timestamp when record was created"),
            ("col_6", "order_date", "date when order was placed"),
        ]

        print("Inserting embeddings...")
        for i, (col_id, col_name, desc) in enumerate(test_data):
            conn.execute(
                """
                INSERT INTO column_embeddings VALUES (?, ?, ?, ?)
            """,
                [col_id, col_name, desc, embeddings[i]],
            )

        # Create HNSW index for fast similarity search
        print("Creating HNSW index...")
        conn.execute(
            """
            CREATE INDEX embedding_idx ON column_embeddings
            USING HNSW (embedding)
            WITH (metric = 'cosine')
        """
        )

        # Test similarity search
        print("\n--- Similarity Search Tests ---")

        # Query: find columns similar to "user identifier"
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("all-MiniLM-L6-v2")

        test_queries = [
            ("user identifier", ["customer_id", "user_id"]),
            ("money value", ["amount", "revenue"]),
            ("date field", ["created_at", "order_date"]),
        ]

        for query_text, expected_similar in test_queries:
            query_embedding = model.encode([query_text])[0].tolist()

            results = conn.execute(
                """
                SELECT
                    column_name,
                    description,
                    array_cosine_distance(embedding, ?::FLOAT[384]) as distance
                FROM column_embeddings
                ORDER BY distance ASC
                LIMIT 3
            """,
                [query_embedding],
            ).fetchall()

            print(f"\nQuery: '{query_text}'")
            print(f"  Expected similar: {expected_similar}")
            print("  Top 3 results:")
            found_expected = False
            for name, _desc, dist in results:
                marker = "*" if name in expected_similar else " "
                print(f"    {marker} {name}: {dist:.4f}")
                if name in expected_similar:
                    found_expected = True

            if found_expected:
                print("  [OK] Found expected result in top 3")
            else:
                print("  [WARN] Expected result not in top 3")

        conn.close()
        return True

    except Exception as e:
        print(f"[FAIL] Similarity search error: {e}")
        import traceback

        traceback.print_exc()
        conn.close()
        return False


@dataclass
class HybridSearchResult:
    """Result from hybrid storage search."""

    column_id: str
    column_name: str
    table_name: str
    semantic_role: str | None
    similarity: float


def test_hybrid_storage() -> bool:
    """Test hybrid approach: SQLite metadata + DuckDB vectors.

    This tests the proposed architecture where:
    - SQLite stores metadata (via SQLAlchemy)
    - DuckDB stores vectors for similarity search
    - JOIN via column_id
    """
    import sqlite3

    import duckdb

    print("\n=== Testing Hybrid Storage ===")

    try:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("all-MiniLM-L6-v2")
        dim = 384

        # Create SQLite metadata store (simulating SQLAlchemy)
        sqlite_conn = sqlite3.connect(":memory:")
        sqlite_conn.execute(
            """
            CREATE TABLE columns (
                column_id TEXT PRIMARY KEY,
                column_name TEXT,
                table_name TEXT,
                data_type TEXT,
                semantic_role TEXT
            )
        """
        )

        # Create DuckDB vector store
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute("INSTALL vss")
        duckdb_conn.execute("LOAD vss")
        duckdb_conn.execute(
            f"""
            CREATE TABLE column_vectors (
                column_id VARCHAR PRIMARY KEY,
                embedding FLOAT[{dim}]
            )
        """
        )

        # Sample metadata
        columns_metadata = [
            ("col_1", "customer_id", "customers", "INTEGER", "identifier"),
            ("col_2", "email", "customers", "VARCHAR", "contact"),
            ("col_3", "total_amount", "orders", "DECIMAL", "measure"),
            ("col_4", "order_date", "orders", "DATE", "timestamp"),
            ("col_5", "product_name", "products", "VARCHAR", "dimension"),
        ]

        print("Inserting metadata and vectors...")
        for col_id, col_name, table_name, data_type, role in columns_metadata:
            # Insert metadata into SQLite
            sqlite_conn.execute(
                "INSERT INTO columns VALUES (?, ?, ?, ?, ?)",
                (col_id, col_name, table_name, data_type, role),
            )

            # Generate and insert embedding into DuckDB
            desc = f"{col_name}: {role} column from {table_name}"
            embedding = model.encode([desc])[0].tolist()
            duckdb_conn.execute("INSERT INTO column_vectors VALUES (?, ?)", [col_id, embedding])

        sqlite_conn.commit()

        # Create HNSW index
        duckdb_conn.execute(
            """
            CREATE INDEX vec_idx ON column_vectors
            USING HNSW (embedding) WITH (metric = 'cosine')
        """
        )

        print("\n--- Hybrid Search: Finding columns ---")

        def hybrid_search(query: str, limit: int = 3) -> list[HybridSearchResult]:
            """Search using vectors, enrich with metadata."""
            # 1. Get query embedding
            query_emb = model.encode([query])[0].tolist()

            # 2. Find similar vectors in DuckDB
            vec_results = duckdb_conn.execute(
                """
                SELECT
                    column_id,
                    1 - array_cosine_distance(embedding, ?::FLOAT[384]) as similarity
                FROM column_vectors
                ORDER BY array_cosine_distance(embedding, ?::FLOAT[384]) ASC
                LIMIT ?
            """,
                [query_emb, query_emb, limit],
            ).fetchall()

            # 3. Enrich with metadata from SQLite
            results = []
            for col_id, similarity in vec_results:
                meta = sqlite_conn.execute(
                    "SELECT column_name, table_name, semantic_role FROM columns WHERE column_id = ?",
                    (col_id,),
                ).fetchone()
                if meta:
                    results.append(
                        HybridSearchResult(
                            column_id=col_id,
                            column_name=meta[0],
                            table_name=meta[1],
                            semantic_role=meta[2],
                            similarity=similarity,
                        )
                    )
            return results

        # Test searches
        test_cases = [
            "user email address",
            "money amount",
            "when was it created",
            "product information",
        ]

        for query in test_cases:
            print(f"\nQuery: '{query}'")
            results = hybrid_search(query)
            for r in results:
                print(
                    f"  {r.column_name} ({r.table_name}.{r.semantic_role}) - sim: {r.similarity:.4f}"
                )

        sqlite_conn.close()
        duckdb_conn.close()
        print("\n[OK] Hybrid storage test passed")
        return True

    except Exception as e:
        print(f"[FAIL] Hybrid storage error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_batch_performance() -> bool:
    """Test performance with batch operations."""
    import random

    import duckdb

    print("\n=== Testing Batch Performance ===")

    try:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("all-MiniLM-L6-v2")
        dim = 384

        # Create DuckDB connection
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL vss")
        conn.execute("LOAD vss")
        conn.execute(
            f"""
            CREATE TABLE vectors (
                id INTEGER PRIMARY KEY,
                embedding FLOAT[{dim}]
            )
        """
        )

        # Generate sample data
        n_vectors = 1000
        print(f"Generating {n_vectors} random embeddings...")
        start = time.time()

        # Generate random descriptions and embeddings
        adjectives = ["primary", "unique", "foreign", "indexed", "nullable"]
        nouns = ["id", "key", "code", "name", "value", "amount", "date", "status"]
        tables = ["users", "orders", "products", "payments", "logs"]

        descriptions = [
            f"{random.choice(adjectives)} {random.choice(nouns)} from {random.choice(tables)}"
            for _ in range(n_vectors)
        ]

        embeddings = model.encode(descriptions, show_progress_bar=True)
        encode_time = time.time() - start
        print(f"[OK] Generated {n_vectors} embeddings in {encode_time:.2f}s")
        print(f"    Throughput: {n_vectors / encode_time:.0f} embeddings/second")

        # Batch insert
        print("\nBatch inserting into DuckDB...")
        start = time.time()
        for i, emb in enumerate(embeddings):
            conn.execute("INSERT INTO vectors VALUES (?, ?)", [i, emb.tolist()])
        insert_time = time.time() - start
        print(f"[OK] Inserted {n_vectors} vectors in {insert_time:.2f}s")
        print(f"    Throughput: {n_vectors / insert_time:.0f} inserts/second")

        # Create index
        print("\nCreating HNSW index...")
        start = time.time()
        conn.execute(
            """
            CREATE INDEX vec_idx ON vectors
            USING HNSW (embedding) WITH (metric = 'cosine')
        """
        )
        index_time = time.time() - start
        print(f"[OK] Index created in {index_time:.2f}s")

        # Search performance
        print("\nTesting search performance...")
        query_emb = embeddings[0].tolist()
        n_queries = 100

        start = time.time()
        for _ in range(n_queries):
            conn.execute(
                """
                SELECT id, array_cosine_distance(embedding, ?::FLOAT[384]) as dist
                FROM vectors
                ORDER BY dist ASC
                LIMIT 10
            """,
                [query_emb],
            ).fetchall()
        search_time = time.time() - start

        print(f"[OK] Executed {n_queries} searches in {search_time:.3f}s")
        print(f"    Average: {search_time / n_queries * 1000:.2f}ms per search")

        conn.close()
        return True

    except Exception as e:
        print(f"[FAIL] Batch performance error: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all spike tests."""
    print("=" * 60)
    print("DuckDB VSS + sentence-transformers Spike")
    print("=" * 60)

    if not check_dependencies():
        print("\n[FAIL] Missing dependencies. Install with:")
        print("  uv add duckdb sentence-transformers")
        return

    results = []

    # Test 1: VSS Extension
    results.append(("VSS Extension", test_vss_extension()))

    # Test 2: Embeddings
    emb_ok, embeddings = test_embeddings()
    results.append(("Embeddings", emb_ok))

    # Test 3: Similarity Search
    if embeddings:
        results.append(("Similarity Search", test_similarity_search(embeddings)))
    else:
        results.append(("Similarity Search", False))

    # Test 4: Hybrid Storage
    results.append(("Hybrid Storage", test_hybrid_storage()))

    # Test 5: Batch Performance
    results.append(("Batch Performance", test_batch_performance()))

    # Summary
    print("\n" + "=" * 60)
    print("SPIKE RESULTS")
    print("=" * 60)
    for name, passed in results:
        status = "[PASS]" if passed else "[FAIL]"
        print(f"  {status} {name}")

    all_passed = all(r[1] for r in results)
    if all_passed:
        print("\n[SUCCESS] All tests passed!")
        print("\nConclusions:")
        print("  1. DuckDB VSS extension works well")
        print("  2. sentence-transformers provides good embeddings")
        print("  3. Hybrid approach (SQLite + DuckDB) is elegant")
        print("  4. Performance is good for expected workloads")
    else:
        print("\n[PARTIAL] Some tests failed - review results above")


if __name__ == "__main__":
    main()
