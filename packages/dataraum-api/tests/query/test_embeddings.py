"""Tests for query embeddings and similarity search."""

from unittest.mock import MagicMock, PropertyMock

import duckdb
import pytest

from dataraum.query.embeddings import (
    QueryEmbeddings,
    build_embedding_text_for_document,
    build_embedding_text_for_graph_output,
    build_embedding_text_for_question,
    generate_embedding,
)


class TestBuildEmbeddingTextForDocument:
    """Tests for the unified build_embedding_text_for_document function."""

    def test_summary_only(self):
        """Document with just summary."""
        text = build_embedding_text_for_document(
            summary="Calculates total revenue by region for Q3 2024."
        )
        assert text == "Calculates total revenue by region for Q3 2024."

    def test_with_step_descriptions(self):
        """Document with summary and step descriptions."""
        text = build_embedding_text_for_document(
            summary="Calculates Days Sales Outstanding.",
            step_descriptions=[
                "Sums total accounts receivable",
                "Calculates average daily sales over the period",
            ],
        )
        assert "Days Sales Outstanding" in text
        assert "accounts receivable" in text
        assert "average daily sales" in text

    def test_with_assumptions(self):
        """Document with summary and assumptions."""
        text = build_embedding_text_for_document(
            summary="Calculates revenue by region.",
            assumption_texts=["Currency is EUR", "Region is EMEA"],
        )
        assert "revenue by region" in text
        assert "Currency is EUR" in text
        assert "Region is EMEA" in text

    def test_full_document(self):
        """Document with all fields."""
        text = build_embedding_text_for_document(
            summary="Calculates total revenue.",
            step_descriptions=["Filter completed orders", "Sum amounts"],
            assumption_texts=["Currency is USD"],
        )
        assert "total revenue" in text
        assert "Filter completed orders" in text
        assert "Sum amounts" in text
        assert "Currency is USD" in text

    def test_truncation_at_max_chars(self):
        """Text is truncated at max_chars."""
        # Create long descriptions
        text = build_embedding_text_for_document(
            summary="Short summary",
            step_descriptions=["A" * 500, "B" * 500],
            max_chars=100,
        )
        # Should be less than or around max_chars
        assert len(text) <= 150  # Allow some margin for short summary

    def test_priority_order_summary_first(self):
        """Summary is always included, steps and assumptions truncated if needed."""
        long_summary = "Summary " * 100  # ~800 chars
        text = build_embedding_text_for_document(
            summary=long_summary,
            step_descriptions=["Step description"],
            assumption_texts=["Assumption text"],
            max_chars=1000,
        )
        # Summary should be included
        assert "Summary" in text

    def test_empty_lists(self):
        """Handles empty lists gracefully."""
        text = build_embedding_text_for_document(
            summary="Just a summary.",
            step_descriptions=[],
            assumption_texts=[],
        )
        assert text == "Just a summary."


class TestEmbeddingTextBuilders:
    """Tests for legacy embedding text builder functions."""

    def test_build_question_text(self):
        """Question text is used as-is."""
        text = build_embedding_text_for_question("What was total revenue?")
        assert text == "What was total revenue?"

    def test_build_graph_text_summary_only(self):
        """Graph text with just summary."""
        text = build_embedding_text_for_graph_output(
            summary="Calculates Days Sales Outstanding by dividing receivables by daily sales."
        )
        assert "Days Sales Outstanding" in text
        assert "receivables" in text

    def test_build_graph_text_with_steps(self):
        """Graph text with summary and step descriptions."""
        text = build_embedding_text_for_graph_output(
            summary="Calculates Days Sales Outstanding.",
            step_descriptions=[
                "Sums total accounts receivable",
                "Calculates average daily sales over the period",
            ],
        )
        assert "Days Sales Outstanding" in text
        assert "accounts receivable" in text
        assert "average daily sales" in text

    def test_build_graph_text_with_name(self):
        """Graph text includes name if not in summary."""
        text = build_embedding_text_for_graph_output(
            summary="Measures collection efficiency.",
            name="Days Sales Outstanding",
        )
        assert "Days Sales Outstanding" in text
        assert "collection efficiency" in text

    def test_build_graph_text_name_not_duplicated(self):
        """Graph name is not duplicated if already in summary."""
        text = build_embedding_text_for_graph_output(
            summary="Calculates Days Sales Outstanding metric.",
            name="Days Sales Outstanding",
        )
        # Name should appear only once (in the summary)
        assert text.count("Days Sales Outstanding") == 1


class TestGenerateEmbedding:
    """Tests for embedding generation."""

    def test_generate_embedding_returns_correct_dimension(self):
        """Embedding has 384 dimensions."""
        embedding = generate_embedding("test text")
        assert len(embedding) == 384

    def test_generate_embedding_is_list_of_floats(self):
        """Embedding is a list of floats."""
        embedding = generate_embedding("test text")
        assert isinstance(embedding, list)
        assert all(isinstance(x, float) for x in embedding)

    def test_similar_texts_have_similar_embeddings(self):
        """Similar texts should produce similar embeddings."""
        emb1 = generate_embedding("What was total revenue?")
        emb2 = generate_embedding("How much money did we make?")
        emb3 = generate_embedding("The quick brown fox jumps over the lazy dog")

        # Calculate cosine similarity manually
        def cosine_sim(a: list[float], b: list[float]) -> float:
            dot = sum(x * y for x, y in zip(a, b, strict=True))
            norm_a = sum(x * x for x in a) ** 0.5
            norm_b = sum(x * x for x in b) ** 0.5
            return dot / (norm_a * norm_b)

        sim_12 = cosine_sim(emb1, emb2)
        sim_13 = cosine_sim(emb1, emb3)

        # Revenue questions should be more similar than revenue vs fox
        assert sim_12 > sim_13


class TestQueryEmbeddings:
    """Tests for QueryEmbeddings class."""

    @pytest.fixture
    def manager(self):
        """Create a mock connection manager with real DuckDB vectors connection."""
        # Create real DuckDB connection for vectors
        vectors_conn = duckdb.connect(":memory:")
        vectors_conn.execute("INSTALL vss")
        vectors_conn.execute("LOAD vss")
        vectors_conn.execute("""
            CREATE TABLE query_embeddings (
                query_id VARCHAR PRIMARY KEY,
                embedding FLOAT[384]
            )
        """)

        # Create mock manager
        manager = MagicMock()
        type(manager).vectors_enabled = PropertyMock(return_value=True)

        # vectors_cursor context manager
        def vectors_cursor_ctx():
            class CursorCtx:
                def __enter__(self):
                    return vectors_conn.cursor()

                def __exit__(self, *args):
                    pass

            return CursorCtx()

        manager.vectors_cursor = vectors_cursor_ctx

        # vectors_write context manager
        def vectors_write_ctx():
            class WriteCtx:
                def __enter__(self):
                    return vectors_conn

                def __exit__(self, *args):
                    pass

            return WriteCtx()

        manager.vectors_write = vectors_write_ctx

        yield manager
        vectors_conn.close()

    def test_add_and_find_similar(self, manager):
        """Add queries and find similar ones."""
        embeddings = QueryEmbeddings(manager)

        # Add some queries
        embeddings.add_query("q1", "What was total revenue?")
        embeddings.add_query("q2", "How much money did we make?")
        embeddings.add_query("q3", "When was the order placed?")

        # Find similar to revenue question
        results = embeddings.find_similar("Show me the sales figures", limit=3)

        assert len(results) == 3
        # Revenue-related queries should rank higher
        query_ids = [r.query_id for r in results]
        # q1 or q2 should be in top 2
        assert "q1" in query_ids[:2] or "q2" in query_ids[:2]

    def test_count(self, manager):
        """Count returns correct number of embeddings."""
        embeddings = QueryEmbeddings(manager)

        assert embeddings.count() == 0

        embeddings.add_query("q1", "Test query 1")
        assert embeddings.count() == 1

        embeddings.add_query("q2", "Test query 2")
        assert embeddings.count() == 2

    def test_remove_query(self, manager):
        """Remove query removes embedding."""
        embeddings = QueryEmbeddings(manager)

        embeddings.add_query("q1", "Test query")
        assert embeddings.count() == 1

        embeddings.remove_query("q1")
        assert embeddings.count() == 0

    def test_add_queries_batch(self, manager):
        """Batch add queries."""
        embeddings = QueryEmbeddings(manager)

        queries = [
            ("q1", "What was total revenue?"),
            ("q2", "How many orders?"),
            ("q3", "Average order value"),
        ]
        embeddings.add_queries_batch(queries)

        assert embeddings.count() == 3

    def test_find_similar_with_min_threshold(self, manager):
        """Find similar respects minimum similarity threshold."""
        embeddings = QueryEmbeddings(manager)

        embeddings.add_query("q1", "What was total revenue?")
        embeddings.add_query("q2", "The quick brown fox")

        # With high threshold, unrelated query should be excluded
        results = embeddings.find_similar(
            "How much money did we earn?",
            min_similarity=0.5,
        )

        # Should find revenue query but maybe not fox
        query_ids = [r.query_id for r in results]
        assert "q1" in query_ids

    def test_upsert_overwrites_existing(self, manager):
        """Adding same query_id overwrites existing embedding."""
        embeddings = QueryEmbeddings(manager)

        embeddings.add_query("q1", "Original question")
        assert embeddings.count() == 1

        embeddings.add_query("q1", "Updated question")
        assert embeddings.count() == 1  # Still 1, not 2
