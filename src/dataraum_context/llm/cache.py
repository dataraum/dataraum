"""LLM response caching to avoid redundant API calls.

Uses the llm_cache table to store and retrieve LLM responses.
Cache key is computed from feature, prompt, model, and table IDs.
"""

import hashlib
import json
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.llm.providers.base import LLMResponse
from dataraum_context.storage.models_v2 import LLMCache as LLMCacheModel


class LLMCache:
    """Cache LLM responses to avoid redundant API calls.

    Cache key is computed from:
    - Feature name (semantic_analysis, quality_rules, etc.)
    - Prompt text
    - Model name
    - Table IDs (optional)

    Cached responses expire based on TTL and can be invalidated
    when source data changes.
    """

    @staticmethod
    def _compute_cache_key(
        feature: str,
        prompt: str,
        model: str,
        table_ids: list[str] | None = None,
    ) -> str:
        """Compute cache key from inputs.

        Args:
            feature: Feature name
            prompt: Prompt text
            model: Model name
            table_ids: Optional table IDs

        Returns:
            SHA256 hash as hex string
        """
        # Create deterministic key data
        key_data = {
            "feature": feature,
            "prompt": prompt,
            "model": model,
            "table_ids": sorted(table_ids or []),
        }

        # Convert to JSON and hash
        key_json = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_json.encode()).hexdigest()

    async def get(
        self,
        session: AsyncSession,
        feature: str,
        prompt: str,
        model: str,
        table_ids: list[str] | None = None,
    ) -> LLMResponse | None:
        """Get cached response if available and valid.

        Args:
            session: Database session
            feature: Feature name
            prompt: Prompt text
            model: Model name
            table_ids: Optional table IDs

        Returns:
            Cached LLMResponse or None if not found/expired
        """
        cache_key = self._compute_cache_key(feature, prompt, model, table_ids)

        # Query for valid, non-expired cache entry
        stmt = select(LLMCacheModel).where(
            LLMCacheModel.cache_key == cache_key,
            LLMCacheModel.is_valid == True,  # noqa: E712
            LLMCacheModel.expires_at > datetime.now(UTC),
        )

        result = await session.execute(stmt)
        cache_entry = result.scalar_one_or_none()

        if cache_entry is None:
            return None

        # Reconstruct LLMResponse from cache
        return LLMResponse(
            content=cache_entry.response_json["content"],
            model=cache_entry.model,
            input_tokens=cache_entry.input_tokens or 0,
            output_tokens=cache_entry.output_tokens or 0,
            cached=True,
            provider_cached=False,
        )

    async def put(
        self,
        session: AsyncSession,
        feature: str,
        prompt: str,
        response: LLMResponse,
        source_id: str | None = None,
        table_ids: list[str] | None = None,
        ontology: str | None = None,
        ttl_seconds: int = 86400,
    ) -> None:
        """Store response in cache.

        Args:
            session: Database session
            feature: Feature name
            prompt: Prompt text
            response: LLM response to cache
            source_id: Optional source ID
            table_ids: Optional table IDs
            ontology: Optional ontology name
            ttl_seconds: Cache TTL in seconds (default 24 hours)
        """
        cache_key = self._compute_cache_key(feature, prompt, response.model, table_ids)

        # Calculate expiry
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)

        # Extract provider from model name (e.g., "claude-sonnet-4" -> "claude")
        provider = response.model.split("-")[0] if "-" in response.model else "unknown"

        # Create cache entry
        cache_entry = LLMCacheModel(
            cache_key=cache_key,
            feature=feature,
            source_id=source_id,
            table_ids={"ids": table_ids or []},
            ontology=ontology,
            provider=provider,
            model=response.model,
            prompt_hash=hashlib.sha256(prompt.encode()).hexdigest()[:16],
            response_json={"content": response.content},
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
            expires_at=expires_at,
            is_valid=True,
        )

        session.add(cache_entry)
        await session.commit()

    async def invalidate_for_source(
        self,
        session: AsyncSession,
        source_id: str,
    ) -> int:
        """Invalidate all cache entries for a source.

        Call this when source data is updated to ensure fresh analysis.

        Args:
            session: Database session
            source_id: Source ID to invalidate

        Returns:
            Number of cache entries invalidated
        """
        stmt = select(LLMCacheModel).where(LLMCacheModel.source_id == source_id)

        result = await session.execute(stmt)
        entries = result.scalars().all()

        count = 0
        for entry in entries:
            entry.is_valid = False
            count += 1

        await session.commit()
        return count

    async def invalidate_for_tables(
        self,
        session: AsyncSession,
        table_ids: list[str],
    ) -> int:
        """Invalidate cache entries containing any of the given table IDs.

        Args:
            session: Database session
            table_ids: Table IDs to invalidate

        Returns:
            Number of cache entries invalidated
        """
        # Query all cache entries and check if they contain any of the table IDs
        stmt = select(LLMCacheModel).where(LLMCacheModel.is_valid == True)  # noqa: E712

        result = await session.execute(stmt)
        entries = result.scalars().all()

        count = 0
        for entry in entries:
            if entry.table_ids:
                cached_table_ids = entry.table_ids.get("ids", [])
                if any(tid in cached_table_ids for tid in table_ids):
                    entry.is_valid = False
                    count += 1

        await session.commit()
        return count
