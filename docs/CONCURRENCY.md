# Concurrency Analysis

This document analyzes concurrency options for the DataRaum Context Engine, particularly for CPU-bound tasks in the analysis pipeline.

## Current Architecture

### Connection Management

The `ConnectionManager` in `core/connections.py` provides thread-safe database access:

- **DuckDB**: Single connection with `cursor()` for reads (thread-safe), mutex-protected writes
- **SQLAlchemy**: Async session factory with connection pooling (SQLite WAL mode enables concurrent reads)

### Workload Characteristics

| Phase | I/O vs CPU | Parallelizable | Notes |
|-------|------------|----------------|-------|
| CSV Loading | I/O bound | Per-file | DuckDB handles internally |
| Statistics | CPU bound | Per-column | pandas operations |
| Type Inference | CPU bound | Per-column | Pattern matching, casting |
| Correlation | CPU bound | Per-pair | O(n²) column pairs |
| TDA/Topology | CPU bound | Per-slice | scipy/ripser computations |
| LLM Calls | I/O bound | Per-request | Network latency dominant |

## Python 3.14 Free-Threading

### What It Is

Python 3.14 introduces experimental free-threading (GIL-disabled mode). When compiled with `--disable-gil`, multiple threads can execute Python bytecode simultaneously on different cores.

### Requirements

```bash
# Check if your build supports free-threading
python -c "import sys; print(sys.abiflags)"
# 't' in output = free-threaded build (e.g., 'cpython-314t')

# Enable GIL-free mode
PYTHON_GIL=0 python script.py
# Or
python -X gil=0 script.py
```

### Getting Free-Threaded Python

1. **Build from source**:
   ```bash
   ./configure --disable-gil
   make && make install
   ```

2. **Fedora 43+**: `dnf install python3.14t` (if available)

3. **pyenv with nogil**:
   ```bash
   pyenv install 3.14.0t  # If supported
   ```

### Impact on Our Stack

| Dependency | Thread-Safe? | Notes |
|------------|--------------|-------|
| **NumPy** | Yes | Releases GIL during computation, C code is thread-safe |
| **pandas** | Mostly | DataFrame operations generally safe, avoid shared mutable state |
| **scipy** | Yes | C/Fortran backends release GIL |
| **ripser/persim** | Yes | C++ backend |
| **DuckDB** | Partial | Single writer, multiple readers via cursors |
| **SQLAlchemy** | Yes | With proper session scoping |
| **PyArrow** | Yes | Immutable data structures, zero-copy safe |

### Recommendation

Free-threading is promising but still experimental. For production:
1. **Short-term**: Use existing async for I/O, DuckDB's internal parallelism for queries
2. **Medium-term**: Test free-threading with our workload once 3.14 stabilizes
3. **Long-term**: Consider Ray for distributed workloads

## DuckDB Internal Parallelism

DuckDB automatically parallelizes queries across all available cores:

```python
# DuckDB uses all cores by default for:
# - Table scans
# - Aggregations
# - Joins
# - Window functions

# Control thread count
conn.execute("SET threads=4")
```

This means CPU-bound analytical queries are already parallelized without any code changes.

## PyArrow Zero-Copy

### Current Usage

DuckDB's `.fetchdf()` and `.arrow()` use zero-copy transfer when possible:

```python
# Efficient - zero-copy from DuckDB to Arrow
arrow_table = conn.execute("SELECT * FROM data").arrow()

# Then to pandas (may copy depending on types)
df = arrow_table.to_pandas()
```

### Benefits

- No memory duplication when passing data between DuckDB and Python
- Thread-safe read access to Arrow tables
- Efficient for large datasets

### Limitations

- Zero-copy only works for compatible types
- Writes still require synchronization
- Not a substitute for multi-process parallelism

## Parallelism Options Comparison

### 1. ThreadPoolExecutor (Current Best Option)

```python
from concurrent.futures import ThreadPoolExecutor
import asyncio

async def parallel_column_analysis(columns: list[str]) -> list[Result]:
    loop = asyncio.get_event_loop()

    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = [
            loop.run_in_executor(pool, analyze_column, col)
            for col in columns
        ]
        return await asyncio.gather(*tasks)
```

**Pros**: Simple, works with async code, low overhead
**Cons**: Limited by GIL (without free-threading)

### 2. ProcessPoolExecutor

```python
from concurrent.futures import ProcessPoolExecutor

def parallel_heavy_computation(data_chunks: list) -> list[Result]:
    with ProcessPoolExecutor(max_workers=4) as pool:
        return list(pool.map(heavy_function, data_chunks))
```

**Pros**: True parallelism, bypasses GIL
**Cons**: Serialization overhead, memory duplication, can't share DuckDB connections

### 3. Ray (For Scale-Out)

```python
import ray

@ray.remote
def process_chunk(chunk):
    return analyze(chunk)

# Distribute across cluster
futures = [process_chunk.remote(c) for c in chunks]
results = ray.get(futures)
```

**Pros**: Distributed computing, fault tolerance, object store
**Cons**: Heavy dependency (PyTorch/etc), operational complexity, overkill for single-machine

### 4. AsyncIO (Current Approach)

```python
async def parallel_llm_calls(prompts: list[str]) -> list[str]:
    async with aiohttp.ClientSession() as session:
        tasks = [call_llm(session, p) for p in prompts]
        return await asyncio.gather(*tasks)
```

**Pros**: Excellent for I/O-bound, lightweight, native Python
**Cons**: Single-threaded for CPU work

## Recommended Strategy

### Phase 1: Optimize Current Architecture

1. **Rely on DuckDB parallelism** for analytical queries
2. **Use async** for LLM calls and database I/O
3. **Batch operations** to reduce overhead

### Phase 2: Add ThreadPoolExecutor for CPU Work

```python
# Example: Parallel column statistics
class StatisticsPhase:
    async def run(self, conn: ConnectionManager) -> None:
        columns = await self.get_columns()

        # CPU-bound work in thread pool
        with ThreadPoolExecutor() as pool:
            loop = asyncio.get_event_loop()
            stats = await asyncio.gather(*[
                loop.run_in_executor(pool, self.compute_stats, col)
                for col in columns
            ])
```

### Phase 3: Evaluate Free-Threading (Python 3.14+)

Once free-threaded Python stabilizes:
1. Test with `PYTHON_GIL=0`
2. Profile CPU-bound phases
3. Remove ThreadPoolExecutor if threads perform better

### Phase 4: Consider Ray (If Needed)

Only if:
- Processing datasets too large for single machine
- Need distributed computing
- Pipeline becomes production-critical

## Connection Migration Plan

Files that need migration to use `ConnectionManager`:

| File | Current Pattern | Migration |
|------|-----------------|-----------|
| `pipeline/runner.py` | Direct `create_async_engine()` | Already uses ConnectionManager |
| `sources/csv/loader.py` | `duckdb.connect(":memory:")` | Intentional - ephemeral analysis |

The in-memory DuckDB connection in `csv/loader.py` is appropriate - it's used for schema analysis during CSV loading and discarded immediately.

## Metrics to Monitor

When implementing parallelism, track:

1. **Wall-clock time** per phase
2. **CPU utilization** (should approach N cores for CPU-bound)
3. **Memory usage** (watch for duplication with ProcessPool)
4. **Database contention** (write lock wait times)

## References

- [PEP 703 - Free-threading CPython](https://peps.python.org/pep-0703/)
- [DuckDB Threading](https://duckdb.org/docs/sql/configuration.html)
- [PyArrow Memory Management](https://arrow.apache.org/docs/python/memory.html)
- [Ray Documentation](https://docs.ray.io/)
