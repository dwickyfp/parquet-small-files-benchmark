# DataFusion vs DuckDB: Deep Technical Analysis — Many Small Parquet Files

## 1. Architecture Differences

### DuckDB

DuckDB is an **embedded analytical database** written in C++. Its Parquet reader uses Apache Arrow's `parquet-cpp` under the hood (via its own fork). Key architectural traits for small-file workloads:

- **Single-process, in-process execution.** No network overhead, no serialization between client and server. File open calls go directly through `libc`.
- **File handle management.** DuckDB opens Parquet files lazily. For queries touching many files (e.g., `SELECT * FROM '*.parquet'` or `read_parquet(['a.parquet','b.parquet',...])`), it opens files on-demand during scan. It does **not** pre-fetch all metadata at plan time — this helps with startup latency but means repeated opens for repeated queries.
- **Parallelism model.** DuckDB uses **Morsel-Driven Execution** with a configurable thread pool (default = hardware threads). Parallelism is applied *within* a file and *across* files. For many small files, each file becomes a small morsel, and scheduling overhead can dominate.
- **Metadata caching.** DuckDB caches Parquet file metadata (footer, row group info) in a per-file cache. On repeated queries to the same file set, subsequent runs are significantly faster. First-run pay is notable for 1000+ files.
- **Projection/filter pushdown.** DuckDB supports column pruning and predicate pushdown into Parquet row groups. For small files with a single row group, the pushdown benefit is minimal — you still pay the open + footer parse cost.
- **`hive_partitioning` support.** DuckDB can auto-detect Hive partitioning from directory structure, but for flat directories of many files, this isn't relevant.

### Apache DataFusion

DataFusion is a **Rust-based query engine** built on Apache Arrow's in-memory columnar format. It's the query engine behind Apache Ballista and often embedded in other systems.

- **Arrow-native from the start.** DataFusion's Parquet reader (`arrow-rs parquet` crate) is written in Rust, not a C++ binding. It reads directly into Arrow RecordBatches with zero-copy where possible.
- **Catalog and table abstraction.** DataFusion uses a `ListingTable` abstraction that can glob directories and treat `path/*.parquet` as a single table. The `TableProvider` interface scans files in the execution plan.
- **Parallelism model.** DataFusion uses a **pull-based, partitioned execution model**. Each Parquet file becomes a partition in the `ParquetExec` node. Tokio handles async I/O, and CPU-bound work runs on a separate thread pool (`tokio::runtime`). For many files, the partition count explodes — DataFusion has a `target_partitions` config to control this.
- **Metadata handling.** `ParquetExec` reads file metadata (footer) during planning to determine schema and row group structure. For 1000+ files, this happens synchronously in `execute()` and can be a significant startup cost. DataFusion added **async metadata fetching** and **metadata caching** in later versions (v33+).
- **Predicate pushdown.** DataFusion supports row-group-level pruning via min/max statistics in Parquet footers. Like DuckDB, for tiny files with one row group, pruning is ineffective.
- **Repartitioning.** DataFusion's optimizer inserts `RepartitionExec` nodes to redistribute work across partitions. For many small files, this can help balance load but adds shuffle overhead.

### Key Architectural Differences Summary

| Aspect | DuckDB | DataFusion |
|--------|--------|------------|
| Language | C++ | Rust |
| Parquet reader | Fork of parquet-cpp | arrow-rs parquet crate |
| I/O model | Synchronous (threads) | Async (tokio) |
| Metadata caching | Per-session file cache | On-demand, with configurable cache |
| File open strategy | Lazy (on scan) | During plan + on scan |
| Parallelism granularity | Per-file + intra-file | Per-file partition + repartition |
| Memory model | Custom buffer manager | Arrow memory pools |
| Default threads | All logical cores | Configurable `target_partitions` |

---

## 2. Scenario-Specific Comparison

### Metadata-Dominated Queries (COUNT(*), schema listing)

When files are very small (100–500 rows each), a `COUNT(*)` query is dominated by:
- Opening each file
- Reading the Parquet footer (metadata)
- Aggregating the row counts

**DuckDB** benefits from its lazy file open pattern — it can stream row counts without full data reads. **DataFusion** pays a planning cost upfront (reading all footers during `ParquetExec` construction) but then executes efficiently.

*Expected: DuckDB has lower first-query latency for <500 files; DataFusion catches up or wins at 1000+ files due to async metadata fetches.*

### Selective Filters (point queries, <1% selectivity)

With predicate pushdown, both engines skip row groups that don't match. For small files (one row group each), **no row groups are skipped** — both engines read all data and filter in memory.

**DuckDB** has a slight edge due to SIMD-optimized filter evaluation and its vectorized execution model. **DataFusion** is competitive with its Arrow-native filter kernels.

*Expected: Close race, DuckDB slightly faster due to mature SIMD paths.*

### Full Scan + Aggregation (SUM, AVG, GROUP BY)

Both engines are highly optimized for columnar scans. For many small files, the bottleneck shifts to I/O scheduling and memory allocation per file.

**DuckDB**'s morsel-driven model handles this well but may have scheduling overhead for tiny morsels. **DataFusion**'s partitioned model maps files to partitions 1:1 by default, which can cause uneven load.

*Expected: Comparable performance; DataFusion may benefit from tuning `target_partitions`.*

### Scale-Up (10,000+ files)

At extreme file counts:
- **DuckDB**: File descriptor pressure, metadata cache growth, thread scheduling overhead.
- **DataFusion**: Memory pressure from storing all file metadata in the plan, potential tokio task scheduling overhead.

*Expected: Both degrade; DuckDB degrades more gracefully due to lazy opening; DataFusion may OOM if all footers are loaded at plan time without caching limits.*

---

## 3. Tuning Recommendations

### DuckDB

1. **`SET threads = N`**: Match to physical cores. For many small files, sometimes *reducing* threads helps (less contention on file opens).
2. **`SET enable_progress_bar = false`**: Progress bar rendering adds overhead for thousands of tiny tasks.
3. **Use `read_parquet(list)` over glob**: Explicit lists avoid repeated directory scans.
4. **Pre-warm metadata cache**: Run a cheap query first (e.g., `SELECT filename, count(*) FROM read_parquet(..., filename=true) GROUP BY 1`) to populate the cache.
5. **Increase `file_search_path`** if files span multiple directories.
6. **Consider merging small files** into larger ones (100MB–1GB) using `COPY ... TO 'merged.parquet'` if the workload allows.

### DataFusion

1. **`config.with_target_partitions(N)`**: Set to number of physical cores. For 10,000+ files, set lower (e.g., 8–16) to reduce partition scheduling overhead.
2. **`config.with_batch_size(8192)`**: Default is 8192; for tiny files, a smaller batch size (e.g., 1024) reduces memory waste.
3. **Enable metadata caching**: Use `ListingTable` with `collect_stat = true` and reuse the same `SessionContext`.
4. **`config.with_repartition_file_scans(true)`** (v35+): Re-partitions small file scans to balance work across executors.
5. **Disable unnecessary optimizations**: `config.with_skip_physical_optimizer_rules(...)` to avoid overhead on simple queries over many files.
6. **Use `register_listing_table`** with explicit schema to skip schema inference.

### Both Engines

- **Merge files first** if possible. The single best optimization is reducing file count.
- **Use Parquet with large row groups** (128MB+) to amortize metadata costs.
- **SSD storage** — seek time on HDD makes many small files dramatically worse.
- **OS file descriptor limits**: Check `ulimit -n`; increase if needed for 10K+ files.

---

## 4. Benchmark Design & Methodology

### Goals
- Measure **wall-clock time** for common query patterns over many small Parquet files.
- Compare first-run (cold) vs subsequent (warm) performance.
- Test at multiple file counts: 100, 500, 1000, 5000, 10000.

### Data Generation
- Each file contains N rows of synthetic data (numeric + string columns).
- Files are 50–500KB each, written with standard Parquet encoding.
- Schema: `id INT64, name UTF8, value FLOAT64, category UTF8, ts TIMESTAMP`

### Queries
1. `COUNT(*)` — metadata-dominated
2. `SELECT * WHERE id = X` — selective filter
3. `SELECT AVG(value), category GROUP BY category` — full scan + agg
4. `SELECT COUNT(DISTINCT name)` — cardinality estimation

### Metrics
- Wall-clock time (ms), 3 runs averaged
- Peak RSS delta
- Files opened count

### Reproducibility
- Pin dependency versions in `requirements.txt`
- Seed random data generation with fixed seed
- Run on a quiescent system; close other applications
- Results include hardware/OS metadata

See `benchmark/` directory for the implementation.
