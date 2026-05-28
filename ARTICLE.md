# DataFusion 51 vs DuckDB: Who reads many small Parquet files faster?

I set out to answer a boring but practical question: when your data lake turns into thousands of tiny Parquet files, which engine handles it better — **Apache DataFusion** or **DuckDB**?

Short answer: in my local benchmark, **DataFusion won every test**.

Long answer: that result comes with context.

## Why I ran this benchmark

Most Parquet benchmarks I see online use a few big files. That is useful, but it does not match a lot of real workloads.

Streaming ingestion, micro-batch ETL, partitioned exports, and event-style pipelines often create many small files. If you leave that alone long enough, you end up with a dataset that looks optimized on paper but is slow in practice.

So I wanted a benchmark focused on the **small-file problem** specifically.

## What I tested

I built a small reproducible harness and compared:

- **DuckDB 1.5.3**
- **DataFusion 53.0.0**

On my machine, that was:

- macOS 26.5 ARM64
- Python 3.12.8
- 8 CPU cores
- 16 GB RAM

The harness generates synthetic Parquet files with repeatable random data, then runs the same queries across both engines.

## Benchmark design

I did not want the benchmark to reward one engine for caching tricks or hidden state. So I made it deliberately strict.

Each measured run:

1. creates a fresh engine session
2. registers the table/files
3. times only the query execution

Before the measured runs, the harness also does a few warmup executions.

The primary metric is **median wall-clock time**.

That design leans toward measuring **open + plan + execute** cost repeatedly, not just the second time a cached query runs. That is intentional, because the small-file problem is usually painful exactly at startup and scheduling time.

## Scenarios

I included five scenarios:

- 100 files
- 500 files
- 1,000 files
- 5,000 files
- 10,000 files

Each scenario used small files, not large ones. That makes metadata and scheduling behavior matter a lot more than raw scan throughput.

## Queries

Each scenario ran four queries:

- `SELECT COUNT(*)`
- selective filter
- simple aggregation
- `COUNT(DISTINCT name)`

Together, those cover a few common patterns: metadata-dominated queries, selective filters, and lightweight aggregations.

## Results

Here is the local snapshot from one full run.

**Environment**
- macOS 26.5 ARM64
- Python 3.12.8
- DuckDB 1.5.3
- DataFusion 53.0.0

**Median times in milliseconds**

| Scenario | Query | DuckDB | DataFusion |
|---|---|---:|---:|
| 100 files | count_star | 3 | 1 |
| 100 files | selective_filter | 4 | 2 |
| 100 files | agg_groupby | 4 | 3 |
| 100 files | count_distinct | 4 | 3 |
| 500 files | count_star | 13 | 4 |
| 500 files | selective_filter | 14 | 7 |
| 500 files | agg_groupby | 16 | 12 |
| 500 files | count_distinct | 15 | 12 |
| 1000 files | count_star | 26 | 7 |
| 1000 files | selective_filter | 28 | 13 |
| 1000 files | agg_groupby | 31 | 21 |
| 1000 files | count_distinct | 29 | 21 |
| 5000 files | count_star | 135 | 36 |
| 5000 files | selective_filter | 151 | 68 |
| 5000 files | agg_groupby | 165 | 107 |
| 5000 files | count_distinct | 153 | 108 |
| 10000 files | count_star | 290 | 115 |
| 10000 files | selective_filter | 326 | 182 |
| 10000 files | agg_groupby | 358 | 272 |
| 10000 files | count_distinct | 336 | 270 |

In this setup, **DataFusion was faster across the board**.

The biggest wins showed up on metadata-heavy and scheduling-heavy queries, especially `count_star`. That makes sense, because those queries stress everything around file handling rather than just data scanning.

## What surprised me

I expected DuckDB to do better on the very small scenarios. It did not.

Even at 100 files, DataFusion was noticeably faster. That tells me the advantage here is probably not just about large scale. It is about overhead per file: registering files, parsing metadata, planning the query, and getting actual execution started.

That said, I want to be careful about generalizing. This benchmark is one harness, one set of file sizes, one OS, and one machine.

## Why DataFusion likely won here

A few things probably helped DataFusion in this benchmark.

First, the benchmark repeatedly exercises startup and planning behavior, not just cached repeated runs. If one engine is better at handling many small partitions cheaply, that shows up fast.

Second, DataFusion is very Arrow-native. In workloads where execution is light but file handling is heavy, that matters.

Third, this benchmark does not give DuckDB much room to show one of its biggest strengths: being an ergonomic, fast default for interactive querying over local files. DuckDB is still incredibly convenient.

## Where DuckDB is still strong

I would not take this benchmark and conclude DuckDB is bad. That would be the wrong lesson.

DuckDB is still excellent for:

- interactive analysis
- quick exploration
- SQL-first local workflows
- mixed workloads with joins
- people who want something simple and fast to set up

If your workload is less about thousands of tiny files and more about querying local datasets in notebooks or scripts, DuckDB is still one of the best tools available.

## What I would test next

If I expand this benchmark, I would add a few more things:

- **persistent sessions**, not just fresh sessions each time
- **S3-backed files**, not only local filesystem
- **mixed file sizes**
- **more selective filters**
- **joins across many small files**
- **query-specific tuning per engine**

That would give a fuller picture.

## How to reproduce it

The repo is here:

- https://github.com/dwickyfp/parquet-small-files-benchmark

You can run:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python benchmark/generate_data.py
python benchmark/run_benchmark.py
```

If you want a faster smoke run, you can generate only the 100-file and 1000-file scenarios first.

## Final take

If your main pain point is **many small Parquet files**, and you care about repeated open/plan/scan overhead, **DataFusion looks like the stronger choice** in this test.

If your main pain point is **fast local analytics with minimal setup**, **DuckDB is still excellent**.

There is no single winner for every workload. But for this specific one, the result was not close.

## Repo and raw results

The benchmark harness and local results are in the GitHub repo:

- https://github.com/dwickyfp/parquet-small-files-benchmark

## One-line summary

For many small Parquet files, **DataFusion was faster than DuckDB** in my local benchmark. But the right engine still depends on your workload, not just one test.
