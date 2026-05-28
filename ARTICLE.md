# DataFusion vs DuckDB: who reads many small Parquet files faster?

## Background

Most Parquet tutorials I see assume clean, medium-sized files. In real data work, that assumption breaks fast.

Streaming ingestion creates files. Micro-batch ETL creates files. Export jobs and partitioned pipelines create files. If nobody cleans up, you end up with thousands of tiny Parquet files that still look fine on paper.

I care about this because I work with pipelines where the problem is not always query complexity. Sometimes the problem is just **file noise**: too many small outputs sitting in a lake, waiting to slow everything down.

That is where engine choice starts to matter. Not just in raw scan speed, but in how the engine handles repeated opens, metadata parsing, planning, and scheduling.

## Problem

The question I wanted to answer was simple: **when there are many small Parquet files, which engine reads them faster?**

This matters because small files change where time is spent.

With big files, the engine usually spends most time reading and processing data. With many small files, the overhead shifts toward:

- opening files
- reading metadata
- registering tables
- planning the query
- scheduling work across partitions

If you benchmark only large files, you can completely miss that.

And if you benchmark engines in a way that rewards cached behavior too much, you also get a misleading picture. The small-file problem is often painful at the **first touch**, not just the fifth.

What I wanted to avoid was another generic comparison. I wanted a benchmark that tried to measure the exact pain point that shows up in real pipelines: **repeated startup and open/plan overhead over many small files**.

## Solution

My approach was to build a small reproducible benchmark focused only on that case.

I did not try to build the most comprehensive database benchmark. I tried to build one that asked a narrow question clearly:

- many small local Parquet files
- different file counts
- a few common query types
- strict fairness rules

The hypothesis was simple: if one engine is better at handling many small partitions cheaply, it should show up here.

I also decided to benchmark in a way that punishes repeated session setup, because that is closer to the real-world pattern I care about. I did not want a benchmark that mainly measures the second or third warm run after the engine already knows everything about the table.

So the benchmark design intentionally favors the use case, not a single engine.

## Implementation

The harness is built in Python and published here:

- https://github.com/dwickyfp/parquet-small-files-benchmark

### What I tested

I compared:

- **DuckDB 1.5.3**
- **DataFusion 53.0.0**

The environment was:

- macOS 26.5 ARM64
- Python 3.12.8
- 8 CPU cores
- 16 GB RAM

### Dataset design

The harness generates many small synthetic Parquet files with fixed randomness.

Scenarios included:

- 100 files
- 500 files
- 1,000 files
- 5,000 files
- 10,000 files

The point was not to simulate a massive warehouse. The point was to stress file handling and scheduling overhead directly.

### Query design

Each scenario ran these queries:

- `SELECT COUNT(*)`
- selective filter
- simple aggregation
- `COUNT(DISTINCT name)`

Those cover different pressure points:

- metadata-dominated work
- filter-heavy work
- lightweight aggregation work

### Fairness controls

This part mattered a lot to me.

I did not want one engine to benefit from hidden caching while the other paid full setup cost every time. So the harness does this on every measured run:

- create a fresh engine session
- register the table/files again
- time the query execution

Before the measured runs, there are also warmup executions.

The final metric is **median wall-clock time** from the measured runs.

That design focuses the benchmark on the question I actually care about: **how painful is repeated open / plan / execute for many small files?**

## Result + Benchmark

### Environment

- macOS 26.5 ARM64
- Python 3.12.8
- DuckDB 1.5.3
- DataFusion 53.0.0

### Median times in milliseconds

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

### Interpretation

In this setup, **DataFusion was faster across every scenario**.

The most obvious wins showed up on queries where startup and planning behavior dominate, especially `count_star`. That makes sense. Those queries expose everything around file handling: metadata, registration, planning, and scheduling.

The gap also widened as file count grew, which is exactly the pattern I expected from the small-file problem.

### What this does not prove

This is still one benchmark, on one machine, with one set of file sizes and queries. I would not use it to make a universal claim about all workloads.

It also does not test:

- persistent sessions
- remote storage
- joins across many files
- big-file scan throughput
- heavily tuned production setups

So the result is meaningful, but narrow.

## Conclusion

If your main problem is **many small Parquet files**, and you care about repeated open / plan / scan overhead, **DataFusion looked stronger than DuckDB in this benchmark**.

If your main problem is quick local analytics with minimal setup, DuckDB is still a great tool. I would not pretend otherwise.

The practical takeaway is simple: pick the tool based on the actual pain point.

If your pipeline keeps creating small files and your queries keep paying file-handling tax, that is where DataFusion is worth a serious look. If your workload is more about interactive exploration over local data, DuckDB still makes a lot of sense.

The full repo and raw results are here:
- https://github.com/dwickyfp/parquet-small-files-benchmark
