# Apache DataFusion vs DuckDB: Many Small Parquet Files Benchmark

A reproducible benchmark harness and deep technical analysis comparing **Apache DataFusion** and **DuckDB** on workloads involving many small local Parquet files.

## Why This Benchmark?

Real-world data lakes often accumulate thousands or millions of small Parquet files (from streaming ingestion, micro-batch ETL, or partitioned exports). Most engine benchmarks test against a handful of large files — this repo fills the gap for the *small-file problem*.

## Research Document

See **[RESEARCH.md](./RESEARCH.md)** for the full deep-dive covering:
1. Architecture differences relevant to small-file ingestion
2. Scenario-specific comparison tables
3. Tuning recommendations per engine
4. Benchmark methodology & design

## Quick Start

```bash
# 1. Create env with Python 3.10+ (recommended 3.12)
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Generate all test datasets
python benchmark/generate_data.py

# 3. Run the full benchmark
python benchmark/run_benchmark.py

# 4. View results
ls results/
```

### Fast Smoke Run (optional)
If you only want a quick check:
```bash
# generate only 100-file and 1000-file scenarios
python benchmark/generate_data.py 0
python benchmark/generate_data.py 2

# run the benchmark harness
python benchmark/run_benchmark.py
```

## Benchmark Scenarios

| Scenario | Files | Rows/File | Description |
|----------|-------|-----------|-------------|
| Metadata-heavy | 1,000 | 100 | Schema listing, COUNT(*) — tests open/scan overhead |
| Selective filter | 500 | 1,000 | Point query returning ~0.1% of rows |
| Full scan + agg | 500 | 1,000 | SUM/AVG over all rows |
| Scale-up | 10,000 | 50 | Stress test for file-handle / metadata limits |

## Output

Results are written to `results/`:
- `results.json` — structured results with timings
- `results.csv` — tabular summary
- Console prints a formatted comparison table

## Requirements

- Python 3.10+ (3.12 recommended)
- ~500MB disk for generated test data (clean up with `rm -rf data/`)

## Latest Local Benchmark Snapshot

This repo ships a reproducible harness, not a final verdict. One representative local run:

- macOS 26.5 ARM64
- Python 3.12.8
- DuckDB 1.5.3
- DataFusion 53.0.0

In that run, **DataFusion was consistently faster than DuckDB** on the included small-file smoke scenarios:
- 100 files
- 500 files
- 1000 files
- 5000 files
- 10000 files

See `RESEARCH.md` for the full table and commentary.

## License

MIT
